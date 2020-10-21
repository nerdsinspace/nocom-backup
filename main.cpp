#include <iostream>
#include <string>
#include <variant>
#include <filesystem>
#include <fstream>
#include <optional>
#include <chrono>
#include <ranges>
#include <memory_resource>
#include <vector>

#define PQXX_HAVE_CONCEPTS
#include <pqxx/pqxx>
#include <args.hxx>

#include "pqxx_extensions.h"


namespace fs = std::filesystem;
using namespace std::literals;


std::optional<uint64_t> filenameAsTimestamp(const fs::path& path) {
    const fs::path name = path.filename().stem(); // strip extension
    try {
        return std::stoi(name.native());
    } catch (...) {
        return {};
    }
}

std::optional<fs::path> getYesterdayDiff(const fs::path& dir) {
    std::vector<fs::path> paths;
    for (auto& p : fs::directory_iterator{dir}) {
        if (filenameAsTimestamp(p).has_value()) {
            paths.push_back(std::move(p));
        }
    }

    const auto cmp = [](const fs::path& a, const fs::path& b) {
        return filenameAsTimestamp(a).value() < filenameAsTimestamp(b).value();
    };

    const auto maxIter = std::ranges::max_element(paths, cmp);
    if (maxIter != paths.end()) {
        return *maxIter;
    } else {
        return {};
    }
}

template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

using i16 = int16_t;
using i32 = int32_t;
using i64 = int64_t;

using Hits =           std::tuple<i64, i64, i32, i32, i32, i32, i32, bool, i32>;
using Blocks =         std::tuple<i32, i16, i32, i32, i64, i16, i16>;
using Tracks =         std::tuple<i32, i64, i64, i64, std::optional<i32>, i16, i16, bool>;
using Signs =          std::tuple<i32, i16, i32, pqxx::binarystring, i16, i16>; // I don't like this binarystring
using Servers =        std::tuple<i16, std::string>;
using Players =        std::tuple<i32, UUID, std::string>;
using PlayerSessions = std::tuple<i32, i16, std::optional<i64>, placeholder/*range*/, bool>;
using LastByServer =   std::tuple<i16, i64>;
using Dimensions =     std::tuple<i32, std::string>;
using Chat =           std::tuple<std::string/*json*/, i16, i32, i64, i16>;
// TODO: define full schema for sanity checking

enum class UpdateType : char {
    Create,
    Delete
};

struct Incremental {
    std::string column; // Column the table is sorted by and will be used in query
};

struct Rewrite {};

template<typename>
constexpr bool is_tuple = false;
template<typename... Ts>
constexpr bool is_tuple<std::tuple<Ts...>> = true;

template<typename Tuple> requires is_tuple<Tuple>
struct Table {
    using tuple_type = Tuple;
    std::string name;
    std::variant<Incremental, Rewrite> info;
};


const auto tables = std::make_tuple(
    Table<Chat>          {"chat",            Incremental{"created_at"}},
    Table<Dimensions>    {"dimensions",      Incremental{"ordinal"}},
    Table<LastByServer>  {"last_by_server",  Rewrite{}},
    Table<PlayerSessions>{"player_sessions", Rewrite{}},
    Table<Players>       {"players",         Rewrite{}},
    Table<Servers>       {"servers",         Incremental{"id"}},
    Table<Signs>         {"signs",           Incremental{"created_at"}},
    Table<Tracks>        {"tracks",          Rewrite{}},
    Table<Hits>          {"hits",            Incremental{"id"}},
    Table<Blocks>        {"blocks",          Incremental{"created_at"}}
);


template<typename T>
struct ParseField {
    std::optional<T> operator()(const pqxx::field& field) const {
        return field.get<T>();
    }
};

template<typename T>
struct ParseField<std::optional<T>> {
    std::optional<T> operator()(const pqxx::field& field) const {
        return ParseField<T>{}(field);
    }
};


template<typename Tuple> requires is_tuple<Tuple>
Tuple rowToTuple(const pqxx::row& row) {
    if (row.size() != std::tuple_size_v<Tuple>) {
        throw std::runtime_error("row size wrong!!");
    }

    // TODO: std::optional needs to be treated differently
    return [&row]<size_t... I>(std::index_sequence<I...>) {
        return std::make_tuple(ParseField<std::tuple_element_t<I, Tuple>>{}(row.at(I)).value()...);
    }(std::make_index_sequence<std::tuple_size_v<Tuple>>{});
}


template<typename T>
T* allocateElements(std::pmr::vector<T>& vec, size_t size) {
    vec.resize(vec.size() + size);
    return &vec.back() - (size - 1);
}

template<typename T> requires std::is_trivial_v<T>
void serialize(std::pmr::vector<char>& vec, T x) {
    auto* buf = allocateElements(vec, sizeof(x));
    memcpy(buf, &x, sizeof(x));
}

void serialize(std::pmr::vector<char>& vec, std::string_view str) {
    const auto bufSize = sizeof(int32_t) + str.size();
    auto* buf = allocateElements(vec, bufSize);
    const int32_t len = str.size();
    memcpy(buf, &len, sizeof(int32_t));
    memcpy(buf + sizeof(int32_t), &str[0], len);
}

template<typename T>
void serialize(std::pmr::vector<char>& vec, const std::optional<T> optional) {
    serialize(vec, optional.has_value());
    if (optional) serialize(vec, *optional);
}

void serialize(std::pmr::vector<char>&, placeholder) {}

void serialize(std::pmr::vector<char>& out, const pqxx::binarystring& vec) {
    const auto bufSize = sizeof(int32_t) + vec.size();
    auto* buf = allocateElements(out, bufSize);
    const int32_t len = vec.size();
    memcpy(buf, &len, sizeof(int32_t));
    memcpy(buf + sizeof(int32_t), &vec[0], len);
}

template<typename Tuple> requires is_tuple<Tuple>
void serializeTupleToBuffer(const Tuple& tuple, std::pmr::vector<char>& vec) {
    std::apply([&vec](const auto&... x) {
        (serialize(vec, x), ...);
    }, tuple);
}

template<typename Tuple> requires is_tuple<Tuple>
void outputTable(const fs::path& file, const pqxx::result& result) {
    std::cout << "Outputting table to " << file.string() << '\n';
    std::ofstream out(file);

    for (const pqxx::row& row : result) {
        const auto dim = rowToTuple<Tuple>(row);
        char allocator_buffer[1000000]; // should be more than enough for any row
        std::pmr::monotonic_buffer_resource resource(allocator_buffer, sizeof(allocator_buffer));
        std::pmr::vector<char> buffer{&resource};
        buffer.reserve(sizeof(allocator_buffer));

        serializeTupleToBuffer(dim, buffer);
        std::cout << "writing " << buffer.size() << " bytes to file\n";
        out.write(&buffer[0], buffer.size());

        //std::cout << "Ordinal = " << std::get<0>(dim) << '\n';
        //std::cout << "Name = " << std::get<1>(dim) << '\n';
    }
}

std::string selectNewestQuery(const std::string& table, const Incremental& inc, const std::string& oldValue) {
    return "SELECT * FROM "s + table + " WHERE "s + inc.column + " > "s + oldValue;
}

std::string selectAllQuery(const std::string& table) {
    return "SELECT * FROM "s + table;
}

void runBackup(pqxx::connection& db, const fs::path& rootOutput) {
    const auto now = std::chrono::system_clock::now();
    const uint64_t millis = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    const fs::path outDir = rootOutput / std::to_string(millis);
    std::cout << "Creating output directory at " << outDir << '\n';
    fs::create_directory(outDir);


    const auto output = [&]<typename Tuple>(const Table<Tuple>& table) {
        if (table.name != "players") return;
        std::string query;
        std::visit(overloaded {
            [&](const Incremental& inc) {
                query = selectNewestQuery(table.name, inc, "-10");
            },
            [&](Rewrite) {
                query = selectAllQuery(table.name);
            }
        }, table.info);
        query += " limit 1";

        pqxx::result result = pqxx::work{db}.exec(query);

        outputTable<Tuple>(outDir / table.name, result);
    };

    std::apply([&](const auto&... table) {
        (output(table), ...);
    }, tables);
}

int main(int argc, char** argv)
{
    try
    {
        pqxx::connection con;
        std::cout << "Connected to " << con.dbname() << std::endl;

        const fs::path out{"output"};
        fs::create_directories(out);

        runBackup(con, out);
        pqxx::work work{con};
        pqxx::result result = work.exec("select * from Players limit 1");
        for (auto row: result)
            std::cout << row[1].c_str() << '\n';

        std::cout << "Done.\n";
    }
    catch (std::exception const &e)
    {
        std::cerr << e.what() << '\n';
        return 1;
    }
    return 0;
}