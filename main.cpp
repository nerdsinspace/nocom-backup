#include <iostream>
#include <string>
#include <variant>
#include <filesystem>
#include <fstream>
#include <optional>
#include <chrono>
#include <memory_resource>
#include <vector>

#include <pqxx/pqxx>
#include <args.hxx>

#include "pqxx_extensions.h"
#include "tables.h"
#include "serialization.h"


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

std::optional<fs::path> getNewestDiff(const fs::path& dir) {
    std::vector<fs::path> paths;
    for (auto& p : fs::directory_iterator{dir}) {
        if (filenameAsTimestamp(p).has_value()) {
            paths.push_back(p);
        }
    }

    const auto cmp = [](const fs::path& a, const fs::path& b) {
        return filenameAsTimestamp(a).value() < filenameAsTimestamp(b).value();
    };

    const auto maxIter = std::ranges::max_element(paths, cmp);
    if (maxIter != paths.end()) {
        return std::move(*maxIter);
    } else {
        return {};
    }
}

template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;


enum class UpdateType : char {
    Create,
    Delete
};

struct Header {
    static constexpr int CURRENT_VERSION = 1;
    int version;
    int numRows;

    explicit Header(int rows): numRows(rows), version(CURRENT_VERSION) {}
    explicit Header() = default;

    void checkVersion() const {
        if (this->version != CURRENT_VERSION) {
            throw std::runtime_error{"File has different version"};
        }
    }
};
static_assert(std::has_unique_object_representations_v<Header>);


template<typename>
constexpr bool is_tuple = false;
template<typename... Ts>
constexpr bool is_tuple<std::tuple<Ts...>> = true;

template<typename>
constexpr bool is_optional = false;
template<typename T>
constexpr bool is_optional<std::optional<T>> = true;

template<typename>
constexpr bool is_incremental = false;
template<typename T>
constexpr bool is_incremental<Incremental<T>> = true;

// TODO: make table names constexpr
const auto tables = std::make_tuple(
    Chat          {"chat"},
    Dimensions    {"dimensions"},
    LastByServer  {"last_by_server"},
    PlayerSessions{"player_sessions"},
    Players       {"players"},
    Servers       {"servers"},
    Signs         {"signs"},
    Tracks        {"tracks"},
    Hits          {"hits"},
    Blocks        {"blocks"}
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

template<typename T>
auto getField(const pqxx::row& row, size_t index) {
    std::optional maybe =  ParseField<T>{}(row.at(index));
    if constexpr (is_optional<T>) {
        return maybe;
    } else {
        return std::move(maybe.value());
    }
}

template<typename Tuple> requires is_tuple<Tuple>
Tuple rowToTuple(const pqxx::row& row) {
    if (row.size() != std::tuple_size_v<Tuple>) {
        throw std::runtime_error("row size wrong!!");
    }

    return [&row]<size_t... I>(std::index_sequence<I...>) {
        return std::make_tuple(getField<std::tuple_element_t<I, Tuple>>(row, I)...);
    }(std::make_index_sequence<std::tuple_size_v<Tuple>>{});
}



template<typename Tuple> requires is_tuple<Tuple>
void serializeTupleToBuffer(const Tuple& tuple, std::pmr::vector<char>& vec) {
    std::apply([&vec]<typename... T>(const T&... x) {
        (Serializable<T>::serialize(vec, x), ...);
    }, tuple);
}

template<typename Tuple> requires is_tuple<Tuple>
void outputTable(const fs::path& file, const pqxx::result& result) {
    std::cout << "Outputting table to " << file.string() << '\n';
    std::ofstream out(file, std::ios_base::out | std::ios_base::binary);
    out.exceptions(std::ios_base::badbit | std::ios_base::failbit);

    for (const pqxx::row& row : result) {
        const auto dim = rowToTuple<Tuple>(row);
        char allocator_buffer[1000000]; // should be more than enough for any row
        std::pmr::monotonic_buffer_resource resource(allocator_buffer, sizeof(allocator_buffer));
        std::pmr::vector<char> buffer{&resource};
        buffer.reserve(sizeof(allocator_buffer));

        serializeTupleToBuffer(dim, buffer);
        std::cout << "writing " << buffer.size() << " bytes to file\n";
        out.write(&buffer[0], buffer.size());
    }
}

template<typename>
struct tuple_storage;
template<typename... T>
struct tuple_storage<std::tuple<T...>> {
    using type = std::tuple<std::aligned_storage_t<sizeof(T), alignof(T)>...>;
};

template<typename Tuple>
Tuple readTuple(std::ifstream& in) {
    // have to assign to this rather than construct because order of evaluation is undefined
    using storage = typename tuple_storage<Tuple>::type;
    storage out;
    [&]<size_t... I>(std::index_sequence<I...>) {
        ((new (reinterpret_cast<void*>(&std::get<I>(out))) std::tuple_element_t<I, Tuple>{Serializable<std::tuple_element_t<I, Tuple>>::deserialize(in)}), ...);
    }(std::make_index_sequence<std::tuple_size_v<Tuple>>{});

    return [&]<size_t... I>(std::index_sequence<I...>) {
        return std::make_tuple(*std::launder(reinterpret_cast<std::tuple_element_t<I, Tuple>*>(&std::get<I>(out)))...);
    }(std::make_index_sequence<std::tuple_size_v<Tuple>>{});
}

template<typename TABLE>
std::optional<typename TABLE::tuple> readNewestRow(const TABLE& table, const fs::path& diffPath) {
    const fs::path file = diffPath / table.name;
    std::ifstream in{file, std::ios_base::in | std::ios_base::binary};
    in.exceptions(std::ios_base::badbit | std::ios_base::failbit);

    Header header{};
    in.read(reinterpret_cast<char*>(&header), sizeof(header));
    if (header.numRows > 0) {
        return readTuple<typename TABLE::tuple>(in);
    } else {
        // This should never happen for a table like hits but dimensions will almost always be empty
        return {};
    }
}


template<typename>
struct ColumnNamesImpl;

template<typename... Columns>
struct ColumnNamesImpl<Table<Columns...>> {
    static std::string get() {
        std::string str = ((std::string{Columns::name} + ", ") + ...);
        //std::string str;
        //((str += Columns::name, str += ", "), ...);
        str.pop_back();
        str.pop_back();
        return str;
    }
};

template<typename TABLE>
std::string columnNames() {
    return ColumnNamesImpl<typename TABLE::base_type>::get();
}

template<typename TABLE> requires is_incremental<typename TABLE::table_type>
std::string selectNewestQuery(const std::string& table, const std::string& oldValue) {
    std::string columnName{TABLE::table_type::column::name};
    return "SELECT "s + columnNames<TABLE>()  + " FROM "s + table + " WHERE "s + columnName + " > "s + oldValue;
}

template<typename TABLE>
std::string selectAllQuery(const std::string& table) {
    return "SELECT "s + columnNames<TABLE>() + " FROM "s + table;
}

void runBackup(pqxx::connection& db, const fs::path& rootOutput) {
    const auto now = std::chrono::system_clock::now();
    const uint64_t millis = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    const fs::path outDir = rootOutput / std::to_string(millis);
    const std::optional<fs::path> lastDiff = getNewestDiff(rootOutput);

    std::cout << "Creating output directory at " << outDir << '\n';
    fs::create_directory(outDir);

    const auto output = [&]<typename T>(const T& table) {
        std::string query;
        if constexpr (is_incremental<typename T::table_type>) {
            if (lastDiff.has_value()) {
                const std::tuple newest = readNewestRow(table, *lastDiff);
                query = selectNewestQuery<T>(table.name, "-10");
            } else {
                query = selectAllQuery<T>(table.name);
            }
        } else if constexpr (std::is_same_v<typename T::table_type, Rewrite>) {
            query = selectAllQuery<T>(table.name);
        } else {
            throw std::logic_error{"unhandled type"};
        }
        query += " limit 1";

        pqxx::result result = pqxx::work{db}.exec(query);

        outputTable<typename T::tuple>(outDir / table.name, result);
    };

    std::apply([&](const auto&... table) {
        (output(table), ...);
    }, tables);
}

int main(int argc, char** argv)
{
    try {
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
    catch (std::exception const &e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
}