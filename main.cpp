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
    Hits          {"hits"},
    Chat          {"chat"},
    Dimensions    {"dimensions"},
    LastByServer  {"last_by_server"},
    PlayerSessions{"player_sessions"},
    Players       {"players"},
    Servers       {"servers"},
    Signs         {"signs"},
    Tracks        {"tracks"},
    //Hits          {"hits"},
    Blocks        {"blocks"}
);


std::optional<uint64_t> filenameAsTimestamp(const fs::path& path) {
    const fs::path name = path.filename().stem(); // strip extension
    try {
        return std::stoi(name.native());
    } catch (...) {
        return {};
    }
}

std::vector<fs::path> getOldOutputsSorted(const fs::path& dir) {
    std::vector<fs::path> paths;
    for (auto& p : fs::directory_iterator{dir}) {
        if (filenameAsTimestamp(p).has_value()) {
            paths.push_back(p);
        }
    }
    const auto cmp = [](const fs::path& a, const fs::path& b) {
        return filenameAsTimestamp(a).value() < filenameAsTimestamp(b).value();
    };
    std::ranges::sort(paths, cmp);
    return paths;
}

std::optional<fs::path> getNewestOutput(const fs::path& dir) {
    std::vector<fs::path> paths = getOldOutputsSorted(dir);
    if (!paths.empty()) {
        return std::move(paths.back());
    } else {
        return {};
    }
}

Header readHeader(const fs::path& file) {
    std::ifstream in{file, std::ios_base::in | std::ios_base::binary};
    in.exceptions(std::ios_base::badbit | std::ios_base::failbit);
    Header h{};
    in.read(reinterpret_cast<char*>(&h), sizeof(h));
    return h;
}

std::optional<fs::path> getNewestNonEmptyDiffForTable(const fs::path& dir, std::string_view tableName) {
    const std::vector<fs::path> paths = getOldOutputsSorted(dir);
    for (const auto& p : paths) {
        const auto tableFile = dir / tableName;
        if (!fs::exists(p)) {
            std::cerr << "Warning: old output (" << p.string() << ") does not contain file for " << tableName << '\n';
        } else {
            const Header h = readHeader(p);
            if (h.version != Header::CURRENT_VERSION) {
                // TODO: handle this better?
                break;
            }
            if (h.numRows > 0) {
                return p;
            }
        }
    }

    return {};
}


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

void writeHeader(std::ofstream& out, int rows) {
    const Header h{rows};
    out.write(reinterpret_cast<const char*>(&h), sizeof(h));
}

void writeHeader(const fs::path& file, int rows) {
    std::ofstream out(file, std::ios_base::out | std::ios_base::binary);
    out.exceptions(std::ios_base::badbit | std::ios_base::failbit);
    writeHeader(out, rows);
}

template<typename Tuple> requires is_tuple<Tuple>
void outputTable(const fs::path& file, const pqxx::result& result) {
    std::cout << "Outputting table to " << file.string() << '\n';
    std::ofstream out(file, std::ios_base::out | std::ios_base::binary);
    out.exceptions(std::ios_base::badbit | std::ios_base::failbit);

    writeHeader(out, result.size());

    for (const pqxx::row& row : result) {
        const auto tuple = rowToTuple<Tuple>(row);
        char allocator_buffer[1000000]; // should be more than enough for any row
        // this looks like it generates bad code so I might replace this with something simpler
        std::pmr::monotonic_buffer_resource resource(allocator_buffer, sizeof(allocator_buffer));
        std::pmr::vector<char> buffer{&resource};
        buffer.reserve(sizeof(allocator_buffer));

        // I can potentially remove this for incremental tables
        Serializable<UpdateType>::serialize(buffer, UpdateType::Create);
        serializeTupleToBuffer(tuple, buffer);
        out.write(&buffer[0], buffer.size());
    }
}

template<typename T, typename... Rest>
std::tuple<T, Rest...> readTuple0(std::ifstream& in) {
    std::tuple<T> x{Serializable<T>::deserialize(in)};
    if constexpr (sizeof...(Rest) > 0) {
        return std::tuple_cat(std::move(x), readTuple0<Rest...>(in));
    } else {
        return x;
    }
}

template<typename>
struct ReadTupleImpl;
template<typename... T>
struct ReadTupleImpl<std::tuple<T...>> {
    static std::tuple<T...> impl(std::ifstream& in) {
        return readTuple0<T...>(in);
    }
};

template<typename Tuple>
Tuple readTuple(std::ifstream& in) {
    return ReadTupleImpl<Tuple>::impl(in);
}

template<typename>
struct TableIndexOf;
template<typename... Ts>
struct TableIndexOf<Table<Ts...>> {
    template<typename T>
    static consteval size_t get() {
        constexpr bool results[]{std::is_same_v<T, Ts>...};
        for (size_t i = 0; i < std::size(results); i++) {
            if (results[i]) return i;
        }
        return -1;
        // this throw should be valid but gcc doesnt like it
        //throw "type not in table";
    }
};

template<typename TABLE>
std::optional<typename TABLE::tuple> readNewestRow(const fs::path& file) {
    std::ifstream in{file, std::ios_base::in | std::ios_base::binary};
    in.exceptions(std::ios_base::badbit | std::ios_base::failbit);

    Header header{};
    in.read(reinterpret_cast<char*>(&header), sizeof(header));
    header.checkVersion();
    if (header.numRows > 0) {
        return readTuple<typename TABLE::tuple>(in);
    } else {
        // This should never happen for a table like hits but dimensions will almost always be empty after the first diff
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
    return "SELECT " + columnNames<TABLE>()  + " FROM " + table + " WHERE " + columnName + " > " + oldValue;
}

std::string sortedResults(const std::string& query, const std::string& column) {
    return "SELECT * FROM (" + query + ") AS UWU ORDER BY " + column + " DESC";
}

template<typename TABLE> requires is_incremental<typename TABLE::table_type>
std::string sortedResults(const std::string& query) {
    std::string columnName{TABLE::table_type::column::name};
    return sortedResults(query, columnName);
}

template<typename TABLE>
std::string selectAllQuery(const std::string& table) {
    return "SELECT "s + columnNames<TABLE>() + " FROM "s + table;
}

// returns the largest number of rows returned by a query
int runBackup(pqxx::connection& db, const fs::path& rootOutput) {
    const auto now = std::chrono::system_clock::now();
    const uint64_t millis = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    const fs::path outDir = rootOutput / std::to_string(millis);
    //const std::optional<fs::path> lastDiff = getNewestOutput(rootOutput);

    std::cout << "Creating output directory at " << outDir << '\n';
    fs::create_directory(outDir);

    const auto output = [&]<typename T>(const T& table) -> int {
        const fs::path tableFile = outDir / table.name;

        std::string query;
        if constexpr (is_incremental<typename T::table_type>) {
            const std::optional<fs::path> lastDiff = getNewestNonEmptyDiffForTable(rootOutput, table.name);
            if (lastDiff.has_value()) {
                const std::optional newestRow = readNewestRow<T>(*lastDiff);
                if (newestRow.has_value()) {
                    using column = typename T::table_type::column;
                    constexpr size_t tupleIndex = TableIndexOf<typename T::base_type>::template get<column>();
                    const auto newest = std::get<tupleIndex>(newestRow.value());
                    query = selectNewestQuery<T>(table.name, std::to_string(newest));
                } else {
                    // this table has no old output data
                    query = selectAllQuery<T>(table.name);
                }
            } else {
                query = selectAllQuery<T>(table.name);
            }

            query += " limit 10000000";
            query = sortedResults<T>(query);
        } else if constexpr (std::is_same_v<typename T::table_type, Rewrite>) {
            query = selectAllQuery<T>(table.name);
            query += " limit 10000000";
        } else {
            throw std::logic_error{"unhandled type"};
        }

        pqxx::result result = pqxx::work{db}.exec(query); // might want to put this outside of the lambda
        std::cout << result.size() << " rows\n";

        outputTable<typename T::tuple>(tableFile, result);

        return result.size();
    };

    int largest = -1;
    std::apply([&](const auto&... table) {
        ((largest = std::max(output(table), largest)), ...);
    }, tables);
    assert(largest != -1);
    return largest;
}

int main(int argc, char** argv)
{
    try {
        pqxx::connection con;
        std::cout << "Connected to " << con.dbname() << std::endl;

        const fs::path out{"output"};
        fs::create_directories(out);

        // queries are limited to 10 mil rows so just keep going until there are no more rows to query
        int mostRows;
        do {
            mostRows = runBackup(con, out);
        } while(mostRows >= 10'000'000);
        runBackup(con, out);
        //pqxx::work work{con};
        //pqxx::result result = work.exec("select * from Players limit 1");
        //for (auto row: result)
        //    std::cout << row[1].c_str() << '\n';

        std::cout << "Done.\n";
    }
    catch (std::exception const &e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
}