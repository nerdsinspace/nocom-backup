#include <iostream>
#include <string>
#include <variant>
#include <filesystem>
#include <fstream>
#include <optional>
#include <chrono>
#include <memory_resource>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <ranges>

#include <pqxx/pqxx>
#include <args.hxx>

#include "pqxx_extensions.h"
#include "tables.h"
#include "serialization.h"


namespace fs = std::filesystem;
using namespace std::literals;

constexpr auto QUERY_LIMIT = 10'000;//10'000'000;

struct Header {
    static constexpr int CURRENT_VERSION = 1;
    int32_t version = CURRENT_VERSION;
    int32_t numRows{};
    int64_t lastRowPos = -1; // byte position in the file of the last row
    // This will need to be changed if a table with a non number key is added.
    // These have meaningless values for rewrite tables.
    int64_t firstRowKey = -666; // yesterday_first_row
    int64_t lastRowKey = -666; // yesterday_last_row

    explicit Header(int v, int rows, int64_t lastRow):
        version(v), numRows(rows), lastRowPos(lastRow) {}
    explicit Header(int v, int rows, int64_t lastRow, int64_t firstKey, int64_t lastKey):
        version(v), numRows(rows), lastRowPos(lastRow), firstRowKey(firstKey), lastRowKey(lastKey) {}
    explicit Header(int rows): numRows(rows) {}
    explicit Header() = default;

    void checkVersion() const {
        if (this->version != CURRENT_VERSION) {
            throw std::runtime_error{"File has different version"};
        }
    }

    static int64_t lastRowPosOffset() {
        return sizeof(version) + sizeof(numRows);
    }
};

template<>
struct Serializable<Header> {
    static void serialize(std::pmr::vector<char>& vec, Header h) {
        Serializable<int32_t>::serialize(vec, h.version);
        Serializable<int32_t>::serialize(vec, h.numRows);
        Serializable<int64_t>::serialize(vec, h.lastRowPos);
        Serializable<int64_t>::serialize(vec, h.firstRowKey);
        Serializable<int64_t>::serialize(vec, h.lastRowKey);
    }

    static Header deserialize(std::ifstream& in) {
        return Header {
          Serializable<int32_t>::deserialize(in), // version
          Serializable<int32_t>::deserialize(in), // numRows
          Serializable<int64_t>::deserialize(in), // lastRowPos
          Serializable<int64_t>::deserialize(in), // firstRowKey
          Serializable<int64_t>::deserialize(in)  // lastRowKey
        };
    }
};

template<typename>
constexpr bool is_tuple = false;
template<typename... Ts>
constexpr bool is_tuple<std::tuple<Ts...>> = true;

template<typename>
constexpr bool is_optional = false;
template<typename T>
constexpr bool is_optional<std::optional<T>> = true;


// TODO: make table names constexpr
const auto tables = std::make_tuple(
    Dimensions    {"dimensions"},
    LastByServer  {"last_by_server"},
    //PlayerSessions{"player_sessions"},
    Players       {"players"},
    Servers       {"servers"},
    Signs         {"signs"},
    //Tracks        {"tracks"},
    Chat          {"chat"},
    Hits          {"hits"},
    Blocks        {"blocks"}
);


std::optional<uint64_t> pathNameAsTimestamp(const fs::path& path) {
    const fs::path name = path.filename();
    try {
        return std::stoull(name.string());
    } catch (...) {
        return {};
    }
}

std::vector<fs::path> getOldOutputsSorted(const fs::path& dir) {
    std::vector<fs::path> paths;
    for (auto& p : fs::directory_iterator{dir}) {
        if (pathNameAsTimestamp(p).has_value()) {
            paths.push_back(p);
        } else {
            std::cout << "troll\n";
        }
    }
    auto str = dir.string();
    const auto cmp = [](const fs::path& a, const fs::path& b) {
        return pathNameAsTimestamp(a).value() > pathNameAsTimestamp(b).value();
    };
    std::ranges::sort(paths, cmp);
    return paths;
}

// Today's output has already been created so we want to be able to ignore it
std::optional<fs::path> getNewestOutput(const fs::path& dir, const fs::path& today) {
    std::vector<fs::path> paths = getOldOutputsSorted(dir);
    auto it = std::find_if_not(paths.begin(), paths.end(), [&](const fs::path& p) { return p == today; });
    if (it != paths.end()) {
        return std::move(*it);
    } else {
        return {};
    }
}

Header readHeader(const fs::path& file) {
    auto troll = file.string();
    std::ifstream in{file, std::ios_base::in | std::ios_base::binary};
    in.exceptions(std::ios_base::badbit | std::ios_base::failbit);
    return Serializable<Header>::deserialize(in);
}


std::optional<fs::path> getNewestFileForTable(const fs::path& dir, std::string_view tableName) {
    std::vector<fs::path> files;
    for (const auto& p : fs::directory_iterator{dir}) {
        const auto name = p.path().filename().string();
        if (name.find(tableName) == 0) {
            files.push_back(p);
        }
    }
    if (files.empty()) return {};

    const auto fileNumber = [&](const fs::path& p) {
        auto numStr = p.string().substr(tableName.length());
        return !numStr.empty() ? std::stoi(numStr) : 0;
    };
    return *std::ranges::max_element(files, {}, fileNumber);
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
    std::optional maybe = ParseField<T>{}(row.at(index));
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
    std::pmr::vector<char> buf;
    Serializable<Header>::serialize(buf, Header{rows});
    out.write(buf.data(), buf.size());
}

void writeHeader(std::ofstream& out, int rows, int64_t firstRowKey, int64_t lastRowKey) {
    std::pmr::vector<char> buf;
    Serializable<Header>::serialize(buf, Header{Header::CURRENT_VERSION, rows, -1, firstRowKey, lastRowKey});
    out.write(buf.data(), buf.size());
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

// returns true if the type is Incremental and its sorted by column is not unique (basically tabled with created_at)
template<IncrementalTable T>
constexpr bool should_do_delete_troll() {
    return !T::table_type::is_unique;
}
template<RewriteTable>
constexpr bool should_do_delete_troll() {
    return false;
}

template<IncrementalTable TABLE> requires (should_do_delete_troll<TABLE>())
int getEndIndex(const pqxx::result& result) {
    using column = typename TABLE::table_type::column;
    using T = typename column::type;
    constexpr size_t columnIndex = TableIndexOf<typename TABLE::base_type>::template get<column>();
    const auto& back = result.back();
    const T last = ParseField<T>{}(back[columnIndex]).value();

    for (int i = result.size() - 1; i >= 0; i--) {
        if (ParseField<T>{}(result[i][columnIndex]).value() != last) {
            return i;
        }
    }

    std::cout << "entire result has the same " << column::name << "\n";
    return -1;
}

template<typename>
auto getEndIndex(const pqxx::result& result) {
    return result.size() - 1;
}

template<typename TABLE>
void outputTable(const fs::path& file, const pqxx::result& result) {
    if (result.empty()) throw "empty result";
    using Tuple = typename TABLE::tuple;

    std::cout << "Outputting table to " << file.string() << '\n';
    std::ofstream out(file, std::ios_base::out | std::ios_base::binary);
    out.exceptions(std::ios_base::badbit | std::ios_base::failbit);


    const int last = getEndIndex<TABLE>(result);
    const int numRows = last + 1;

    if (last >= 0)  {
        if constexpr (IncrementalTable<TABLE>) {
            const std::tuple firstTuple = rowToTuple<typename TABLE::tuple>(result.front());
            using column = typename TABLE::table_type::column;
            constexpr size_t tupleIndex = TableIndexOf<typename TABLE::base_type>::template get<column>();

            const auto firstElement = std::get<tupleIndex>(firstTuple);
            if (numRows > 1) {
                const std::tuple lastTuple  = rowToTuple<typename TABLE::tuple>(result[last]);
                const auto lastElement = std::get<tupleIndex>(lastTuple);

                writeHeader(out, numRows, firstElement, lastElement);
            } else {
                // very unlikely but possible
                writeHeader(out, numRows, firstElement, firstElement);
            }
        } else {
            writeHeader(out, numRows, -666, -666);
        }
    } else {
        writeHeader(out, 0, -666, -666);
        return;
    }

    int64_t lastRowPosition = 0;
    for (int i = 0; i <= last; i++) {
        const pqxx::row& row = result[i];
        const auto tuple = rowToTuple<Tuple>(row);
        char allocator_buffer[1000000]; // should be more than enough for any row
        // this looks like it generates bad code so I might replace this with something simpler
        std::pmr::monotonic_buffer_resource resource(allocator_buffer, sizeof(allocator_buffer));
        std::pmr::vector<char> buffer{&resource};
        buffer.reserve(sizeof(allocator_buffer));

        serializeTupleToBuffer(tuple, buffer);

        lastRowPosition = out.tellp();
        out.write(buffer.data(), buffer.size());
    }

    out.seekp(Header::lastRowPosOffset());
    const std::array chars = toCharArray(lastRowPosition);
    out.write(chars.data(), chars.size());
    // cursor no longer points to the end of the file
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


template<typename TABLE>
std::optional<typename TABLE::tuple> readNewestRow(const fs::path& file) {
    std::ifstream in{file, std::ios_base::in | std::ios_base::binary};
    in.exceptions(std::ios_base::badbit | std::ios_base::failbit);

    const Header h = Serializable<Header>::deserialize(in);
    h.checkVersion();
    if (h.numRows > 0) {
        if (h.lastRowPos < 0) throw "trolled";
        in.seekg(h.lastRowPos);
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

template<IncrementalTable TABLE>
std::string incrementalGetNewestRowsQuery(const std::string& table, const std::optional<std::string>& oldValue, int64_t limit) {
    std::string columnName{TABLE::table_type::column::name};
    std::stringstream ss;
    ss << "SELECT " << columnNames<TABLE>()  << " FROM " << table;
    if (oldValue) {
        ss << " WHERE " << columnName << " > " << *oldValue;
    }
    ss << " ORDER BY " << columnName << " ASC LIMIT " << std::to_string(limit);

    return ss.str();
}

// Query rows WHERE the_column >= $yesterday_first_row AND the_column <= $yesterday_last_row
template<IncrementalTable TABLE>
std::string incrementalSelectYesterdayQuery(const std::string& table, const std::string& first, const std::string& last) {
    std::string columnName{TABLE::table_type::column::name};
    std::stringstream ss;
    ss
    << "SELECT " << columnNames<TABLE>()  << " FROM " << table
    << " WHERE " << columnName << " >= " << first << " AND " << columnName << " <= " << last
    << " ORDER BY " << columnName << " ASC";

    return ss.str();
}

template<RewriteTable TABLE>
std::string selectAllQuery(const std::string& table) {
    return "SELECT " + columnNames<TABLE>() + " FROM " + table;
}

void backupToday(pqxx::work& transaction, const fs::path& outDir, const fs::path& rootOutput) {

    const auto output = [&]<typename T>(const T& table, const fs::path& file, const std::optional<fs::path>& lastDiff) -> int {
        std::string query;
        if constexpr (IncrementalTable<T>) {
            if (lastDiff.has_value()) {
                const std::optional newestRow = readNewestRow<T>(*lastDiff);
                if (newestRow.has_value()) {
                    using column = typename T::table_type::column;
                    constexpr size_t tupleIndex = TableIndexOf<typename T::base_type>::template get<column>();
                    const auto newest = std::get<tupleIndex>(newestRow.value());

                    query = incrementalGetNewestRowsQuery<T>(table.name, std::to_string(newest), QUERY_LIMIT);
                } else {
                    // this table has no old output data
                    query = incrementalGetNewestRowsQuery<T>(table.name, std::nullopt, QUERY_LIMIT);
                }
            } else {
                query = incrementalGetNewestRowsQuery<T>(table.name, std::nullopt, QUERY_LIMIT);
            }
        } else if constexpr (RewriteTable<T>) {
            query = selectAllQuery<T>(table.name);
        } else {
            throw std::logic_error{"unhandled type"};
        }

        std::cout << query << '\n';
        pqxx::result result = transaction.exec(query); // might want to put this outside of the lambda
        std::cout << result.size() << " rows\n";
        if (result.empty()) return 0;

        outputTable<T>(file, result);

        return result.size();
    };


    const std::optional<fs::path> yesterday = getNewestOutput(rootOutput, outDir);
    std::apply([&]<typename... T>(const T&... table) {
        ([&] {
            const std::optional<fs::path> lastDiff = yesterday.has_value() ? getNewestFileForTable(yesterday.value(), table.name) : std::nullopt;

            const fs::path firstFile = outDir / table.name;
            int rows = output(table, firstFile, lastDiff);
            if constexpr (IncrementalTable<T> && false) {
                int n = 1;
                fs::path lastFile = firstFile;
                while (rows >= QUERY_LIMIT) {
                    const fs::path file = outDir / (table.name + std::to_string(n));
                    rows = output(table, file, lastFile);
                    n++;
                    lastFile = file;
                }
            }
        }(), ...);
    }, tables);
}

// represents all or part of a table's data in memory
template<typename TABLE>
struct FullTable {
    Header header;
    std::vector<typename TABLE::tuple> rows;

    static FullTable readFromFile(const fs::path& file) {
        std::ifstream in{file, std::ios_base::in | std::ios_base::binary};
        in.exceptions(std::ios_base::badbit | std::ios_base::failbit);

        decltype(rows) rowVector;
        const Header h = Serializable<Header>::deserialize(in);
        h.checkVersion();
        for (int i = 0; i < h.numRows; i++) {
            auto tuple = readTuple<typename TABLE::tuple>(in);
            rowVector.push_back(std::move(tuple));
        }

        return FullTable<TABLE>{h, std::move(rowVector)};
    }
};

void part2(pqxx::work& transaction, const fs::path& rootOutput, const fs::path& today) {
    const std::optional<fs::path> yesterday = getNewestOutput(rootOutput, today);
    if (!yesterday.has_value()) return;

    std::apply([&]<typename... T>(const T&... table) {
        ([&] {
            if constexpr (IncrementalTable<T>) {
                const fs::path file = getNewestFileForTable(yesterday.value(), table.name).value();
                FullTable yesterdayData = FullTable<T>::readFromFile(file);
                const Header& h = yesterdayData.header;
                const std::string query = incrementalSelectYesterdayQuery<T>(table.name, std::to_string(h.firstRowKey), std::to_string(h.lastRowKey));
                std::cout << "table = " << table.name << '\n';
                std::cout << "first = " << h.firstRowKey << " second = " << h.lastRowKey << '\n';

                pqxx::result result = transaction.exec(query);
                std::vector<typename T::tuple> tuplesFromQuery;
                for (const auto& row : result) {
                    std::tuple tuple = rowToTuple<typename T::tuple>(row);
                    tuplesFromQuery.push_back(std::move(tuple));
                }
                // At this point there are now 3 copies of the table's data in memory

                const auto uwu = std::min(tuplesFromQuery.size(), yesterdayData.rows.size());
                for (int i = 0; i < uwu; i++) {
                    const auto& a = tuplesFromQuery[i];
                    const auto& b = yesterdayData.rows[i];
                    if (a != b) {
                        if constexpr (std::is_same_v<T, Signs>) {
                            std::cout << "created_at = " << std::get<int64_t>(a) << ", " << std::get<int64_t>(b) << '\n';
                            std::cout << "x = " << std::get<0>(a) << ", " << std::get<0>(b) << '\n';
                        }
                        std::cout << "uh oh stinky " << i << "\n";
                    }
                }
                auto rowNumDif = static_cast<ssize_t>(tuplesFromQuery.size()) - static_cast<ssize_t>(yesterdayData.rows.size());
                //auto querySz = tuplesFromQuery.size();
                //auto yesterdaySz = yesterdayData.rows.size();
                if (rowNumDif > 0) {
                    std::cout << rowNumDif << " new rows!!\n";
                } else if (rowNumDif < 0) {
                    std::cout << "???\n";
                }

            }
        }(), ...);
    }, tables);

}

int main(int argc, char** argv)
{
    try {
        pqxx::connection con;
        std::cout << "Connected to " << con.dbname() << std::endl;

        const auto now = std::chrono::system_clock::now();
        const uint64_t millis = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

        const auto rootOutput = fs::path{"output"};
        const auto out = rootOutput / std::to_string(millis); // output for today
        std::cout << "Creating output directory at " << out << '\n';
        fs::create_directories(out);

        auto t0 = std::chrono::system_clock::now();

        pqxx::work transaction{con};

        backupToday(transaction, out, rootOutput);
        part2(transaction, rootOutput, out);
        auto today = fs::path{"output"} / "1604293536741" / "chat";
        //part2(transaction, rootOutput, out);

        auto t1 = std::chrono::system_clock::now();

        auto time = std::chrono::duration_cast<std::chrono::seconds>(t1 - t0).count();
        std::cout << "Backup took " << time << " seconds to run\n";

        std::cout << "Done.\n";
    }
    catch (std::exception const &e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
}