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
#include <span>

#include <pqxx/pqxx>
#include <args.hxx>

#include "pqxx_extensions.h"
#include "tables.h"
#include "serialization.h"


namespace fs = std::filesystem;
using namespace std::literals;

constexpr auto ROW_LIMIT = 10'000;//10'000'000;

struct Header {
    static constexpr int CURRENT_VERSION = 1;
    int32_t version = CURRENT_VERSION;
    int32_t numRows{};
    int64_t lastRowPos = -1; // byte position in the file of the last row

    static constexpr int SIZE = sizeof(version) + sizeof(numRows) + sizeof(lastRowPos);

    explicit Header(int v, int rows, int64_t lastRow):
        version(v), numRows(rows), lastRowPos(lastRow) {}
    explicit Header(int rows, int64_t lastRow):
        numRows(rows), lastRowPos(lastRow) {}
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
    }

    static Header deserialize(std::ifstream& in) {
        return Header {
          Serializable<int32_t>::deserialize(in), // version
          Serializable<int32_t>::deserialize(in), // numRows
          Serializable<int64_t>::deserialize(in), // lastRowPos
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

template<IncrementalTable T>
using key_column = typename T::table_type::column;
template<IncrementalTable T>
using key_type = typename T::table_type::column::type;


// TODO: make table names constexpr
const auto tables = std::make_tuple(
    //Dimensions    {"dimensions"},
    //LastByServer  {"last_by_server"},
    ////PlayerSessions{"player_sessions"},
    //Players       {"players"},
    //Servers       {"servers"},
    Signs         {"signs"}
    ////Tracks        {"tracks"},
    //Chat          {"chat"},
    //Hits          {"hits"},
    //Blocks        {"blocks"}
);


std::optional<uint64_t> pathNameAsTimestamp(const fs::path& path) {
    const fs::path name = path.filename();
    try {
        return std::stoull(name.string());
    } catch (...) {
        return {};
    }
}

std::vector<fs::path> getAllOutputsSorted(const fs::path& dir) {
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

std::vector<fs::path> getOldOutputsSorted(const fs::path& dir, const fs::path& today) {
    std::vector<fs::path> paths = getAllOutputsSorted(dir);
    paths.erase(std::remove_if(paths.begin(), paths.end(), [&](const fs::path& p) {
        return p == today;
    }), paths.end());

    return paths;
}

// Today's output has already been created so we want to be able to ignore it
std::optional<fs::path> getNewestOutput(const fs::path& dir, const fs::path& today) {
    std::vector<fs::path> paths = getOldOutputsSorted(dir, today);
    if (!paths.empty()) {
        return paths.back();
    } else {
        return {};
    }
}


// oldest -> newest
std::vector<fs::path> getTableFilesSorted(const fs::path& output, std::string_view tableName) {
    std::vector<fs::path> files;
    for (const auto& p : fs::directory_iterator{output}) {
        const auto name = p.path().filename().string();
        if (name.find(tableName) == 0) {
            files.push_back(p);
        }
    }

    const auto fileNumber = [&](const fs::path& p) -> int {
        auto numStr = p.string().substr(tableName.length());
        return !numStr.empty() ? std::stoi(numStr) : 0;
    };
    std::ranges::sort(files, {}, fileNumber);

    return files;
}

std::optional<fs::path> getNewestFileForTableInOutput(const fs::path& output, std::string_view tableName) {
    std::vector<fs::path> files = getTableFilesSorted(output, tableName);
    if (files.empty()) return {};
    else return files.back();
}

std::optional<fs::path> getNewestFileForTable(const std::vector<fs::path>& outputsSorted, std::string_view tableName) {
    for (const auto& p : outputsSorted) {
        std::optional file = getNewestFileForTableInOutput(p, tableName);
        if (file) return file.value();
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

template<typename T>
void writeToFile(std::ofstream& out, const T& x) {
    char allocator_buffer[1000];
    std::pmr::monotonic_buffer_resource resource(allocator_buffer, sizeof(allocator_buffer));
    std::pmr::vector<char> buffer{&resource};
    buffer.reserve(sizeof(allocator_buffer));
    Serializable<T>::serialize(buffer, x);

    out.write(buffer.data(), buffer.size());
}

void writeHeader(std::ofstream& out, int rows) {
    writeToFile(out, Header{rows});
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
constexpr bool should_ignore_newest_rows() {
    return !T::table_type::is_unique;
}
template<RewriteTable>
constexpr bool should_ignore_newest_rows() {
    return false;
}

template<IncrementalTable TABLE> requires (should_ignore_newest_rows<TABLE>())
int getEndIndex(const pqxx::result& result) {
    using column = key_column<TABLE>;
    using T = key_type<TABLE>;
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

template<typename... Ts>
void writeTuple(std::ofstream& out, const std::tuple<Ts...>& tuple) {
    char allocator_buffer[1000000]; // should be more than enough for any row
    // this looks like it generates bad code so I might replace this with something simpler
    std::pmr::monotonic_buffer_resource resource(allocator_buffer, sizeof(allocator_buffer));
    std::pmr::vector<char> buffer{&resource};
    buffer.reserve(sizeof(allocator_buffer));

    serializeTupleToBuffer(tuple, buffer);

    out.write(buffer.data(), buffer.size());
}

std::ofstream newOutputStream(const fs::path& file) {
    std::ofstream out(file, std::ios_base::out | std::ios_base::binary);
    out.exceptions(std::ios_base::badbit | std::ios_base::failbit);
    return out;
}

template<typename TABLE>
void outputTable(const fs::path& file, const pqxx::result& result) {
    if (result.empty()) throw "empty result";
    using Tuple = typename TABLE::tuple;

    std::cout << "Outputting table to " << file.string() << '\n';
    std::ofstream out = newOutputStream(file);


    const int last = getEndIndex<TABLE>(result);
    const int numRows = last + 1;

    if (last >= 0)  {
        writeHeader(out, numRows);
    } else {
        // no rows
        writeHeader(out, 0);
        return;
    }

    int64_t lastRowPosition = 0;
    for (int i = 0; i <= last; i++) {
        const pqxx::row& row = result[i];
        const auto tuple = rowToTuple<Tuple>(row);

        lastRowPosition = out.tellp();
        writeTuple(out, tuple);
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
std::string incrementalSelectRangeQuery(const std::string& table, const std::string& first, const std::string& last) {
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

template<typename TABLE>
auto streamTable(pqxx::work& tx, std::string_view query) {
    using tuple = typename TABLE::tuple;
    return [&]<size_t... I>(std::index_sequence<I...>) {
        return tx.stream<std::tuple_element_t<I, tuple>...>(query);
    }(std::make_index_sequence<std::tuple_size_v<tuple>>{});
}

void backupToday(pqxx::work& tx, const fs::path& outDir, const fs::path& rootOutput) {

    const auto output = [&]<typename T>(const T& table, const fs::path& file, const std::optional<fs::path>& lastDiff) -> int {
        std::string query;
        if constexpr (IncrementalTable<T>) {
            if (lastDiff.has_value()) {
                const std::optional newestRow = readNewestRow<T>(*lastDiff);
                if (newestRow.has_value()) {
                    using column = key_column<T>;
                    constexpr size_t tupleIndex = TableIndexOf<typename T::base_type>::template get<column>();
                    const auto newest = std::get<tupleIndex>(newestRow.value());

                    query = incrementalGetNewestRowsQuery<T>(table.name, std::to_string(newest), ROW_LIMIT);
                } else {
                    // this table has no old output data
                    query = incrementalGetNewestRowsQuery<T>(table.name, std::nullopt, ROW_LIMIT);
                }
            } else {
                query = incrementalGetNewestRowsQuery<T>(table.name, std::nullopt, ROW_LIMIT);
            }
        } else if constexpr (RewriteTable<T>) {
            query = selectAllQuery<T>(table.name);
        } else {
            throw std::logic_error{"unhandled type"};
        }

        std::cout << query << '\n';
        pqxx::result result = tx.exec(query);
        std::cout << result.size() << " rows\n";
        if (result.empty()) return 0;

        outputTable<T>(file, result);

        return result.size();
    };


    const std::vector<fs::path> oldOutputs = getOldOutputsSorted(rootOutput, outDir);
    std::apply([&]<typename... T>(const T&... table) {
        ([&] {
            const std::optional<fs::path> lastDiff = !oldOutputs.empty() ? getNewestFileForTable(oldOutputs, table.name) : std::nullopt;

            const fs::path firstFile = outDir / table.name;
            int rows = output(table, firstFile, lastDiff);
            if constexpr (IncrementalTable<T> && false) {
                int n = 1;
                fs::path lastFile = firstFile;
                while (rows >= ROW_LIMIT) {
                    const fs::path file = outDir / (table.name + std::to_string(n));
                    rows = output(table, file, lastFile);
                    n++;
                    lastFile = file;
                }
            }
        }(), ...);
    }, tables);
}

// TODO: make this more generic and replace outputTable implementation with this
template<IncrementalTable TABLE>
void runBackupForRange(pqxx::work& tx, const fs::path& output, const int64_t begin, const int64_t end) {
    const auto& table = std::get<TABLE>(tables);
    const auto query = incrementalSelectRangeQuery<TABLE>(table.name, std::to_string(begin), std::to_string(end));

    std::ofstream out = newOutputStream(output / table.name);

    int64_t lastRowPosition = 0;
    int n = 0;
    int rowsReceived = 0;
    const auto makeHeader = [&] {
        return Header{rowsReceived, lastRowPosition};
    };
    for (const auto& tuple : streamTable<TABLE>(tx, query)) {
        if (rowsReceived >= ROW_LIMIT) {
            out.seekp(0);
            writeToFile(out, makeHeader());

            out = newOutputStream(output / (table.name + std::to_string(n)));
            const char zero[Header::SIZE]{};
            out.write(zero, sizeof(zero)); // This will be filled later
            rowsReceived = 0;
            n++;
        }
        lastRowPosition = out.tellp();
        writeTuple(out, tuple);
        rowsReceived++;
    }
    // write header
    out.seekp(0);
    writeToFile(out, makeHeader());
}

// represents all or part of a table's data in memory
template<typename TABLE>
struct TableData {
    Header header;
    std::vector<typename TABLE::tuple> rows;

    static TableData readFromFile(const fs::path& file) {
        std::ifstream in{file, std::ios_base::in | std::ios_base::binary};
        in.exceptions(std::ios_base::badbit | std::ios_base::failbit);

        decltype(rows) rowVector;
        const Header h = Serializable<Header>::deserialize(in);
        h.checkVersion();
        for (int i = 0; i < h.numRows; i++) {
            auto tuple = readTuple<typename TABLE::tuple>(in);
            rowVector.push_back(std::move(tuple));
        }

        return TableData<TABLE>{h, std::move(rowVector)};
    }
};

template<IncrementalTable TABLE>
void writeTableData(const fs::path& file, const std::vector<typename TABLE::tuple>& data) {
    std::ofstream out = newOutputStream(file);

    using column = key_column<TABLE>;
    using T = key_type<TABLE>;
    constexpr size_t columnIndex = TableIndexOf<typename TABLE::base_type>::template get<column>();

    const auto element = [](const typename TABLE::tuple& tuple) {
        return std::get<columnIndex>(tuple);
    };

    if (data.empty()) {
        writeHeader(out, 0);
    }  else {
        // element(data.front()), element(data.back())
        writeHeader(out, data.size());

        int64_t lastRowPosition = 0;
        for (const auto& tuple : data) {
            lastRowPosition = out.tellp();
            writeTuple(out, tuple);
        }

        out.seekp(Header::lastRowPosOffset());
        const std::array chars = toCharArray(lastRowPosition);
        out.write(chars.data(), chars.size());
    }
}

template<typename TABLE>
auto& getKeyElement(const typename TABLE::tuple& tuple) {
    using column = key_column<TABLE>;
    using T = key_type<TABLE>;
    constexpr size_t columnIndex = TableIndexOf<typename TABLE::base_type>::template get<column>();

    return std::get<columnIndex>(tuple);
}


template<typename T>
auto equalTo(const T* x) {
    return [x](const T& y) {
        return *x == y;
    };
}

template<typename TABLE>
auto nextRangeForKey(std::span<const typename TABLE::tuple> tuples, const key_type<TABLE>& key) {
    if (tuples.empty()) throw std::logic_error{"empty span"};

    const auto begin = tuples.begin();
    const auto end = std::ranges::find_if_not(tuples, equalTo(&key), [](const auto& tuple) { return getKeyElement<TABLE>(tuple); });

    return std::span{begin, end};
}

// This is technically incorrect and can be slightly wrong when duplicate data is involved but that shouldn't ever be a problem
template<typename T>
bool unorderedEqual(std::span<const T> a, std::span<const T> b) {
    if (a.size() != b.size()) throw std::logic_error{"must be same size"};

    for (const auto& y : b) {
        const auto& uwu = y;
    }

    for (const auto& i : a) {
        if (std::ranges::none_of(b, equalTo(&i))) {
            return false;
        }
    }
    return true;
}

void part2(pqxx::work& tx, const fs::path& rootOutput, const fs::path& today) {
    const std::optional<fs::path> yesterday = getNewestOutput(rootOutput, today);
    if (!yesterday.has_value()) return;

    std::apply([&]<typename... T>(const T&... table) {
        ([&] {
            if constexpr (IncrementalTable<T>) {
                const std::vector<fs::path> outputFiles = getTableFilesSorted(yesterday.value(), table.name);

                // If there were no new rows in a table we do not output a file
                // because we can not check if an empty file is correct because we can not put an upper bound on the query.
                if (outputFiles.empty()) return;

                for (const auto& file : outputFiles) {
                    const TableData yesterdayData = TableData<T>::readFromFile(file); // this is the data we are checking
                    const Header& h = yesterdayData.header;
                    const auto& yesterdayRows = yesterdayData.rows;
                    // TODO: check if data is empty (shouldn't ever happen)
                    // data is in ascending order
                    const auto& firstKey = getKeyElement<T>(yesterdayRows.front());
                    const auto& lastKey = getKeyElement<T>(yesterdayRows.back());
                    if (firstKey > lastKey) {
                        throw "trolled";
                    }
                    const std::string query = incrementalSelectRangeQuery<T>(table.name, std::to_string(firstKey), std::to_string(lastKey));
                    std::cout << "retry = " << query << '\n';

                    std::cout << "table = " << table.name << '\n';
                    std::cout << "first = " << firstKey << " second = " << lastKey << '\n';

                    std::vector<typename T::tuple> tuplesFromQuery; // this is the definitely correct data
                    for (auto tuple : streamTable<T>(tx, query)) {
                        tuplesFromQuery.push_back(std::move(tuple));
                    }
                    // At this point there are now 2 copies of the table's data in memory

                    bool oldDataGood = true;
                    const auto smallerSize = std::min(tuplesFromQuery.size(), yesterdayRows.size());
                    const auto sizeA = tuplesFromQuery.size();
                    const auto sizeB = yesterdayRows.size();
                    for (int i = 0; i < smallerSize;) {
                        const auto& key = getKeyElement<T>(tuplesFromQuery[i]);
                        if (getKeyElement<T>(yesterdayRows[i]) != key) {
                            // TODO: handle data not being same
                            std::cout << "keys not lined up\n";
                            oldDataGood = false;
                            break;
                        }

                        const std::span nextA = nextRangeForKey<T>({tuplesFromQuery.begin() + i, tuplesFromQuery.begin() + smallerSize}, key);
                        const std::span nextB = nextRangeForKey<T>({yesterdayRows.begin() + i, yesterdayRows.begin() + smallerSize}, key);
                        if (nextA.size() != nextB.size()) {
                            std::cout << "ranges for key " << key << " not the same size\n";
                            oldDataGood = false;
                            break;
                        }
                        if (!unorderedEqual(nextA, nextB)) {
                            std::cout << "ranges for key " << key << " not equivalent\n";
                            oldDataGood = false;
                            break;
                        }

                        i += nextA.size();
                    }


                    const auto rowNumDif = static_cast<ssize_t>(tuplesFromQuery.size()) - static_cast<ssize_t>(yesterdayRows.size());
                    if (rowNumDif > 0) {
                        std::cout << rowNumDif << " new rows!!\n";
                    } else if (rowNumDif < 0) {
                        // we somehow LOST rows??
                        std::cout << "We somehow lost " << rowNumDif << " rows\n";
                    }
                    oldDataGood &= (rowNumDif == 0);

                    if (!oldDataGood) {
                        //writeTableData<T>(file, tuplesFromQuery);
                        std::cout << "yay rerunning backup for " << table.name << "!\n";
                        runBackupForRange<T>(tx, today, firstKey, lastKey); // we assume that the file is now correct
                        break;
                    } else {

                    }
                }
            }
        }(), ...);
    }, tables);
}

int main(int argc, char** argv)
{
    //std::cout << orderByColumns<Signs>() << '\n';
    //return 0;
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