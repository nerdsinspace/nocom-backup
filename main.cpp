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
#include <simdjson.h>

#include "pqxx_extensions.h"
#include "tables.h"
#include "serialization.h"


namespace fs = std::filesystem;
using namespace std::literals;


struct Header {
    static constexpr int CURRENT_VERSION = 1;
    uint32_t version = CURRENT_VERSION;
    uint64_t numRows{};
    int64_t lastRowPos = -1; // byte position in the file of the last row
    // TODO: re add bounds from the original query

    static constexpr int SIZE = sizeof(version) + sizeof(numRows) + sizeof(lastRowPos);

    explicit Header(uint32_t v, uint64_t rows, int64_t lastRow):
        version(v), numRows(rows), lastRowPos(lastRow) {}
    explicit Header(uint64_t rows, int64_t lastRow):
        numRows(rows), lastRowPos(lastRow) {}
    explicit Header(uint64_t rows): numRows(rows) {}
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
        Serializable<decltype(h.version)>::serialize(vec, h.version);
        Serializable<decltype(h.numRows)>::serialize(vec, h.numRows);
        Serializable<decltype(h.lastRowPos)>::serialize(vec, h.lastRowPos);
    }

    static Header deserialize(std::ifstream& in) {
        return Header {
          Serializable<uint32_t>::deserialize(in), // version
          Serializable<uint64_t>::deserialize(in), // numRows
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
    Dimensions    {"dimensions"},
    LastByServer  {"last_by_server"},
    PlayerSessions{"player_sessions"},
    Players       {"players"},
    Servers       {"servers"},
    Signs         {"signs"},
    Tracks        {"tracks"},
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

// newest -> oldest
std::vector<fs::path> getAllOutputsSorted(const fs::path& dir) {
    std::vector<fs::path> paths;
    for (auto& p : fs::directory_iterator{dir}) {
        if (pathNameAsTimestamp(p).has_value()) {
            paths.push_back(p);
        } else {
            std::cout << "troll" << std::endl;
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
        return paths.front();
    } else {
        return {};
    }
}


std::optional<fs::path> getNewestFileForTable(std::span<const fs::path> outputsSorted, std::string_view tableName) {
    for (const auto& p : outputsSorted) {
        fs::path file = p / tableName;
        if (fs::exists(file)) return file;
    }
    return {};
}

std::optional<fs::path> getPreviousOutputFile(const fs::path& rootOutput, const fs::path& output, std::string_view tableName) {
    const std::vector<fs::path> allOutputs = getAllOutputsSorted(rootOutput);
    const auto startIt = std::ranges::find(allOutputs, output);

    if (startIt == allOutputs.end()) {
        return {}; // shouldnt happen
    } else {
        for (auto it = startIt + 1; it != allOutputs.end(); it++) {
            fs::path p = *it / tableName;
            if (fs::exists(p)) {
                return p;
            }
        }
        return {};
    }
}

template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

// because I don't like std::apply
template<typename F, typename Tuple>
void visitTuple(F&& f, Tuple&& t) {
    std::apply([&]<typename... T>(T&&... xs) {
        (f(std::forward<T>(xs)), ...);
    }, std::forward<Tuple>(t));
}


template<typename Tuple> requires is_tuple<Tuple>
void serializeTupleToBuffer(const Tuple& tuple, std::pmr::vector<char>& vec) {
    visitTuple([&vec]<typename T>(const T& x) {
        Serializable<T>::serialize(vec, x);
    }, tuple);
}

template<typename T>
void writeObjectToFile(std::ofstream& out, const T& x) {
    char allocator_buffer[1000];
    std::pmr::monotonic_buffer_resource resource(allocator_buffer, sizeof(allocator_buffer));
    std::pmr::vector<char> buffer{&resource};
    buffer.reserve(sizeof(allocator_buffer));
    Serializable<T>::serialize(buffer, x);

    out.write(buffer.data(), buffer.size());
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

std::ifstream newInputStream(const fs::path& file) {
    std::ifstream in(file, std::ios_base::in | std::ios_base::binary);
    in.exceptions(std::ios_base::badbit | std::ios_base::failbit);
    return in;
}

// TODO: add concept for iterable
template<typename TABLE>
int64_t outputTable(const fs::path& output, auto&& iterable) {
    const auto& table = std::get<TABLE>(tables);

    const fs::path file = output / table.name;
    std::ofstream out = newOutputStream(file);
    // reserve space for the header
    char zero[Header::SIZE]{};
    out.write(zero, sizeof(zero));

    int64_t lastRowPosition = 0;
    uint64_t rowsReceived = 0;

    const auto makeHeader = [&] {
        return Header{rowsReceived, lastRowPosition};
    };
    // TODO: measure throughput
    for (const auto& tuple : iterable) {
        //std::cout << tuple << std::endl;
        lastRowPosition = out.tellp();
        writeTuple(out, tuple);
        rowsReceived++;
        if (rowsReceived > 0 && rowsReceived % 10'000'000 == 0) {
            std::cout << rowsReceived << " rows..." << std::endl;
        }
    }
    std::cout << rowsReceived << " rows!!" << std::endl;
    if (rowsReceived > 0) {
        // write header
        out.seekp(0);
        writeObjectToFile(out, makeHeader());
    } else {
        // delete the file if there were no rows
        // (can probably just check if begin() == end() )
        out.close();
        fs::remove(file);
    }

    return rowsReceived;
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
    std::ifstream in = newInputStream(file);

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
std::string incrementalGetNewestRowsQuery(const std::string& table, const std::optional<std::string>& oldValue) {
    std::string columnName{TABLE::table_type::column::name};
    std::stringstream ss;
    ss << "SELECT " << columnNames<TABLE>()  << " FROM " << table;
    if (oldValue) {
        ss << " WHERE " << columnName << " > " << *oldValue;
    }
    ss << " ORDER BY " << columnName << " ASC";

    return ss.str();
}

// Query rows WHERE the_column >= $yesterday_first_row AND the_column <= $yesterday_last_row
template<IncrementalTable TABLE>
std::string incrementalSelectRangeQuery(const std::string& table, const std::optional<key_type<TABLE>>& first, const key_type<TABLE>& last) {
    std::string columnName{TABLE::table_type::column::name};
    std::stringstream ss;
    ss
    << "SELECT " << columnNames<TABLE>()  << " FROM " << table
    //<< " WHERE " << columnName << " >= " << first << " AND " << columnName << " <= " << last
    << " WHERE ";
    if (first.has_value()) {
        ss << columnName << " >= " << std::to_string(first.value()) << " AND ";
    }
    ss << columnName << " <= " << std::to_string(last) << " ORDER BY " << columnName << " ASC";

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

    const auto run = [&]<typename T>(const T& table, const std::optional<fs::path>& lastDiff) {
        std::string query;
        if constexpr (IncrementalTable<T>) {
            if (lastDiff.has_value()) {
                const std::optional newestRow = readNewestRow<T>(*lastDiff);
                if (newestRow.has_value()) {
                    using column = key_column<T>;
                    constexpr size_t tupleIndex = TableIndexOf<typename T::base_type>::template get<column>();
                    const auto newest = std::get<tupleIndex>(newestRow.value());

                    query = incrementalGetNewestRowsQuery<T>(table.name, std::to_string(newest));
                } else {
                    // this table has no old output data (this shouldn't happen)
                    query = incrementalGetNewestRowsQuery<T>(table.name, std::nullopt);
                }
            } else {
                query = incrementalGetNewestRowsQuery<T>(table.name, std::nullopt);
            }
        } else if constexpr (RewriteTable<T>) {
            query = selectAllQuery<T>(table.name);
        } else {
            throw std::logic_error{"unhandled type"};
        }

        std::cout << query << std::endl;

        outputTable<T>(outDir, streamTable<T>(tx, query));
    };


    const std::vector<fs::path> oldOutputs = getOldOutputsSorted(rootOutput, outDir);

    visitTuple([&]<typename T>(const T& table) {
        const std::optional<fs::path> lastDiff = !oldOutputs.empty() ? getNewestFileForTable(oldOutputs, table.name) : std::nullopt;
        run(table, lastDiff);
    }, tables);
}


template<IncrementalTable TABLE>
void runBackupForRange(pqxx::work& tx, const fs::path& output, const std::optional<int64_t>& first, const int64_t last) {
    const auto& table = std::get<TABLE>(tables);
    const auto query = incrementalSelectRangeQuery<TABLE>(table.name, first, last);

    outputTable<TABLE>(output, streamTable<TABLE>(tx, query));
}


// returns true if the type is Incremental and its sorted by column is not unique (basically tables with created_at)
template<IncrementalTable T>
constexpr bool should_ignore_newest_rows() {
    return !T::table_type::is_unique;
}
template<RewriteTable>
constexpr bool should_ignore_newest_rows() {
    return false;
}

template<typename TABLE>
auto& getKeyElement(const typename TABLE::tuple& tuple) {
    using column = key_column<TABLE>;
    constexpr size_t columnIndex = TableIndexOf<typename TABLE::base_type>::template get<column>();

    return std::get<columnIndex>(tuple);
}

// represents all or part of a table's data in memory
template<typename TABLE>
struct TableData {
    const uint64_t firstRowPos;
    const uint64_t lastRowPos; // might make more sense to change this to be the end of the region
    std::vector<typename TABLE::tuple> rows;

    // There must be at least maxRows in the stream
    static TableData readRows(std::ifstream& in, uint64_t maxRows) {
        const uint64_t first = in.tellg();

        decltype(rows) rowVector;
        rowVector.reserve(maxRows);
        if constexpr (should_ignore_newest_rows<TABLE>()) {
            const auto currentPos = in.tellg();
            uint64_t lastRowPosition = currentPos;
            // "unique" is kind of a bad name here
            uint64_t endOfUniqueRows = currentPos;
            uint64_t lastUniqueRow = currentPos;
            key_type<TABLE> lastKey = 0;
            uint32_t lastRows = 0;

            for (uint64_t i = 0; i < maxRows; i++) {
                const int64_t prevRowPos = lastRowPosition;
                lastRowPosition = in.tellg();
                auto tuple = readTuple<typename TABLE::tuple>(in);
                lastRows++;

                if (auto& key = getKeyElement<TABLE>(tuple); key != lastKey) {
                    lastKey = key;
                    endOfUniqueRows = lastRowPosition;
                    lastUniqueRow = prevRowPos;
                    lastRows = 1;
                }

                rowVector.push_back(std::move(tuple));
            }
            if (lastRows != rowVector.size()) {
                rowVector.erase(rowVector.end() - lastRows, rowVector.end());
                in.seekg(endOfUniqueRows);

                return TableData<TABLE>{first, lastUniqueRow, std::move(rowVector)};
            } else { // all of the rows have the same key (this is the case for the last chunk)
                return TableData<TABLE>{first, lastRowPosition, std::move(rowVector)};
            }
        } else {
            uint64_t last = first;
            for (uint64_t i = 0; i < maxRows; i++) {
                last = in.tellg();
                auto tuple = readTuple<typename TABLE::tuple>(in);
                rowVector.push_back(std::move(tuple));
            }

            return TableData<TABLE>{first, last, std::move(rowVector)};
        }
    }
};


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

    for (const auto& i : a) {
        if (std::ranges::none_of(b, equalTo(&i))) {
            return false;
        }
    }
    return true;
}

template<IncrementalTable TABLE>
auto getLastRowOfFile(const fs::path& file) {
    std::ifstream in = newInputStream(file);
    const Header h = Serializable<Header>::deserialize(in);
    in.seekg(h.lastRowPos);
    return readTuple<typename TABLE::tuple>(in);
}

void part2(pqxx::work& tx, const fs::path& rootOutput, const fs::path& today) {
    const std::optional<fs::path> yesterday = getNewestOutput(rootOutput, today);
    if (!yesterday.has_value()) return;

    visitTuple(overloaded {
        [&]<IncrementalTable T>(const T& table) {
            const fs::path file = yesterday.value() / table.name;
            // If there were no new rows in a table we do not output a file.
            // Because we can not check if an empty file is correct because we can not put an upper bound on the query.
            if (!fs::exists(file)) return;
            std::cout << "checking " << file << std::endl;

            std::ifstream stream = newInputStream(file);
            const auto h = Serializable<Header>::deserialize(stream);

            auto currentPos = stream.tellg();
            const std::optional<fs::path> previousOutputFile = getPreviousOutputFile(rootOutput, yesterday.value(), table.name); // 2 days old
            const std::optional<key_type<T>> first = previousOutputFile.has_value() ?
                 std::optional{getKeyElement<T>(getLastRowOfFile<T>(previousOutputFile.value())) + 1}
               : std::nullopt;

            stream.seekg(h.lastRowPos);
            const auto last = getKeyElement<T>(readTuple<typename T::tuple>(stream));
            stream.seekg(currentPos);

            // TODO: check if file has no rows (shouldn't ever happen)

            std::optional<key_type<T>> prevChunkLastKey; // null on first iteration
            for (uint64_t rowsRead = 0; rowsRead < h.numRows;) {
                constexpr uint64_t ROW_LIMIT = 10'000'000;

                const auto n = std::min(h.numRows - rowsRead, ROW_LIMIT);
                // will ignore the last few rows
                const auto fileData = TableData<T>::readRows(stream, n);
                const auto& fileRows = fileData.rows;
                rowsRead += fileRows.size();
                std::cout << "fetched " << fileRows.size() << " rows from file" << std::endl;
                std::cout << "rowsRead = " << rowsRead << std::endl;

                // data is in ascending order
                const std::optional firstKey = prevChunkLastKey.has_value() ? (*prevChunkLastKey + 1) : first;
                const auto& lastKey = getKeyElement<T>(fileRows.back());

                std::cout << "table = " << table.name << std::endl;
                std::cout << "first = " << (firstKey.has_value() ? std::to_string(*firstKey) : std::string{"null"}) << " second = " << lastKey << std::endl;
                if (firstKey.has_value() && *firstKey > lastKey) {
                    throw std::runtime_error{"firstKey > lastKey"};
                }

                const std::string query = incrementalSelectRangeQuery<T>(table.name, firstKey, lastKey);
                prevChunkLastKey = getKeyElement<T>(fileRows.back());
                std::cout << "retry = " << query << std::endl;


                std::vector<typename T::tuple> tuplesFromQuery; // this is the definitely correct data
                for (auto tuple : streamTable<T>(tx, query)) {
                    tuplesFromQuery.push_back(std::move(tuple));
                }
                // At this point there are now 2 copies of the table's data in memory

                bool oldDataGood = true;
                const auto smallerSize = std::min(tuplesFromQuery.size(), fileRows.size());
                for (uint64_t i = 0; i < smallerSize;) {
                    const auto& key = getKeyElement<T>(tuplesFromQuery[i]);
                    if (getKeyElement<T>(fileRows[i]) != key) {
                        std::cout << "keys not lined up" << std::endl;
                        oldDataGood = false;
                        break;
                    }

                    const std::span nextA = nextRangeForKey<T>({tuplesFromQuery.begin() + i, tuplesFromQuery.begin() + smallerSize}, key);
                    const std::span nextB = nextRangeForKey<T>({fileRows.begin() + i, fileRows.begin() + smallerSize}, key);
                    if (nextA.size() != nextB.size()) {
                        std::cout << "ranges for key " << key << " not the same size" << std::endl;
                        oldDataGood = false;
                        break;
                    }
                    if (!unorderedEqual(nextA, nextB)) {
                        // TODO: this usually happens at the last chunk and is normal
                        std::cout << "ranges for key " << key << " not equivalent" << std::endl;
                        oldDataGood = false;
                        break;
                    }

                    i += nextA.size();
                }

                const ssize_t rowNumDif = static_cast<ssize_t>(tuplesFromQuery.size()) - static_cast<ssize_t>(fileRows.size());
                if (rowNumDif > 0) {
                    std::cout << rowNumDif << " new rows!!" << std::endl;
                } else if (rowNumDif < 0) {
                    // we somehow LOST rows??
                    std::cout << "We somehow lost " << -rowNumDif << " rows in an append only table" << std::endl;

                    throw std::runtime_error{"UHH OH STINKY WE LOST DATA THIS IS NOT GOOD!!!"};
                }
                oldDataGood &= (rowNumDif == 0);

                if (!oldDataGood) {
                    std::cout << "yay rerunning backup for " << table.name << "!" << std::endl;
                    const bool isLastChunk = rowsRead >= h.numRows;
                    // If the last chunk is bad then rewrite in place, else we just redo the whole file
                    if (isLastChunk) {
                        std::cout << "fixing last chunk" << std::endl;
                        // this is close to the end and shouldn't be logged
                        std::ofstream out = newOutputStream(file);
                        out.seekp(fileData.firstRowPos);
                        uint64_t lastRowPos = 0;
                        for (const auto& tuple : tuplesFromQuery) {
                            lastRowPos = out.tellp();
                            writeTuple(out, tuple);
                        }
                        out.seekp(Header::lastRowPosOffset());

                        writeObjectToFile(out, lastRowPos);
                    } else {
                        std::cout << "rewriting the whole file" << std::endl;
                        runBackupForRange<T>(tx, yesterday.value(), first, last);
                    }
                    break;
                } else {
                    // we good
                }
            }
        },
        [](RewriteTable auto&) {}
    }, tables);
}

void readChatTable(pqxx::work& tx) {
    using namespace simdjson;
    using namespace string_view_literals;
    ondemand::parser parser;

    uint64_t bytes = 0;
    uint64_t dups = 0;
    uint64_t unique = 0;
    std::string prevData; // unparsed
    uint64_t prevCreatedAt = 0;
    for (auto [data, reportedBy, createdAt, serverId] : tx.stream<std::string, int32_t, int64_t, int16_t>("SELECT data, reported_by, created_at, server_id FROM chat WHERE chat_type=0 ORDER BY created_at")) {
    //for (auto [data, type, reportedBy, createdAt, serverId] : streamTable<Chat>(tx, "SELECT * FROM chat WHERE chat_type=0 ORDER BY created_at")) {
        bytes += data.size();
        if (createdAt - prevCreatedAt < 1000 && data == prevData) {
            // duplicate if it's the same data and received in the same second
            dups++;
        } else {
            unique++;
            padded_string json{data};
            ondemand::document nbt = parser.iterate(json);
            ondemand::array extra;
            auto error = nbt["extra"sv].get(extra);
            if (error == SUCCESS) { // this is an in game chat message (death message or player message)
                std::string out;
                for (auto object : extra) {
                    auto text = std::string_view(object["text"sv]);
                    out += text;
                }
                if (out.find("whispers:") != std::string::npos) {
                    std::cout << data << '\n';
                }
            } else {
                std::string_view text = std::string_view(nbt["text"sv]);
            }
        }

        prevCreatedAt = createdAt;
        prevData = std::move(data);
        //std::cout << '\n';
        //std::cout << data << '\n';
    }
    std::cout << "Read " << bytes << " bytes\n";
    std::cout << dups << " duplicates\n";
    std::cout << unique << " unique messages\n";
}

int main(int argc, char** argv)
{
    try {
        pqxx::connection con{"postgres://nocom:<redacted>>@localhost:5432/nocom?sslmode=disable"};
        std::cout << "Connected to " << con.dbname() << std::endl;

        /*const auto now = std::chrono::system_clock::now();
        const uint64_t millis = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

        const auto rootOutput = fs::path{"output"};
        const auto out = rootOutput / std::to_string(millis); // output for today
        std::cout << "Creating output directory at " << out << std::endl;
        fs::create_directories(out);*/

        auto t0 = std::chrono::system_clock::now();

        pqxx::work transaction{con};

        //backupToday(transaction, out, rootOutput);
        //part2(transaction, rootOutput, out);
        readChatTable(transaction);

        auto t1 = std::chrono::system_clock::now();

        auto time = std::chrono::duration_cast<std::chrono::seconds>(t1 - t0).count();
        std::cout << "Backup took " << time << " seconds to run" << std::endl;

        std::cout << "Done." << std::endl;
    }
    catch (std::exception const &e) {
        std::cerr << e.what() << std::endl;
        return 1;
    }
}
