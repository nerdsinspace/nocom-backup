#include <iostream>
#include <string>
#include <variant>
#include <filesystem>
#include <fstream>
#include <optional>
#include <chrono>
#include <ranges>

#include <pqxx/pqxx>
#include <args.hxx>


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


struct Incremental {
    std::string column; // Column the table is sorted by and will be used in query
};

struct Rewrite {};

struct Table {
    std::string name;
    std::variant<Incremental, Rewrite> info;
};


const std::vector<Table> tables = {
    {"chat",            Incremental{"created_at"}},
    {"dimensions",      Incremental{"ordinal"}},
    {"last_by_server",  Rewrite{}},
    {"player_sessions", Rewrite{}},
    {"players",         Rewrite{}},
    {"servers",         Incremental{"id"}},
    {"signs",           Incremental{"created_at"}},
    {"tracks",          Rewrite{}},
    {"hits",            Incremental{"id"}},
    {"blocks",          Incremental{"created_at"}}
};

using i16 = int16_t;
using i32 = int32_t;
using i64 = int64_t;

struct placeholder {};

using Hits =           std::tuple<i64, i64, i32, i32, i32, i32, i32, bool, i32>;
using Blocks =         std::tuple<i32, i16, i32, i32, i64, i16, i16>;
using Tracks =         std::tuple<i32, i64, i64, i64, std::optional<i32>, i16, i16, bool>;
using Signs =          std::tuple<i32, i16, i32, std::vector<std::byte>, i16, i16>;
using Servers =        std::tuple<i16, std::string>;
using Players =        std::tuple<i32, std::byte[16], std::string>;
using PlayerSessions = std::tuple<i32, i16, std::optional<i64>, placeholder/*range*/, bool>;
using LastByServer =   std::tuple<i16, i64>;
using Dimensions =     std::tuple<i32, std::string>;
using Chat =           std::tuple<std::string/*json*/, i16, i32, i64, i16>;
// TODO: define full schema for sanity checking

enum class UpdateType : char {
    Create,
    Delete
};

std::string selectNewestQuery(const std::string& table, const Incremental& inc, const std::string& oldValue) {
    return "SELECT * FROM "s + table + " WHERE "s + inc.column + " > "s + oldValue;
}

std::string selectAllQuery(const std::string& table) {
    return "SELECT * FROM "s + table;
}

void outputTable(const fs::path& file, const pqxx::result& result) {
    std::cout << "Outputting table to " << file.string() << '\n';
    std::ofstream out(file);
    out << "This table has " << result.size() << " rows uwu\n";
}

void runBackup(pqxx::connection& db, const fs::path& rootOutput, const Table& table) {
    const auto now = std::chrono::system_clock::now();
    const uint64_t millis = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    const fs::path outDir = rootOutput / std::to_string(millis);
    std::cout << "Creating output directory at " << outDir << '\n';
    fs::create_directory(outDir);

    std::string query;
    std::visit(overloaded {
        [&](const Incremental& inc) {
            query = selectNewestQuery(table.name, inc, "-10");
        },
        [&](Rewrite) {
            query = selectAllQuery(table.name);
        }
    }, table.info);


    pqxx::result result = pqxx::work{db}.exec(query);

    outputTable(outDir / table.name, result);
}

int main(int argc, char** argv)
{
    try
    {
        pqxx::connection con;
        std::cout << "Connected to " << con.dbname() << std::endl;

        const fs::path out{"output"};
        fs::create_directories(out);

        for (const Table& table : tables) {
            if (table.name == "dimensions")
                runBackup(con, out, table);
        }

        std::cout << "Done.\n";
    }
    catch (std::exception const &e)
    {
        std::cerr << e.what() << '\n';
        return 1;
    }
    return 0;
}