#include <iostream>
#include <string>
#include <variant>
#include <filesystem>
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
    {"blocks",          Incremental{"created_at"}},
    {"chat",            Incremental{"created_at"}},
    {"dimensions",      Incremental{"ordinal"}},
    {"hits",            Incremental{"id"}},
    {"last_by_server",  Rewrite{}},
    {"player_sessions", Rewrite{}},
    {"players",         Rewrite{}},
    {"servers",         Incremental{"id"}},
    {"signs",           Incremental{"created_at"}},
    {"tracks",          Rewrite{}}
};

std::string selectNewestQuery(const std::string& table, const Incremental& inc, const std::string& oldValue) {
    return "SELECT * FROM "s + table + "WHERE "s + inc.column + " > "s + oldValue;
}

std::string selectAllQuery(const std::string& table) {
    return "SELECT * FROM "s + table;
}

void outputTable(const fs::path& file, const pqxx::result& result) {
    std::cout << "Outputting table to " << file.string() << '\n';
}

void runBackup(pqxx::connection& db, const fs::path& rootOutput, const Table& table) {
    const auto now = std::chrono::system_clock::now();
    const uint64_t millis = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    const fs::path outDir = rootOutput / std::to_string(millis);
    std::cout << "Creating output directory at " << outDir << '\n';

    std::string query;
    std::visit(overloaded {
        [&](const Incremental& inc) {
            query = selectNewestQuery(table.name, inc, "666");
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
        pqxx::connection C;
        std::cout << "Connected to " << C.dbname() << std::endl;
        pqxx::work W{C};

        pqxx::result R = W.exec("select max(id) from hits");

        std::cout << "Found " << R.size() << " results:\n";
        for (auto row: R)
            std::cout << row[0].c_str() << '\n';

        W.commit();
        std::cout << "OK.\n";
    }
    catch (std::exception const &e)
    {
        std::cerr << e.what() << '\n';
        return 1;
    }
    return 0;
}