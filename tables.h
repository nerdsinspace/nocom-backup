#pragma once

#include <string_view>
#include <tuple>
#include <pqxx/pqxx>

#include "pqxx_extensions.h"


struct Incremental {
    std::string column; // Column the table is sorted by and will be used in query
};

struct Rewrite {};

template<typename... Columns>
struct Table {
    using tuple = std::tuple<typename Columns::type...>;
    std::string name;
    std::variant<Incremental, Rewrite> info;
};

// this might be useless
template<typename T>
struct Column {
    using type = T;
};

struct Id : Column<int32_t> {
    static constexpr std::string_view name = "id";
};

struct X : Column<int32_t> {
    static constexpr std::string_view name = "x";
};

struct Y : Column<int16_t> {
    static constexpr std::string_view name = "y";
};

struct Z : Column<int32_t> {
    static constexpr std::string_view name = "z";
};

struct CreatedAt : Column<int64_t> {
    static constexpr std::string_view name = "created_at";
};

struct Dimension : Column<int16_t> {
    static constexpr std::string_view name = "dimension";
};

struct ServerId : Column<int16_t> {
    static constexpr std::string_view name = "server_id";
};

struct Legacy : Column<bool> {
    static constexpr std::string_view name = "legacy";
};

struct TrackId : Column<int32_t> {
    static constexpr std::string_view name = "track_id";
};

struct BlockState : Column<int32_t> {
    static constexpr std::string_view name = "block_state";
};

struct FirstHitID : Column<int64_t> {
    static constexpr std::string_view name = "first_hit_id";
};

struct LastHitId : Column<int64_t> {
    static constexpr std::string_view name = "last_hit_id";
};

struct UpdatedAt : Column<int64_t> {
    static constexpr std::string_view name = "updated_at";
};

struct PrevTrackId : Column<std::optional<int32_t>> {
    static constexpr std::string_view name = "prev_track_id";
};

struct Nbt : Column<pqxx::binarystring> {
    static constexpr std::string_view name = "nbt";
};

struct Servers_Id : Column<int16_t> {
    static constexpr std::string_view name = "id";
};

struct Hostname : Column<std::string> {
    static constexpr std::string_view name = "hostname";
};

struct Uuid : Column<UUID> {
    static constexpr std::string_view name = "uuid";
};

struct Username : Column<std::string> {
    static constexpr std::string_view name = "username";
};

struct PlayerId : Column<int32_t> {
    static constexpr std::string_view name = "player_id";
};

struct Join : Column<int64_t> {
    static constexpr std::string_view name = "\"join\"";
};

struct Leave : Column<std::optional<int64_t>> {
    static constexpr std::string_view name = "leave";
};

struct Range : Column<placeholder> {
    static constexpr std::string_view name = "range";
};

struct Ordinal : Column<int16_t> {
    static constexpr std::string_view name = "ordinal";
};

struct Name : Column<std::string> {
    static constexpr std::string_view name = "name";
};

struct Data : Column<std::string> { // json
    static constexpr std::string_view name = "data";
};

struct ChatType : Column<int16_t> {
    static constexpr std::string_view name = "chat_type";
};

struct ReportedBy : Column<int32_t> {
    static constexpr std::string_view name = "reported_by";
};

using Hits =           Table<Id, CreatedAt, X, Z, Dimension, ServerId, Legacy, TrackId>;
using Blocks =         Table<X, Y, Z, BlockState, CreatedAt, Dimension, ServerId>;
using Tracks =         Table<Id, FirstHitID, LastHitId, UpdatedAt, PrevTrackId, Dimension, ServerId, Legacy>;
using Signs =          Table<X, Y, Z, Nbt, CreatedAt, Dimension, ServerId>;
using Servers =        Table<Servers_Id, Hostname>;
using Players =        Table<Id, Uuid, Username>;
using PlayerSessions = Table<PlayerId, ServerId, Join, Leave, Range, Legacy>;
using LastByServer =   Table<ServerId, CreatedAt>;
using Dimensions =     Table<Ordinal, Name>;
using Chat =           Table<Data, ChatType, ReportedBy, CreatedAt, ServerId>;