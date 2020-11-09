#pragma once

#include <string_view>
#include <tuple>
#include <pqxx/pqxx>

#include "pqxx_extensions.h"


// The code assumes int64_t can represent any key column
template<typename Column, bool Unique> requires std::is_integral_v<typename Column::type>
struct Incremental {
    using column = Column; // Column the table is sorted by and will be used in query
    static constexpr bool is_unique = Unique;
};

struct Rewrite {};

template<typename T>
concept RewriteTable = std::is_same_v<typename T::table_type, Rewrite>;

template<typename>
constexpr bool is_incremental = false;
template<typename T, bool unique>
constexpr bool is_incremental<Incremental<T, unique>> = true;

template<typename T>
concept IncrementalTable = is_incremental<typename T::table_type>;

template<typename... Columns> // TODO: make sure there are no duplicate types
struct Table {
    using tuple = std::tuple<typename Columns::type...>;
    using base_type = Table<Columns...>; // Inheritance breaks specialization so we use this

    std::string name;
};

// this might be useless
template<typename T>
struct Column {
    using type = T;
};

template<typename T> requires std::is_integral_v<T>
struct Id : Column<T> {
    static constexpr std::string_view name = "id";
};

using Id16 = Id<int16_t>;
using Id32 = Id<int32_t>;
using Id64 = Id<int64_t>;

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

struct TrackId : Column<std::optional<int32_t>> {
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

struct Nbt : Column<binary> {
    static constexpr std::string_view name = "nbt";
};

struct Hostname : Column<std::string> {
    static constexpr std::string_view name = "hostname";
};

struct Uuid : Column<UUID> {
    static constexpr std::string_view name = "uuid";
};

struct Username : Column<std::optional<std::string>> {
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

struct Hits : Table<Id64, CreatedAt, X, Z, Dimension, ServerId, Legacy, TrackId> {
    //static constexpr std::string_view name = "hits";
    using table_type = Incremental<Id64, true>;
};

struct Blocks : Table<X, Y, Z, BlockState, CreatedAt, Dimension, ServerId> {
    using table_type = Incremental<CreatedAt, false>;
};

struct Tracks : Table<Id32, FirstHitID, LastHitId, UpdatedAt, PrevTrackId, Dimension, ServerId, Legacy> {
    using table_type = Rewrite;
};

struct Signs : Table<X, Y, Z, Nbt, CreatedAt, Dimension, ServerId> {
    using table_type = Incremental<CreatedAt, false>;
};

struct Servers : Table<Id16, Hostname> {
    using table_type = Incremental<Id16, true>;
};

struct Players : Table<Id32, Uuid, Username> {
    using table_type = Rewrite;
};

struct PlayerSessions : Table<PlayerId, ServerId, Join, Leave, Range, Legacy> {
    using table_type = Rewrite;
};

struct LastByServer : Table<ServerId, CreatedAt> {
    using table_type = Rewrite;
};

struct Dimensions : Table<Ordinal, Name> {
    using table_type = Incremental<Ordinal, true>;
};

struct Chat : Table<Data, ChatType, ReportedBy, CreatedAt, ServerId> {
    using table_type = Incremental<CreatedAt, false>;
};
