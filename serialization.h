#pragma once

#include <type_traits>
#include <vector>
#include <memory_resource>
#include <fstream>

#include "pqxx_extensions.h"

template<typename T>
struct Serializable;

template<typename T>
T* allocateElements(std::pmr::vector<T>& vec, size_t size) {
    vec.resize(vec.size() + size);
    return &vec.back() - (size - 1);
}


template<typename T> requires std::is_floating_point_v<T>
struct Serializable<T> {
    static void serialize(std::pmr::vector<char>& vec, T x) {
        auto* buf = allocateElements(vec, sizeof(x));
        memcpy(buf, &x, sizeof(x));
    }

    static T deserialize(std::ifstream& in) {
        T out;
        in.read(reinterpret_cast<char*>(&out), sizeof(T));
        return out;
    }
};

// Big endian (same as java)
template<typename T> requires std::is_integral_v<T>
struct Serializable<T> {
    static void serialize(std::pmr::vector<char>& vec, T x) {
        auto* buf = allocateElements(vec, sizeof(T));
        /*if (sizeof(T) == 1) {

        } else if constexpr (sizeof(T) == 2) {

        } else if constexpr (sizeof(T) == 4) {

        } else if constexpr (sizeof(T) == 8) {

        }*/
        [&]<size_t... I>(std::index_sequence<I...>) {
            ((buf[I] = (x >> (((sizeof(T) - 1) - I) * 8)) & 0xFF), ...);
        }(std::make_index_sequence<sizeof(T)>{});
    }

    static T deserialize(std::ifstream& in) {
        char bytes[sizeof(T)];
        in.read(bytes, sizeof(T));

        //if constexpr (sizeof(T) == 1) {
        //    return static_cast<T>(bytes[0]);
        //} else if constexpr (sizeof(T) == 2) {
        //    return bytes[0] << 8 | bytes[1];
        //} else if constexpr (sizeof(T) == 4) {
        //    return bytes[0] << 24 | bytes[1] << 16 | bytes[2] << 8 | bytes[3];
        //} else if constexpr (sizeof(T) == 8) {
        //    return bytes[0] << 56 | bytes[1] << 48 | bytes[2] << 40 | bytes[3] << 32 | bytes[4] << 24 | bytes[5] << 16 | bytes[6] << 8 | bytes[7];
        //}

        auto out =  [&]<size_t... I>(std::index_sequence<I...>) {
           return (( static_cast<T>(bytes[I]) << (((sizeof(T) - 1) - I) * 8) ) | ...);
        }(std::make_index_sequence<sizeof(T)>{});

        return out;
    }
};

template<typename E> requires std::is_enum_v<E>
struct Serializable<E> {
    using T = std::underlying_type_t<E>;
    static void serialize(std::pmr::vector<char>& vec, E x) {
        Serializable<T>::serialize(vec, static_cast<T>(x));
    }

    static E deserialize(std::ifstream& in) {
        return static_cast<E>(Serializable<T>::deserialize(in));
    }
};

template<>
struct Serializable<UUID> {
    static void serialize(std::pmr::vector<char>& vec, const UUID& x) {
        auto* buf = allocateElements(vec, sizeof(x.bytes));
    }

    static UUID deserialize(std::ifstream& in) {
        UUID out{};
        in.read(reinterpret_cast<char*>(out.bytes), sizeof(out.bytes));
        return out;
    }
};

template<>
struct Serializable<std::string_view> {
    static void serialize(std::pmr::vector<char>& vec, std::string_view str) {
        const auto len = str.size();
        Serializable<int32_t>::serialize(vec, len);

        auto* buf = allocateElements(vec, len);
        memcpy(buf, &str[0], len);
    }

    // this does not return string_view
    static std::string deserialize(std::ifstream& in) {
        const auto len = Serializable<int32_t>::deserialize(in);
        auto cursor = in.tellg();
        std::string out;
        out.resize(len);
        in.read(out.data(), len);
        return out;
    }
};

// could template this on any type convertible to string_view
template<>
struct Serializable<std::string> : Serializable<std::string_view> {};

template<typename T>
struct Serializable<std::optional<T>> {
    static void serialize(std::pmr::vector<char>& vec, const std::optional<T>& optional) {
        Serializable<bool>::serialize(vec, optional.has_value());
        if (optional) Serializable<T>::serialize(vec, *optional);
    }

    static std::optional<T> deserialize(std::ifstream& in){
        const bool exists = Serializable<bool>::deserialize(in);
        if (exists) {
            return Serializable<T>::deserialize(in);
        } else {
            return {};
        }
    }
};

template<>
struct Serializable<placeholder> {
    static void serialize(std::pmr::vector<char>& vec, placeholder) {}

    static placeholder deserialize(std::ifstream& in) {
        return {};
    }
};

template<>
struct Serializable<pqxx::binarystring> {
    static void serialize(std::pmr::vector<char>& out, const pqxx::binarystring& data) {
        const int32_t len = data.size();
        Serializable<int32_t>::serialize(out, len);

        const auto bufSize = data.size();
        auto* buf = allocateElements(out, bufSize);
        memcpy(buf, &data[0], len);
    }

    static pqxx::binarystring deserialize(std::ifstream& in) {
        const auto len = Serializable<int32_t>::deserialize(in);
        using buf_type = pqxx::binarystring::value_type;
        using size_type = pqxx::binarystring::size_type;
        auto* buf = new buf_type[len];
        in.read(reinterpret_cast<char*>(buf), len);

        // binarystring uses shared_ptr
        auto shared = std::shared_ptr<buf_type>{buf};
        return pqxx::binarystring{std::move(shared), static_cast<size_type>(len)};
    }
};