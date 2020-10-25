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

template<typename T>
concept is_simple = std::has_unique_object_representations_v<T> && std::is_standard_layout_v<T> && std::is_trivial_v<T>;

// TODO: dont just use memcpy
template<typename T> requires is_simple<T>
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

template<>
struct Serializable<std::string_view> {
    static void serialize(std::pmr::vector<char>& vec, std::string_view str) {
        const auto bufSize = sizeof(int32_t) + str.size();
        auto* buf = allocateElements(vec, bufSize);
        const int32_t len = str.size();
        memcpy(buf, &len, sizeof(int32_t));
        memcpy(buf + sizeof(int32_t), &str[0], len);
    }

    // this does not return string_view
    static std::string deserialize(std::ifstream& in) {
        const auto len = Serializable<int32_t>::deserialize(in);
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
        const auto bufSize = sizeof(int32_t) + data.size();
        auto* buf = allocateElements(out, bufSize);
        const int32_t len = data.size();
        memcpy(buf, &len, sizeof(int32_t));
        memcpy(buf + sizeof(int32_t), &data[0], len);
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