#pragma once

#include <pqxx/pqxx>

struct placeholder {
    //placeholder(const auto&) {}
};

// stupid library doesnt support UUIDs
struct UUID {
    unsigned char bytes[16];
};

inline unsigned hex_to_digit(char hex) {
    auto x = static_cast<unsigned char>(hex);
    if (x >= '0' and x <= '9')
        return x - '0';
    else if (x >= 'a' and x <= 'f')
        return 10 + x - 'a';
    else if (x >= 'A' and x <= 'F')
        return 10 + x - 'A';
    else
        throw std::runtime_error{"Invalid hex in bytea."};
}

namespace pqxx {
    template<>
    struct string_traits<placeholder> {
        static placeholder from_string(std::string_view) {
            return {};
        }
    };

    template<>
    struct string_traits<UUID> {

        static UUID from_string(std::string_view text) {
            namespace views = std::ranges::views;
            std::string hex;
            for (char c : text) {
                if (c != '-') hex.push_back(c);
            }
            if (hex.size() != 32) throw std::runtime_error("bad uuid string");

            UUID uuid{};
            for (int i = 0; i < 16; i += 2) {
                auto hi = hex_to_digit(hex[i]), lo = hex_to_digit(hex[i + 1]);

                uuid.bytes[i] = static_cast<unsigned char>((hi << 4) | lo);
            }
            return uuid;
        }
    };
}