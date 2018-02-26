// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <fmt/format.h>
#include <dsn/cpp/address.h>

/// Overloads fmt::format_arg for the basic types in rDSN, so that
/// they can be formated using fmt::format("{}").

namespace fmt {

// Move it to rDSN core if there're other use cases.
inline void
format_arg(fmt::BasicFormatter<char> &f, const char *&format_str, const dsn::rpc_address &p)
{
    f.format(p.to_string());
}

template <typename T>
inline void
format_arg(fmt::BasicFormatter<char> &f, const char *&format_str, const std::vector<T> &vec)
{
    auto &writer = f.writer();
    writer.write("[");

    for (int i = 0; i < vec.size(); i++) {
        if (i == 0) {
            writer.write("{}", vec[i]);
        } else {
            writer.write(", ");
            writer.write("{}", vec[i]);
        }
    }

    writer.write("]");
}

} // namespace fmt
