// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <map>

namespace pegasus {
namespace test {

class onebox_controller
{
public:
    onebox_controller();

    // ./run.sh start_onebox -r "replica_count" -m "meta_count"
    void start_onebox(int meta_count = 3, int replica_count = 5);

    void stop_onebox();

    void clear_onebox();

    // run run.sh command `cmd` with specified arguments `args`.
    // eg., to run `./run.sh start_onebox -r 3 -m 3`, call
    // `run_command("start_onebox", {{"-r", "3"}, {"-m", "3"}});`
    void run_command(
        const std::string &cmd,
        const std::map<std::string, std::string> &args = std::map<std::string, std::string>());
};

} // namespace test
} // namespace pegasus
