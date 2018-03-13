// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "onebox_controller.h"

#include <cstring>
#include <sstream>

#include <dsn/c/api_utilities.h>
#include <dsn/dist/fmt_logging.h>
#include <gflags/gflags.h>

DEFINE_string(run_script_path, "", "the path of run.sh");

namespace pegasus {
namespace test {

onebox_controller::onebox_controller()
{
    dassert(!FLAGS_run_script_path.empty(), "flag run_script_path should not be empty");
}

void onebox_controller::start_onebox(int meta_count, int replica_count)
{
    return run_command("start_onebox",
                       {{"-r", std::to_string(replica_count)}, {"-m", std::to_string(meta_count)}});
}

void onebox_controller::stop_onebox() { return run_command("stop_onebox"); }

void onebox_controller::clear_onebox() { return run_command("clear_onebox"); }

void onebox_controller::run_command(const std::string &cmd,
                                    const std::map<std::string, std::string> &args)
{
    std::stringstream ss;
    ss << "cd " << FLAGS_run_script_path << "; bash run.sh " << cmd << " ";
    for (const auto &arg : args) {
        ss << arg.first << " " << arg.second << " ";
    }

    std::string full_cmd = ss.str();
    ddebug_f("{} command: {}", cmd, full_cmd);
    dassert_f(system(full_cmd.c_str()) == 0,
              "{} encountered errno({}, {})",
              cmd.c_str(),
              errno,
              strerror(errno));
}

} // namespace test
} // namespace pegasus
