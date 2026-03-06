/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <algorithm>
#include <cctype>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "common/config/Flags.h"
#include "common/init/light.h"
#include "dwio/nimble/tools/NimbleDslLib.h"
#include "folly/Singleton.h"
#include "folly/logging/Init.h"
#include "velox/common/base/StatsReporter.h"

using namespace facebook;

namespace {

std::string toUpper(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(), ::toupper);
  return s;
}

std::string trim(const std::string& s) {
  auto start = s.find_first_not_of(" \t\r\n");
  if (start == std::string::npos) {
    return "";
  }
  auto end = s.find_last_not_of(" \t\r\n");
  return s.substr(start, end - start + 1);
}

std::vector<std::string> tokenize(const std::string& input) {
  std::vector<std::string> tokens;
  std::istringstream stream{input};
  std::string token;
  while (stream >> token) {
    // Strip trailing semicolons and commas.
    while (!token.empty() && (token.back() == ';' || token.back() == ',')) {
      token.pop_back();
    }
    if (!token.empty()) {
      tokens.push_back(token);
    }
  }
  return tokens;
}

// Parse a SELECT command.
// SELECT [col1, col2, ...] | * [LIMIT n] [OFFSET n] [STRIPE s] [FROM ...]
void parseAndExecSelect(
    const std::vector<std::string>& tokens,
    nimble::tools::NimbleDslLib& lib) {
  std::vector<std::string> columns;
  uint64_t limit = 20; // Default limit.
  uint64_t offset = 0;
  std::optional<uint32_t> stripeId;

  size_t i = 1; // Skip "SELECT".

  // Parse column list until we hit a keyword or end.
  while (i < tokens.size()) {
    auto upper = toUpper(tokens[i]);
    if (upper == "LIMIT" || upper == "OFFSET" || upper == "STRIPE" ||
        upper == "FROM") {
      break;
    }
    if (upper != "*") {
      columns.push_back(tokens[i]);
    }
    ++i;
  }

  // Parse optional clauses.
  while (i < tokens.size()) {
    auto upper = toUpper(tokens[i]);
    if (upper == "LIMIT" && i + 1 < tokens.size()) {
      limit = std::stoull(tokens[i + 1]);
      i += 2;
    } else if (upper == "OFFSET" && i + 1 < tokens.size()) {
      offset = std::stoull(tokens[i + 1]);
      i += 2;
    } else if (upper == "STRIPE" && i + 1 < tokens.size()) {
      stripeId = std::stoul(tokens[i + 1]);
      i += 2;
    } else if (upper == "FROM") {
      // Accept and ignore FROM clause.
      i += 2;
    } else {
      ++i;
    }
  }

  lib.select(columns, limit, offset, stripeId);
}

void printUsage(std::ostream& out) {
  out << "Usage: nimble_dsl <file>" << std::endl;
  out << std::endl;
  out << "Interactive SQL-like REPL for inspecting Nimble files." << std::endl;
  out << "Opens the given Nimble file and presents a prompt where you can"
      << std::endl;
  out << "run commands to query data, view schema, and inspect metadata."
      << std::endl;
  out << std::endl;
  out << "Type HELP at the prompt for a list of available commands."
      << std::endl;
}

void printHelp(std::ostream& out, bool color) {
  const char* bold = color ? "\033[1m" : "";
  const char* yellow = color ? "\033[33m" : "";
  const char* cyan = color ? "\033[36m" : "";
  const char* dim = color ? "\033[2m" : "";
  const char* reset = color ? "\033[0m" : "";

  out << bold << "Commands" << reset << dim
      << " (case-insensitive, trailing semicolons optional):" << reset
      << std::endl;
  out << std::endl;

  out << "  " << yellow << "SELECT" << reset
      << " * [LIMIT n] [OFFSET n] [STRIPE s]" << std::endl;
  out << "  " << yellow << "SELECT" << reset
      << " col1, col2 [LIMIT n] [OFFSET n] [STRIPE s]" << std::endl;
  out << "      Read and display row data. Default LIMIT is 20." << std::endl;
  out << dim << "      Examples:" << std::endl;
  out << "        SELECT *" << std::endl;
  out << "        SELECT name, age LIMIT 5" << std::endl;
  out << "        SELECT * LIMIT 10 OFFSET 100" << std::endl;
  out << "        SELECT * LIMIT 50 STRIPE 0" << reset << std::endl;
  out << std::endl;

  out << "  " << yellow << "DESCRIBE" << reset << std::endl;
  out << "      Show top-level column names, Velox types, and Nimble stream"
      << std::endl;
  out << "      offsets in a table." << std::endl;
  out << std::endl;

  out << "  " << yellow << "SHOW SCHEMA" << reset << std::endl;
  out << "      Show the full Nimble schema tree including nested types"
      << std::endl;
  out << "      (arrays, maps, rows, flat maps) with stream offsets."
      << std::endl;
  out << std::endl;

  out << "  " << yellow << "SHOW INFO" << reset << std::endl;
  out << "      Show file-level metadata: Nimble version, file size, checksum,"
      << std::endl;
  out << "      stripe count, row count, and user-defined metadata."
      << std::endl;
  out << std::endl;

  out << "  " << yellow << "SHOW STATS" << reset << std::endl;
  out << "      Show per-column statistics: value count, null count, min/max,"
      << std::endl;
  out << "      logical size, and physical size. Requires the file to have"
      << std::endl;
  out << "      been written with vectorized stats enabled." << std::endl;
  out << std::endl;

  out << "  " << yellow << "SHOW STRIPES" << reset << std::endl;
  out << "      Show stripe-level information: stripe ID, byte offset,"
      << std::endl;
  out << "      byte size, and row count for each stripe." << std::endl;
  out << std::endl;

  out << "  " << yellow << "SHOW STREAMS" << reset << " [STRIPE s]"
      << std::endl;
  out << "      Show stream-level information: stream ID, byte offset,"
      << std::endl;
  out << "      byte size, item count, and stream label. Optionally"
      << std::endl;
  out << "      filter to a single stripe." << std::endl;
  out << std::endl;

  out << "  " << yellow << "SHOW HISTOGRAM" << reset << " [STRIPE s]"
      << std::endl;
  out << "      Show encoding histogram: encoding type, data type,"
      << std::endl;
  out << "      compression, instance count, and storage bytes." << std::endl;
  out << std::endl;

  out << "  " << yellow << "SHOW CONTENT" << reset << " <stream_id>"
      << " [STRIPE s]" << std::endl;
  out << "      Show raw decoded content of a specific stream." << std::endl;
  out << std::endl;

  out << "  " << yellow << "SHOW FILE LAYOUT" << reset << std::endl;
  out << "      Show physical file layout: offsets and sizes of all"
      << std::endl;
  out << "      sections (stripes, footer, postscript, etc.)." << std::endl;
  out << std::endl;

  out << "  " << yellow << "SHOW INDEX" << reset << std::endl;
  out << "      Show index information: index columns, sort orders,"
      << std::endl;
  out << "      index groups, and key streams." << std::endl;
  out << std::endl;

  out << "  " << yellow << "SHOW STRIPE GROUPS" << reset << std::endl;
  out << "      Show stripe group metadata: group ID, offset, size,"
      << std::endl;
  out << "      and compression type." << std::endl;
  out << std::endl;

  out << "  " << yellow << "SHOW OPTIONAL SECTIONS" << reset << std::endl;
  out << "      Show optional sections metadata: name, compression,"
      << std::endl;
  out << "      offset, and size." << std::endl;
  out << std::endl;

  out << "  " << cyan << "HELP" << reset << std::endl;
  out << "      Show this help message." << std::endl;
  out << std::endl;

  out << "  " << cyan << "QUIT" << reset << " | " << cyan << "EXIT" << reset
      << " | Ctrl-D" << std::endl;
  out << "      Exit the REPL." << std::endl;
}

bool isColorfulTty() {
  auto isTty = isatty(fileno(stdout));
  if (!isTty) {
    return false;
  }
  auto term = std::getenv("TERM");
  return !(term == nullptr || term[0] == '\0' || strcmp(term, "dumb") == 0);
}

} // namespace

FOLLY_INIT_LOGGING_CONFIG("CRITICAL");
int main(int argc, char* argv[]) {
  facebook::config::Flags::overrideDefault("minloglevel", "5");

  auto init = init::InitFacebookLight{
      &argc, &argv, folly::InitOptions().useGFlags(false)};

  if (argc < 2) {
    printUsage(std::cerr);
    return EXIT_FAILURE;
  }

  std::string filePath = argv[1];

  if (filePath == "--help" || filePath == "-h") {
    printUsage(std::cout);
    return EXIT_SUCCESS;
  }

  bool enableColors = isColorfulTty();

  if (enableColors) {
    // clang-format off
    std::cout << "\033[1m\033[36m"
      "  _   _ _           _     _      \n"
      " | \\ | (_)_ __ ___ | |__ | | ___ \n"
      " |  \\| | | '_ ` _ \\| '_ \\| |/ _ \\\n"
      " | |\\  | | | | | | | |_) | |  __/\n"
      " |_| \\_|_|_| |_| |_|_.__/|_|\\___|\n"
      "\033[0m" << std::endl;
    // clang-format on
    std::cout << " Interactive SQL-like shell for Nimble files." << std::endl;
    std::cout << " Type \033[33mHELP\033[0m for commands, \033[33mQUIT\033[0m"
                 " to exit."
              << std::endl;
    std::cout << std::endl;
  } else {
    // clang-format off
    std::cout <<
      "  _   _ _           _     _      \n"
      " | \\ | (_)_ __ ___ | |__ | | ___ \n"
      " |  \\| | | '_ ` _ \\| '_ \\| |/ _ \\\n"
      " | |\\  | | | | | | | |_) | |  __/\n"
      " |_| \\_|_|_| |_| |_|_.__/|_|\\___|\n"
      << std::endl;
    // clang-format on
    std::cout << " Interactive SQL-like shell for Nimble files." << std::endl;
    std::cout << " Type HELP for commands, QUIT to exit." << std::endl;
    std::cout << std::endl;
  }

  nimble::tools::NimbleDslLib lib{std::cout, enableColors, filePath};

  std::string line;
  while (true) {
    if (enableColors) {
      std::cout << "\033[1m\033[32mnimble>\033[0m " << std::flush;
    } else {
      std::cout << "nimble> " << std::flush;
    }
    if (!std::getline(std::cin, line)) {
      // EOF (Ctrl-D).
      std::cout << std::endl;
      break;
    }

    auto trimmed = trim(line);
    if (trimmed.empty()) {
      continue;
    }

    auto tokens = tokenize(trimmed);
    if (tokens.empty()) {
      continue;
    }

    auto command = toUpper(tokens[0]);

    try {
      if (command == "QUIT" || command == "EXIT") {
        break;
      } else if (command == "HELP") {
        printHelp(std::cout, enableColors);
      } else if (command == "SELECT") {
        parseAndExecSelect(tokens, lib);
      } else if (command == "DESCRIBE") {
        lib.describe();
      } else if (command == "SHOW" && tokens.size() >= 2) {
        auto subcommand = toUpper(tokens[1]);
        if (subcommand == "SCHEMA") {
          lib.showSchema();
        } else if (subcommand == "INFO") {
          lib.showInfo();
        } else if (subcommand == "STATS") {
          lib.showStats();
        } else if (subcommand == "STRIPES") {
          lib.showStripes();
        } else if (subcommand == "STREAMS") {
          std::optional<uint32_t> stripeId;
          if (tokens.size() >= 4 && toUpper(tokens[2]) == "STRIPE") {
            stripeId = std::stoul(tokens[3]);
          }
          lib.showStreams(stripeId);
        } else if (subcommand == "HISTOGRAM") {
          std::optional<uint32_t> stripeId;
          if (tokens.size() >= 4 && toUpper(tokens[2]) == "STRIPE") {
            stripeId = std::stoul(tokens[3]);
          }
          lib.showHistogram(stripeId);
        } else if (subcommand == "CONTENT" && tokens.size() >= 3) {
          auto streamId = static_cast<uint32_t>(std::stoul(tokens[2]));
          std::optional<uint32_t> stripeId;
          if (tokens.size() >= 5 && toUpper(tokens[3]) == "STRIPE") {
            stripeId = std::stoul(tokens[4]);
          }
          lib.showContent(streamId, stripeId);
        } else if (
            subcommand == "FILE" && tokens.size() >= 3 &&
            toUpper(tokens[2]) == "LAYOUT") {
          lib.showFileLayout();
        } else if (subcommand == "INDEX") {
          lib.showIndex();
        } else if (
            subcommand == "STRIPE" && tokens.size() >= 3 &&
            toUpper(tokens[2]) == "GROUPS") {
          lib.showStripeGroups();
        } else if (
            subcommand == "OPTIONAL" && tokens.size() >= 3 &&
            toUpper(tokens[2]) == "SECTIONS") {
          lib.showOptionalSections();
        } else {
          std::cerr << (enableColors ? "\033[31m" : "")
                    << "Unknown SHOW subcommand: " << tokens[1] << ". Try HELP."
                    << (enableColors ? "\033[0m" : "") << std::endl;
        }
      } else {
        std::cerr << (enableColors ? "\033[31m" : "")
                  << "Unknown command: " << tokens[0] << ". Try HELP."
                  << (enableColors ? "\033[0m" : "") << std::endl;
      }
    } catch (const std::exception& e) {
      std::cerr << (enableColors ? "\033[31m" : "") << "Error: " << e.what()
                << (enableColors ? "\033[0m" : "") << std::endl;
    }
  }

  return EXIT_SUCCESS;
}

// NOLINTNEXTLINE(facebook-avoid-non-const-global-variables)
folly::Singleton<facebook::velox::BaseStatsReporter> reporter([]() {
  return new facebook::velox::DummyStatsReporter();
});
