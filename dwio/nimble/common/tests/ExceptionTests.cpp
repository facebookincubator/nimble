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
#include <gtest/gtest.h>
#include "dwio/nimble/common/Exceptions.h"

namespace facebook {
namespace {

template <typename T>
void verifyException(
    const T& e,
    const std::string& exceptionName,
    const std::string& fileName,
    const std::string& fileLine,
    const std::string& functionName,
    const std::string& failingExpression,
    const std::string& errorMessage,
    const std::string& errorSource,
    const std::string& errorCode,
    const std::string& retryable,
    const std::string& additionalMessage = "") {
  EXPECT_EQ(fileName, e.fileName());
  if (!fileLine.empty()) {
    EXPECT_EQ(fileLine, folly::to<std::string>(e.fileLine()));
  }
  EXPECT_EQ(functionName, e.functionName());
  EXPECT_EQ(failingExpression, e.failingExpression());
  EXPECT_EQ(errorMessage, e.errorMessage());
  EXPECT_EQ(errorSource, e.errorSource());
  EXPECT_EQ(errorCode, e.errorCode());
  EXPECT_EQ(retryable, e.retryable() ? "True" : "False");

  EXPECT_NE(
      std::string(e.what()).find(exceptionName + "\n"), std::string::npos);
  EXPECT_NE(
      std::string(e.what()).find("Error Source: " + errorSource + "\n"),
      std::string::npos);
  EXPECT_NE(
      std::string(e.what()).find("Error Code: " + errorCode + "\n"),
      std::string::npos);
  if (!errorMessage.empty()) {
    EXPECT_NE(
        std::string(e.what()).find("Error Message: " + errorMessage + "\n"),
        std::string::npos);
  }
  EXPECT_NE(
      std::string(e.what()).find("Retryable: " + retryable + "\n"),
      std::string::npos);
  EXPECT_NE(
      std::string(e.what()).find(
          "Location: " +
          folly::to<std::string>(functionName, '@', fileName, ':', fileLine)),
      std::string::npos);
  if (!failingExpression.empty()) {
    EXPECT_NE(
        std::string(e.what()).find("Expression: " + failingExpression + "\n"),
        std::string::npos);
  }
  EXPECT_NE(std::string(e.what()).find("Stack Trace:\n"), std::string::npos);

  if (!additionalMessage.empty()) {
    EXPECT_NE(std::string(e.what()).find(additionalMessage), std::string::npos);
  }
}

TEST(ExceptionTests, Format) {
  verifyException(
      nimble::NimbleUserError(
          "file1", 23, "func1", "expr1", "err1", "code1", true),
      "NimbleUserError",
      "file1",
      "23",
      "func1",
      "expr1",
      "err1",
      "USER",
      "code1",
      "True");

  verifyException(
      nimble::NimbleInternalError(
          "file2", 24, "func2", "expr2", "err2", "code2", false),
      "NimbleInternalError",
      "file2",
      "24",
      "func2",
      "expr2",
      "err2",
      "INTERNAL",
      "code2",
      "False");

  verifyException(
      nimble::NimbleExternalError(
          "file3", 25, "func3", "expr3", "err3", "code3", true, "source"),
      "NimbleExternalError",
      "file3",
      "25",
      "func3",
      "expr3",
      "err3",
      "EXTERNAL",
      "code3",
      "True");
}

TEST(ExceptionTests, Check) {
  int a = 5;
  try {
    NIMBLE_CHECK(a < 3, "error message1");
  } catch (const nimble::NimbleUserError& e) {
    verifyException(
        e,
        "NimbleUserError",
        __FILE__,
        "",
        "TestBody",
        "a < 3",
        "error message1",
        "USER",
        "INVALID_ARGUMENT",
        "False");
  }
}

TEST(ExceptionTests, Assert) {
  int a = 5;
  try {
    NIMBLE_ASSERT(a > 8, "error message2");
  } catch (const nimble::NimbleInternalError& e) {
    verifyException(
        e,
        "NimbleInternalError",
        __FILE__,
        "",
        "TestBody",
        "a > 8",
        "error message2",
        "INTERNAL",
        "INVALID_STATE",
        "False");
  }
}

TEST(ExceptionTests, Verify) {
  try {
    NIMBLE_VERIFY_EXTERNAL(
        1 == 2,
        LocalFileSystem,
        nimble::error_code::NotSupported,
        true,
        "error message3");
  } catch (const nimble::NimbleExternalError& e) {
    verifyException(
        e,
        "NimbleExternalError",
        __FILE__,
        "",
        "TestBody",
        "1 == 2",
        "error message3",
        "EXTERNAL",
        "NOT_SUPPORTED",
        "True",
        "External Source: FILE_SYSTEM");
  }
}

TEST(ExceptionTests, Unreachable) {
  try {
    NIMBLE_UNREACHABLE("error message");
  } catch (const nimble::NimbleInternalError& e) {
    verifyException(
        e,
        "NimbleInternalError",
        __FILE__,
        "",
        "TestBody",
        "",
        "error message",
        "INTERNAL",
        "UNREACHABLE_CODE",
        "False");
  }
}

TEST(ExceptionTests, NotImplemented) {
  try {
    NIMBLE_NOT_IMPLEMENTED("error message7");
  } catch (const nimble::NimbleInternalError& e) {
    verifyException(
        e,
        "NimbleInternalError",
        __FILE__,
        "",
        "TestBody",
        "",
        "error message7",
        "INTERNAL",
        "NOT_IMPLEMENTED",
        "False");
  }
}

TEST(ExceptionTests, NotSupported) {
  try {
    NIMBLE_NOT_SUPPORTED("error message6");
  } catch (const nimble::NimbleUserError& e) {
    verifyException(
        e,
        "NimbleUserError",
        __FILE__,
        "",
        "TestBody",
        "",
        "error message6",
        "USER",
        "NOT_SUPPORTED",
        "False");
  }
}

TEST(ExceptionTests, StackTraceThreads) {
  // Make sure captured stack trace doesn't need anything from thread local
  // storage
  std::exception_ptr e;
  auto throwFunc = []() { NIMBLE_CHECK(false, "Test."); };
  std::thread t([&]() {
    try {
      throwFunc();
    } catch (...) {
      e = std::current_exception();
    }
  });

  t.join();

  ASSERT_NE(nullptr, e);
  EXPECT_NE(
      std::string::npos,
      folly::exceptionStr(e).find(
          "facebook::nimble::NimbleException::NimbleException"));
}

TEST(ExceptionTests, Context) {
  auto messageFunc = [](velox::VeloxException::Type exceptionType, void* arg) {
    auto msg = *static_cast<const std::string*>(arg);
    switch (exceptionType) {
      case velox::VeloxException::Type::kUser:
        return fmt::format("USER {}", msg);
      case velox::VeloxException::Type::kSystem:
        return fmt::format("SYSTEM {}", msg);
    }
  };
  std::string context1Message = "1";
  velox::ExceptionContextSetter context1({messageFunc, &context1Message, true});
  std::string context2Message = "2";
  velox::ExceptionContextSetter context2(
      {messageFunc, &context2Message, false});
  std::string context3Message = "3";
  velox::ExceptionContextSetter context3(
      {messageFunc, &context3Message, false});

  try {
    NIMBLE_NOT_SUPPORTED("");
    FAIL();
  } catch (const nimble::NimbleException& e) {
    ASSERT_EQ(e.context(), "USER 3 USER 1");
    ASSERT_NE(
        std::string(e.what()).find("Context: " + e.context()),
        std::string::npos);
  }

  try {
    NIMBLE_UNKNOWN("");
    FAIL();
  } catch (const nimble::NimbleException& e) {
    ASSERT_EQ(e.context(), "SYSTEM 3 SYSTEM 1");
    ASSERT_NE(
        std::string(e.what()).find("Context: " + e.context()),
        std::string::npos);
  }
}

} // namespace
} // namespace facebook
