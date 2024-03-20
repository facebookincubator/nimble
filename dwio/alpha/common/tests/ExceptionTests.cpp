// Copyright 2004-present Facebook. All Rights Reserved.

#include <gtest/gtest.h>
#include "dwio/alpha/common/Exceptions.h"

using namespace ::facebook;

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
      alpha::AlphaUserError(
          "file1", 23, "func1", "expr1", "err1", "code1", true),
      "AlphaUserError",
      "file1",
      "23",
      "func1",
      "expr1",
      "err1",
      "USER",
      "code1",
      "True");

  verifyException(
      alpha::AlphaInternalError(
          "file2", 24, "func2", "expr2", "err2", "code2", false),
      "AlphaInternalError",
      "file2",
      "24",
      "func2",
      "expr2",
      "err2",
      "INTERNAL",
      "code2",
      "False");

  verifyException(
      alpha::AlphaExternalError(
          "file3", 25, "func3", "expr3", "err3", "code3", true, "source"),
      "AlphaExternalError",
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
    ALPHA_CHECK(a < 3, "error message1");
  } catch (const alpha::AlphaUserError& e) {
    verifyException(
        e,
        "AlphaUserError",
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
    ALPHA_ASSERT(a > 8, "error message2");
  } catch (const alpha::AlphaInternalError& e) {
    verifyException(
        e,
        "AlphaInternalError",
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
    ALPHA_VERIFY_EXTERNAL(
        1 == 2,
        LocalFileSystem,
        alpha::error_code::NotSupported,
        true,
        "error message3");
  } catch (const alpha::AlphaExternalError& e) {
    verifyException(
        e,
        "AlphaExternalError",
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
    ALPHA_UNREACHABLE("error message");
  } catch (const alpha::AlphaInternalError& e) {
    verifyException(
        e,
        "AlphaInternalError",
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
    ALPHA_NOT_IMPLEMENTED("error message7");
  } catch (const alpha::AlphaInternalError& e) {
    verifyException(
        e,
        "AlphaInternalError",
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
    ALPHA_NOT_SUPPORTED("error message6");
  } catch (const alpha::AlphaUserError& e) {
    verifyException(
        e,
        "AlphaUserError",
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
  auto throwFunc = []() { ALPHA_CHECK(false, "Test."); };
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
          "facebook::alpha::AlphaException::AlphaException"));
}
