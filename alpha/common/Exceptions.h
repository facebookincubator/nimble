// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#pragma once

#include <glog/logging.h>
#include "folly/FixedString.h"
#include "folly/experimental/symbolizer/Symbolizer.h"
#include "folly/synchronization/CallOnce.h"

// Standard errors used throughout the codebase.

namespace facebook::alpha {

namespace error_source {
using namespace folly::string_literals;

// Errors where the root cause of the problem is either because of bad input
// or an unsupported pattern of use are classified with source USER.
inline constexpr auto User = "USER"_fs;

// Errors where the root cause of the problem is an unexpected internal state in
// the system.
inline constexpr auto Internal = "INTERNAL"_fs;

// Errors where the root cause of the problem is the result of a dependency or
// an environment failures.
inline constexpr auto External = "EXTERNAL"_fs;
} // namespace error_source

namespace error_code {
using namespace folly::string_literals;

// An error raised when an argument verification fails
inline constexpr auto InvalidArgument = "INVALID_ARGUMENT"_fs;

// An error raised when the current state of a component is invalid.
inline constexpr auto InvalidState = "INVALID_STATE"_fs;

// An error raised when unreachable code point was executed.
inline constexpr auto UnreachableCode = "UNREACHABLE_CODE"_fs;

// An error raised when a requested operation is not yet implemented.
inline constexpr auto NotImplemented = "NOT_IMPLEMENTED"_fs;

// An error raised when a requested operation is not supported.
inline constexpr auto NotSupported = "NOT_SUPPORTED"_fs;

// As error raised during encoding optimization, when incompatible encoding is
// attempted.
inline constexpr auto IncompatibleEncoding = "INCOMPATIBLE_ENCODING"_fs;

// Am error raised if a corrupted file is detected
inline constexpr auto CorruptedFile = "CORRUPTED_FILE"_fs;

// We do not know how to classify it yet.
inline constexpr auto Unknown = "UNKNOWN"_fs;
} // namespace error_code

namespace external_source {
using namespace folly::string_literals;

// Warm Storage
inline constexpr auto WarmStorage = "WARM_STORAGE"_fs;

// Local File System
inline constexpr auto LocalFileSystem = "FILE_SYSTEM"_fs;

} // namespace external_source

// Based exception, used by all other Alpha exceptions. Provides common
// functionality for all other exception types.
class AlphaException : public std::exception {
 public:
  explicit AlphaException(
      std::string_view exceptionName,
      const char* fileName,
      size_t fileLine,
      const char* functionName,
      std::string_view failingExpression,
      std::string_view errorMessage,
      std::string_view errorCode,
      bool retryable)
      : exceptionName_{std::move(exceptionName)},
        fileName_{fileName},
        fileLine_{fileLine},
        functionName_{functionName},
        failingExpression_{std::move(failingExpression)},
        errorMessage_{std::move(errorMessage)},
        errorCode_{std::move(errorCode)},
        retryable_{retryable} {
    captureStackTraceFrames();
  }

  const char* what() const throw() {
    try {
      folly::call_once(once_, [&] { finalizeMessage(); });
      return finalizedMessage_.c_str();
    } catch (...) {
      return "<unknown failure in AlphaException::what>";
    }
  }

  const std::string& exceptionName() const {
    return exceptionName_;
  }

  const char* fileName() const {
    return fileName_;
  }

  size_t fileLine() const {
    return fileLine_;
  }

  const char* functionName() const {
    return functionName_;
  }

  const std::string& failingExpression() const {
    return failingExpression_;
  }

  const std::string& errorMessage() const {
    return errorMessage_;
  }

  virtual const std::string_view errorSource() const = 0;

  const std::string& errorCode() const {
    return errorCode_;
  }

  bool retryable() const {
    return retryable_;
  }

 protected:
  virtual void appendMessage(std::string& /* message */) const {}

 private:
  void captureStackTraceFrames() {
    try {
      constexpr size_t skipFrames = 2;
      constexpr size_t maxFrames = 200;
      uintptr_t addresses[maxFrames];
      ssize_t n = folly::symbolizer::getStackTrace(addresses, maxFrames);

      if (n < skipFrames) {
        return;
      }

      exceptionFrames_.assign(addresses + skipFrames, addresses + n);
    } catch (const std::exception& ex) {
      LOG(WARNING) << "Unable to capture stack trace: " << ex.what();
    } catch (...) { // Should never happen, catchall
      LOG(WARNING) << "Unable to capture stack trace.";
    }
  }

  void finalizeMessage() const {
    finalizedMessage_ += exceptionName_;
    finalizedMessage_ += "\nError Source: ";
    finalizedMessage_ += errorSource();
    finalizedMessage_ += "\nError Code: ";
    finalizedMessage_ += errorCode_;
    if (!errorMessage_.empty()) {
      finalizedMessage_ += "\nError Message: ";
      finalizedMessage_ += errorMessage_;
    }
    finalizedMessage_ += "\n";
    appendMessage(finalizedMessage_);
    finalizedMessage_ += "Retryable: ";
    finalizedMessage_ += retryable_ ? "True" : "False";
    finalizedMessage_ += "\nLocation: ";
    finalizedMessage_ += functionName_;
    finalizedMessage_ += "@";
    finalizedMessage_ += fileName_;
    finalizedMessage_ += ":";
    finalizedMessage_ += folly::to<std::string>(fileLine_);

    if (!failingExpression_.empty()) {
      finalizedMessage_ += "\nExpression: ";
      finalizedMessage_ += failingExpression_;
    }

    if (LIKELY(!exceptionFrames_.empty())) {
      std::vector<folly::symbolizer::SymbolizedFrame> symbolizedFrames;
      symbolizedFrames.resize(exceptionFrames_.size());

      folly::symbolizer::Symbolizer symbolizer{
          folly::symbolizer::LocationInfoMode::FULL};
      symbolizer.symbolize(
          exceptionFrames_.data(),
          symbolizedFrames.data(),
          symbolizedFrames.size());

      folly::symbolizer::StringSymbolizePrinter printer{
          folly::symbolizer::StringSymbolizePrinter::COLOR};
      printer.println(symbolizedFrames.data(), symbolizedFrames.size());

      finalizedMessage_ += "\nStack Trace:\n";
      finalizedMessage_ += printer.str();
    }
  }

  std::vector<uintptr_t> exceptionFrames_;
  const std::string stackTrace_;
  const std::string exceptionName_;
  const char* fileName_;
  const size_t fileLine_;
  const char* functionName_;
  const std::string failingExpression_;
  const std::string errorMessage_;
  const std::string errorCode_;
  const bool retryable_;

  mutable folly::once_flag once_;
  mutable std::string finalizedMessage_;
};

// Exception representing an error originating by a user misusing Alpha.
class AlphaUserError : public AlphaException {
 public:
  AlphaUserError(
      const char* fileName,
      size_t fileLine,
      const char* functionName,
      std::string_view failingExpression,
      std::string_view errorMessage,
      std::string_view errorCode,
      bool retryable)
      : AlphaException(
            "AlphaUserError",
            fileName,
            fileLine,
            functionName,
            failingExpression,
            errorMessage,
            errorCode,
            retryable) {}

  const std::string_view errorSource() const override {
    return error_source::User;
  }
};

// Excpetion representing unexpected behavior in Alpha. This usually means a bug
// in Alpha.
class AlphaInternalError : public AlphaException {
 public:
  AlphaInternalError(
      const char* fileName,
      size_t fileLine,
      const char* functionName,
      std::string_view failingExpression,
      std::string_view errorMessage,
      std::string_view errorCode,
      bool retryable)
      : AlphaException(
            "AlphaInternalError",
            fileName,
            fileLine,
            functionName,
            failingExpression,
            errorMessage,
            errorCode,
            retryable) {}

  const std::string_view errorSource() const override {
    return error_source::Internal;
  }
};

// Exception representing an issue originating from an Alpha external dependency
// (for example, Warm Storage or file system). These exceptions should not
// affect Alpha's SLA.
class AlphaExternalError : public AlphaException {
 public:
  AlphaExternalError(
      const char* fileName,
      const size_t fileLine,
      const char* functionName,
      std::string_view failingExpression,
      std::string_view errorMessage,
      std::string_view errorCode,
      bool retryable,
      std::string_view externalSource)
      : AlphaException(
            "AlphaExternalError",
            fileName,
            fileLine,
            functionName,
            failingExpression,
            errorMessage,
            errorCode,
            retryable),
        externalSource_{externalSource} {}

  const std::string_view errorSource() const override {
    return error_source::External;
  }

  void appendMessage(std::string& message) const override {
    message += "External Source: ";
    message += externalSource_;
    message += "\n";
  }

 private:
  const std::string externalSource_;
};

#define _ALPHA_RAISE_EXCEPTION(                      \
    exception, expression, message, code, retryable) \
  throw exception(                                   \
      __FILE__, __LINE__, __FUNCTION__, expression, message, code, retryable)

#define _ALPHA_RAISE_EXCEPTION_EXTENDED(                  \
    exception, expression, message, code, retryable, ...) \
  throw exception(                                        \
      __FILE__,                                           \
      __LINE__,                                           \
      __FUNCTION__,                                       \
      expression,                                         \
      message,                                            \
      code,                                               \
      retryable,                                          \
      __VA_ARGS__)

#define ALPHA_RAISE_USER_ERROR(expression, message, code, retryable) \
  _ALPHA_RAISE_EXCEPTION(                                            \
      ::facebook::alpha::AlphaUserError, expression, message, code, retryable)

#define ALPHA_RAISE_INTERNAL_ERROR(expression, message, code, retryable) \
  _ALPHA_RAISE_EXCEPTION(                                                \
      ::facebook::alpha::AlphaInternalError,                             \
      expression,                                                        \
      message,                                                           \
      code,                                                              \
      retryable)

#define ALPHA_RAISE_EXTERNAL_ERROR(               \
    expression, source, message, code, retryable) \
  _ALPHA_RAISE_EXCEPTION_EXTENDED(                \
      ::facebook::alpha::AlphaExternalError,      \
      expression,                                 \
      message,                                    \
      code,                                       \
      retryable,                                  \
      source)

// Check user related conditions. Failure of this condition means the user
// misused Alpha and will trigger a user error.
#define ALPHA_CHECK(condition, message)                 \
  if (UNLIKELY(!(condition))) {                         \
    ALPHA_RAISE_USER_ERROR(                             \
        #condition,                                     \
        message,                                        \
        ::facebook::alpha::error_code::InvalidArgument, \
        /* retryable */ false);                         \
  }

// Assert an internal Alpha expected behavior. Failure of this condition means
// Alpha encountered an unexpected behavior and will trigger an internal error.
#define ALPHA_ASSERT(condition, message)             \
  if (UNLIKELY(!(condition))) {                      \
    ALPHA_RAISE_INTERNAL_ERROR(                      \
        #condition,                                  \
        message,                                     \
        ::facebook::alpha::error_code::InvalidState, \
        /* retryable */ false);                      \
  }

// Verify a result from an external Alpha dependency. Failure of this condition
// means an external dependency returned an error and all retries (if
// applicable) were exhausted. This will trigger an external error.
#define ALPHA_VERIFY_EXTERNAL(condition, source, code, retryable, message) \
  if (UNLIKELY(!(condition))) {                                            \
    ALPHA_RAISE_EXTERNAL_ERROR(                                            \
        #condition,                                                        \
        ::facebook::alpha::external_source::source,                        \
        message,                                                           \
        code,                                                              \
        retryable);                                                        \
  }

// Verify an expected file format conditions. Failure of this condition means
// the file is corrupted (e.g. passed magic number and version verification, but
// got unexpected format). This will trigger a user error.
#define ALPHA_CHECK_FILE(condition, message)          \
  if (UNLIKELY(!(condition))) {                       \
    ALPHA_RAISE_USER_ERROR(                           \
        #condition,                                   \
        message,                                      \
        ::facebook::alpha::error_code::CorruptedFile, \
        /* retryable */ false);                       \
  }

// Should be raised when we don't expect to hit a code path, but we did. This
// means a bug in Alpha.
#define ALPHA_UNREACHABLE(message)                    \
  ALPHA_RAISE_INTERNAL_ERROR(                         \
      "",                                             \
      message,                                        \
      ::facebook::alpha::error_code::UnreachableCode, \
      /* retryable */ false);

// Should be raised in places where we still didn't implement the required
// functionality, but intend to do so in the future. This raises an internal
// error to indicate users needs this functionality, but we don't provide it.
#define ALPHA_NOT_IMPLEMENTED(message)               \
  ALPHA_RAISE_INTERNAL_ERROR(                        \
      "",                                            \
      message,                                       \
      ::facebook::alpha::error_code::NotImplemented, \
      /* retryable */ false);

// Should be raised in places where we don't support a functionality, and have
// no intention to support it in the future. This raises a user error, as the
// user should not expect this functionality to exist in the first place.
#define ALPHA_NOT_SUPPORTED(message)               \
  ALPHA_RAISE_USER_ERROR(                          \
      "",                                          \
      message,                                     \
      ::facebook::alpha::error_code::NotSupported, \
      /* retryable */ false);

// Incompatible Encoding errors are used in Alpha's encoding optimization, to
// indicate that an attempted encoding is incompatible with the data and should
// be avoided.
#define ALPHA_INCOMPATIBLE_ENCODING(message)               \
  ALPHA_RAISE_USER_ERROR(                                  \
      "",                                                  \
      message,                                             \
      ::facebook::alpha::error_code::IncompatibleEncoding, \
      /* retryable */ false);

// Should be used in "catch all" exception handlers, where we can't classify the
// error correctly. These errors mean that we are missing error classification.
#define ALPHA_UNKNOWN(message)                \
  ALPHA_RAISE_INTERNAL_ERROR(                 \
      "",                                     \
      message,                                \
      ::facebook::alpha::error_code::Unknown, \
      /* retryable */ true);

// Should be used in "catch all" exception handlers wrapping external Alpha
// dependencies, where we can't classify the error correctly. These errors mean
// that we are missing error classification.
#define ALPHA_UNKNOWN_EXTERNAL(source, message)   \
  ALPHA_RAISE_EXTERNAL_ERROR(                     \
      "",                                         \
      ::facebook::alpha::external_source::source, \
      message,                                    \
      ::facebook::alpha::error_code::Unknown,     \
      /* retryable */ true);

#ifndef NDEBUG
#define ALPHA_DCHECK(condition, message) ALPHA_CHECK(condition, message)
#define ALPHA_DASSERT(condition, message) ALPHA_ASSERT(condition, message)
#else
#define ALPHA_DCHECK(condition, message) ALPHA_CHECK(true, message)
#define ALPHA_DASSERT(condition, message) ALPHA_ASSERT(true, message)
#endif

} // namespace facebook::alpha
