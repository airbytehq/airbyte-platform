/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.errorreporter

import io.airbyte.commons.lang.Exceptions
import io.github.oshai.kotlinlogging.KotlinLogging
import io.sentry.protocol.SentryException
import io.sentry.protocol.SentryStackFrame
import io.sentry.protocol.SentryStackTrace
import java.util.Arrays
import java.util.Collections
import java.util.EnumMap
import java.util.Optional
import java.util.regex.Pattern

/**
 * Prepare errors to be sent to Sentry.
 */
class SentryExceptionHelper {
  /**
   * Exceptions parsed to send to sentry.
   *
   * @param platform platform
   * @param exceptions exceptions to send
   */
  @JvmRecord
  data class SentryParsedException(
    @JvmField val platform: SentryExceptionPlatform,
    @JvmField val exceptions: List<SentryException>,
  )

  /**
   * Keys to known error types.
   */
  enum class ErrorMapKeys {
    ERROR_MAP_MESSAGE_KEY,
    ERROR_MAP_TYPE_KEY,
  }

  /**
   * Specifies the platform for a thrown exception. Values must be supported by Sentry as specified in
   * https://develop.sentry.dev/sdk/event-payloads/#required-attributes. Currently, only java, python
   * and dbt (other) exceptions are supported.
   */
  enum class SentryExceptionPlatform(
    @JvmField val value: String,
  ) {
    JAVA("java"),
    PYTHON("python"),
    OTHER("other"),
    ;

    override fun toString(): String = value.toString()
  }

  /**
   * Processes a raw stacktrace string into structured SentryExceptions
   *
   *
   * Currently, Java and Python stacktraces are supported. If an unsupported stacktrace format is
   * encountered, an empty optional will be returned, in which case we can fall back to alternate
   * grouping.
   */
  fun buildSentryExceptions(stacktrace: String): Optional<SentryParsedException> {
    return Exceptions.swallowWithDefault({
      if (stacktrace.startsWith("Traceback (most recent call last):")) {
        return@swallowWithDefault buildPythonSentryExceptions(stacktrace)
      }
      if (stacktrace.contains("\tat ") && (stacktrace.contains(".java") || stacktrace.contains(".kt"))) {
        return@swallowWithDefault buildJavaSentryExceptions(stacktrace)
      }
      if (stacktrace.startsWith("AirbyteDbtError: ")) {
        return@swallowWithDefault buildNormalizationDbtSentryExceptions(stacktrace)
      }
      Optional.empty<SentryParsedException>()
    }, Optional.empty<SentryParsedException>())
  }

  companion object {
    private val log = KotlinLogging.logger {}

    private fun buildPythonSentryExceptions(stacktrace: String): Optional<SentryParsedException> {
      val sentryExceptions: MutableList<SentryException> = ArrayList()

      // separate chained exceptions
      // e.g "\n\nThe above exception was the direct cause of the following exception:\n\n"
      // "\n\nDuring handling of the above exception, another exception occurred:\n\n"
      val exceptionSeparator = "\n\n[\\w ,]+:\n\n"
      val exceptions = stacktrace.split(exceptionSeparator.toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()

      for (exceptionStr in exceptions) {
        val stackTrace = SentryStackTrace()
        val stackFrames: MutableList<SentryStackFrame> = ArrayList()

        // Use a regex to grab stack trace frame information
        val framePattern = Pattern.compile("File \"(?<absPath>.+)\", line (?<lineno>\\d+), in (?<function>.+)\\n {4}(?<contextLine>.+)\\n")
        val matcher = framePattern.matcher(exceptionStr)
        var lastMatchIdx = -1

        while (matcher.find()) {
          val absPath = matcher.group("absPath")
          val lineno = matcher.group("lineno")
          val function = matcher.group("function")
          val contextLine = matcher.group("contextLine")

          val stackFrame = SentryStackFrame()
          stackFrame.absPath = absPath
          stackFrame.lineno = lineno.toInt()
          stackFrame.function = function
          stackFrame.contextLine = contextLine
          stackFrames.add(stackFrame)

          lastMatchIdx = matcher.end()
        }

        if (!stackFrames.isEmpty()) {
          stackTrace.frames = stackFrames

          val sentryException = SentryException()
          sentryException.stacktrace = stackTrace

          // The final part of our stack trace has the exception type and (optionally) a value
          // (e.g. "RuntimeError: This is the value")
          val remaining = exceptionStr.substring(lastMatchIdx)
          val parts = remaining.split(":".toRegex(), limit = 2).toTypedArray()

          if (parts.size > 0) {
            sentryException.type = parts[0].trim { it <= ' ' }
            if (parts.size == 2) {
              sentryException.value = parts[1].trim { it <= ' ' }
            }

            sentryExceptions.add(sentryException)
          }
        }
      }

      if (sentryExceptions.isEmpty()) {
        return Optional.empty()
      }

      return Optional.of(SentryParsedException(SentryExceptionPlatform.PYTHON, sentryExceptions))
    }

    private fun buildJavaSentryExceptions(stacktrace: String): Optional<SentryParsedException> {
      val sentryExceptions: MutableList<SentryException> = ArrayList()

      // separate chained exceptions
      // e.g "\nCaused by: "
      val exceptionSeparator = "\nCaused by: "
      val exceptions = stacktrace.split(exceptionSeparator.toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()

      for (exceptionStr in exceptions) {
        val stackTrace = SentryStackTrace()
        val stackFrames: MutableList<SentryStackFrame?> = ArrayList()

        val framePattern =
          Pattern.compile(
            "\n\tat (?:[\\w.$/]+/)?(?<module>[\\w$.]+)\\.(?<function>[\\w<>$]+)\\((?:(?<filename>[\\w]+\\.(?<extension>java|kt)):(?<lineno>\\d+)\\)|(?<desc>[\\w\\s]*))",
          )
        val matcher = framePattern.matcher(exceptionStr)

        while (matcher.find()) {
          val module = matcher.group("module")
          val filename = matcher.group("filename")
          val lineno = matcher.group("lineno")
          val function = matcher.group("function")
          val sourceDescription = matcher.group("desc")

          val stackFrame = SentryStackFrame()
          stackFrame.module = module
          stackFrame.function = function
          stackFrame.filename = filename

          if (lineno != null) {
            stackFrame.lineno = lineno.toInt()
          }
          if ("Native Method" == sourceDescription) {
            stackFrame.isNative = true
          }

          stackFrames.add(stackFrame)
        }

        if (!stackFrames.isEmpty()) {
          Collections.reverse(stackFrames)
          stackTrace.frames = stackFrames

          val sentryException = SentryException()
          sentryException.stacktrace = stackTrace

          // The first section of our stacktrace before the first frame has exception type and value
          val sections = exceptionStr.split("\n\tat ".toRegex(), limit = 2).toTypedArray()
          val headerParts = sections[0].split(": ".toRegex(), limit = 2).toTypedArray()

          if (headerParts.size > 0) {
            sentryException.type = headerParts[0].trim { it <= ' ' }
            if (headerParts.size == 2) {
              sentryException.value = headerParts[1].trim { it <= ' ' }
            }

            sentryExceptions.add(sentryException)
          }
        }
      }

      if (sentryExceptions.isEmpty()) {
        return Optional.empty()
      }

      return Optional.of(SentryParsedException(SentryExceptionPlatform.JAVA, sentryExceptions))
    }

    private fun buildNormalizationDbtSentryExceptions(stacktrace: String): Optional<SentryParsedException> {
      val sentryExceptions: MutableList<SentryException> = ArrayList()

      val usefulErrorMap = getUsefulErrorMessageAndTypeFromDbtError(stacktrace)

      // if our errorMessage from the function != stacktrace then we know we've pulled out something
      // useful
      if (usefulErrorMap[ErrorMapKeys.ERROR_MAP_MESSAGE_KEY] != stacktrace) {
        val usefulException = SentryException()
        usefulException.value = usefulErrorMap[ErrorMapKeys.ERROR_MAP_MESSAGE_KEY]
        usefulException.type = usefulErrorMap[ErrorMapKeys.ERROR_MAP_TYPE_KEY]
        sentryExceptions.add(usefulException)
      }

      if (sentryExceptions.isEmpty()) {
        return Optional.empty()
      }

      return Optional.of(SentryParsedException(SentryExceptionPlatform.OTHER, sentryExceptions))
    }

    /**
     * Extract errors from dbt stacktrace.
     *
     * @param stacktrace stacktrace
     * @return map of known errors found in the stacktrace with error details
     */
    fun getUsefulErrorMessageAndTypeFromDbtError(stacktrace: String): Map<ErrorMapKeys, String> {
      // the dbt 'stacktrace' is really just all the log messages at 'error' level, stuck together.
      // therefore there is not a totally consistent structure to these,
      // see the docs: https://docs.getdbt.com/guides/legacy/debugging-errors
      // the logic below is built based on the ~450 unique dbt errors we encountered before this PR
      // and is a best effort to isolate the useful part of the error logs for debugging and grouping
      // and bring some semblance of exception 'types' to differentiate between errors.
      val errorMessageAndType: MutableMap<ErrorMapKeys, String> =
        EnumMap(
          ErrorMapKeys::class.java,
        )
      val stacktraceLines = stacktrace.split("\n".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()

      var defaultNextLine = false
      // TODO: this whole code block is quite ugh, commented to try and make each part clear but could be
      // much more readable.
      mainLoop@ for (i in stacktraceLines.indices) {
        // This order is important due to how these errors can co-occur.
        // This order attempts to keep error definitions consistent based on our observations of possible
        // dbt error structures.
        try {
          // Database Errors
          if (stacktraceLines[i].contains("Database Error in model")) {
            // Database Error : SQL compilation error
            if (stacktraceLines[i + 1].contains("SQL compilation error")) {
              errorMessageAndType[ErrorMapKeys.ERROR_MAP_MESSAGE_KEY] =
                String.format(
                  "%s %s",
                  stacktraceLines[i + 1].trim { it <= ' ' },
                  stacktraceLines[i + 2].trim { it <= ' ' },
                )
              errorMessageAndType[ErrorMapKeys.ERROR_MAP_TYPE_KEY] = "DbtDatabaseSQLCompilationError"
              break
              // Database Error: Invalid input
            } else if (stacktraceLines[i + 1].contains("Invalid input")) {
              for (followingLine in Arrays.copyOfRange<String>(stacktraceLines, i + 1, stacktraceLines.size)) {
                if (followingLine.trim { it <= ' ' }.startsWith("context:")) {
                  errorMessageAndType[ErrorMapKeys.ERROR_MAP_MESSAGE_KEY] =
                    String.format(
                      "%s\n%s",
                      stacktraceLines[i + 1].trim { it <= ' ' },
                      followingLine.trim { it <= ' ' },
                    )
                  errorMessageAndType[ErrorMapKeys.ERROR_MAP_TYPE_KEY] = "DbtDatabaseInvalidInputError"
                  break@mainLoop
                }
              }
              // Database Error: Syntax error
            } else if (stacktraceLines[i + 1].contains("syntax error at or near \"")) {
              errorMessageAndType[ErrorMapKeys.ERROR_MAP_MESSAGE_KEY] =
                String.format(
                  "%s\n%s",
                  stacktraceLines[i + 1].trim { it <= ' ' },
                  stacktraceLines[i + 2].trim { it <= ' ' },
                )
              errorMessageAndType[ErrorMapKeys.ERROR_MAP_TYPE_KEY] = "DbtDatabaseSyntaxError"
              break
              // Database Error: default
            } else {
              errorMessageAndType[ErrorMapKeys.ERROR_MAP_TYPE_KEY] = "DbtDatabaseError"
              defaultNextLine = true
            }
            // Unhandled Error
          } else if (stacktraceLines[i].contains("Unhandled error while executing model")) {
            errorMessageAndType[ErrorMapKeys.ERROR_MAP_TYPE_KEY] = "DbtUnhandledError"
            defaultNextLine = true
            // Compilation Errors
          } else if (stacktraceLines[i].contains("Compilation Error")) {
            // Compilation Error: Ambiguous Relation
            if (stacktraceLines[i + 1].contains("When searching for a relation, dbt found an approximate match.")) {
              errorMessageAndType[ErrorMapKeys.ERROR_MAP_MESSAGE_KEY] =
                String.format(
                  "%s %s",
                  stacktraceLines[i + 1].trim { it <= ' ' },
                  stacktraceLines[i + 2].trim { it <= ' ' },
                )
              errorMessageAndType[ErrorMapKeys.ERROR_MAP_TYPE_KEY] = "DbtCompilationAmbiguousRelationError"
              break
              // Compilation Error: default
            } else {
              errorMessageAndType[ErrorMapKeys.ERROR_MAP_TYPE_KEY] = "DbtCompilationError"
              defaultNextLine = true
            }
            // Runtime Errors
          } else if (stacktraceLines[i].contains("Runtime Error")) {
            // Runtime Error: Database error
            for (followingLine in Arrays.copyOfRange<String>(stacktraceLines, i + 1, stacktraceLines.size)) {
              if ("Database Error" == followingLine.trim { it <= ' ' }) {
                errorMessageAndType[ErrorMapKeys.ERROR_MAP_MESSAGE_KEY] =
                  String.format(
                    "%s",
                    stacktraceLines[Arrays.stream(stacktraceLines).toList().indexOf(followingLine) + 1]
                      .trim { it <= ' ' },
                  )
                errorMessageAndType[ErrorMapKeys.ERROR_MAP_TYPE_KEY] = "DbtRuntimeDatabaseError"
                break@mainLoop
              }
            }
            // Runtime Error: default
            errorMessageAndType[ErrorMapKeys.ERROR_MAP_TYPE_KEY] = "DbtRuntimeError"
            defaultNextLine = true
            // Database Error: formatted differently, catch last to avoid counting other types of errors as
            // Database Error
          } else if ("Database Error" == stacktraceLines[i].trim { it <= ' ' }) {
            errorMessageAndType[ErrorMapKeys.ERROR_MAP_TYPE_KEY] = "DbtDatabaseError"
            defaultNextLine = true
          }
          // handle the default case without repeating code
          if (defaultNextLine) {
            errorMessageAndType[ErrorMapKeys.ERROR_MAP_MESSAGE_KEY] = stacktraceLines[i + 1].trim { it <= ' ' }
            break
          }
        } catch (e: ArrayIndexOutOfBoundsException) {
          // this means our logic is slightly off, our assumption of where error lines are is incorrect
          log.warn { "Failed trying to parse useful error message out of dbt error, defaulting to full stacktrace" }
        }
      }
      if (errorMessageAndType.isEmpty()) {
        // For anything we haven't caught, just return full stacktrace
        errorMessageAndType[ErrorMapKeys.ERROR_MAP_MESSAGE_KEY] = stacktrace
        errorMessageAndType[ErrorMapKeys.ERROR_MAP_TYPE_KEY] = "AirbyteDbtError"
      }
      return errorMessageAndType
    }
  }
}
