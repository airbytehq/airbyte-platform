/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors

import io.airbyte.api.model.generated.KnownExceptionInfo
import java.io.IOException
import java.io.InterruptedIOException
import java.io.LineNumberReader
import java.io.PrintWriter
import java.io.StringReader
import java.io.StringWriter
import java.util.stream.Collectors
import java.util.stream.Stream

/**
 * Exception wrapper to handle formatting API exception outputs nicely.
 */
abstract class KnownException : RuntimeException {
  val details: Map<String, Any>? // Add an optional details field

  constructor(message: String?) : super(message) {
    this.details = null
  }

  constructor(message: String?, details: Map<String, Any>?) : super(message) {
    this.details = details
  }

  constructor(message: String?, cause: Throwable?) : super(message, cause) {
    this.details = null
  }

  constructor(message: String?, cause: Throwable?, details: Map<String, Any>?) : super(message, cause) {
    this.details = details
  }

  abstract fun getHttpCode(): Int

  fun getKnownExceptionInfo(): KnownExceptionInfo = infoFromThrowable(this, details)

  fun getKnownExceptionInfoWithStackTrace(): KnownExceptionInfo = infoFromThrowableWithMessageAndStackTrace(this)

  companion object {
    @JvmStatic
    fun getStackTraceAsList(throwable: Throwable): List<String> {
      val stringWriter = StringWriter()
      throwable.printStackTrace(PrintWriter(stringWriter))
      val stackTrace =
        stringWriter
          .toString()
          .split("\n".toRegex())
          .dropLastWhile { it.isEmpty() }
          .toTypedArray()
      return Stream.of(*stackTrace).collect(Collectors.toList())
    }

    /**
     * Static factory for creating a known exception.
     *
     * @param t throwable to wrap
     * @param message error message
     * @param details additional details
     * @return known exception
     */
    @JvmOverloads
    fun infoFromThrowableWithMessage(
      t: Throwable,
      message: String?,
      details: Map<String, Any>? = null,
    ): KnownExceptionInfo {
      val exceptionInfo =
        KnownExceptionInfo()
          .exceptionClassName(t.javaClass.name)
          .message(message)

      if (details != null) {
        exceptionInfo.details(details)
      }

      return exceptionInfo
    }

    private fun infoFromThrowableWithMessageAndStackTrace(t: Throwable): KnownExceptionInfo {
      val exceptionInfo =
        KnownExceptionInfo()
          .exceptionClassName(t.javaClass.name)
          .message(t.message)
          .exceptionStack(toStringList(t))
      if (t.cause != null) {
        exceptionInfo.rootCauseExceptionClassName(t.javaClass.name)
        exceptionInfo.rootCauseExceptionStack(toStringList(t.cause!!))
      }
      return exceptionInfo
    }

    fun infoFromThrowable(
      t: Throwable,
      details: Map<String, Any>?,
    ): KnownExceptionInfo = infoFromThrowableWithMessage(t, t.message, details)

    private fun toStringList(throwable: Throwable): List<String> {
      val sw = StringWriter()
      val pw = PrintWriter(sw)
      try {
        throwable.printStackTrace(pw)
      } catch (ex: RuntimeException) {
        // Ignore any exceptions.
      }
      pw.flush()
      val lines: MutableList<String> = ArrayList()
      try {
        LineNumberReader(StringReader(sw.toString())).use { reader ->
          var line = reader.readLine()
          while (line != null) {
            lines.add(line)
            line = reader.readLine()
          }
        }
      } catch (ex: IOException) {
        if (ex is InterruptedIOException) {
          Thread.currentThread().interrupt()
        }
        lines.add(ex.toString())
      }
      return lines
    }
  }
}
