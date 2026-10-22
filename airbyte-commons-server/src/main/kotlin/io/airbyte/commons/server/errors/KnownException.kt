/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.errors

import io.airbyte.api.server.generated.models.KnownExceptionInfo
import java.io.IOException
import java.io.InterruptedIOException
import java.io.LineNumberReader
import java.io.PrintWriter
import java.io.StringReader
import java.io.StringWriter
import java.util.UUID

/**
 * Exception wrapper to handle formatting API exception outputs nicely.
 */
abstract class KnownException : RuntimeException {
  val details: Map<String, Any>? // Add an optional details field
  val errorId: UUID = UUID.randomUUID() // Unique ID for this error instance

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
      return stringWriter
        .toString()
        .split("\n")
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
    ): KnownExceptionInfo =
      KnownExceptionInfo(
        message = message,
        details = details ?: if (t is KnownException) t.details else null,
        errorId = if (t is KnownException) t.errorId else null,
      )

    fun infoFromThrowableWithMessageAndStackTrace(t: Throwable): KnownExceptionInfo {
      val cause = t.cause
      return KnownExceptionInfo(
        message = t.message,
        details = if (t is KnownException) t.details else null,
        errorId = if (t is KnownException) t.errorId else null,
        exceptionStack = toStringList(t),
        rootCauseExceptionClassName = cause?.javaClass?.name,
        rootCauseExceptionStack = cause?.let { toStringList(it) },
      )
    }

    fun infoFromThrowableWithMessageAndStackTrace(
      t: Throwable,
      message: String?,
    ): KnownExceptionInfo {
      val cause = t.cause
      return KnownExceptionInfo(
        message = message,
        details = if (t is KnownException) t.details else null,
        errorId = if (t is KnownException) t.errorId else null,
        exceptionStack = toStringList(t),
        rootCauseExceptionClassName = cause?.javaClass?.name,
        rootCauseExceptionStack = cause?.let { toStringList(it) },
      )
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
      } catch (_: RuntimeException) {
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
