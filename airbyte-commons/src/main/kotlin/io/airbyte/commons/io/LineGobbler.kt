/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.io

import io.airbyte.commons.concurrency.VoidCallable
import io.airbyte.commons.io.IOs.newBufferedReader
import io.airbyte.commons.logging.MdcScope
import io.github.oshai.kotlinlogging.KotlinLogging
import org.slf4j.MDC
import java.io.ByteArrayInputStream
import java.io.IOException
import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.function.Consumer

/**
 * Abstraction to consume an [InputStream] to completion.
 */
class LineGobbler
  @JvmOverloads
  internal constructor(
    inputStream: InputStream,
    private val consumer: Consumer<String>,
    private val executor: ExecutorService,
    private val mdc: Map<String, String>?,
    private val caller: String = GENERIC,
    private val containerLogMdcBuilder: MdcScope.Builder = MdcScope.DEFAULT_BUILDER,
  ) : VoidCallable {
    private val inputStream = newBufferedReader(inputStream)

    override fun voidCall() {
      MDC.setContextMap(mdc)
      try {
        var line = inputStream.readLine()
        while (line != null) {
          containerLogMdcBuilder.build().use { mdcScope ->
            consumer.accept(line!!)
          }
          line = inputStream.readLine()
        }
      } catch (i: IOException) {
        log.warn { "$caller gobbler IOException: ${i.message}. Typically happens when cancelling a job." }
      } catch (e: Exception) {
        log.error(e) { "$caller gobbler error when reading stream" }
      } finally {
        executor.shutdown()
      }
    }

    companion object {
      private val log = KotlinLogging.logger {}
      private const val GENERIC = "generic"

      /**
       * Connect a message to be consumed by consumer.
       *
       * @param message message to be consumed
       * @param consumer consumer
       */
      fun gobble(
        message: String,
        consumer: Consumer<String>,
      ) {
        val stringAsSteam: InputStream = ByteArrayInputStream(message.toByteArray(StandardCharsets.UTF_8))
        gobble(inputStream = stringAsSteam, consumer = consumer)
      }

      /**
       * Connect an input stream to be consumed by consumer with an [MdcScope], caller label, and
       * executor.
       *
       * Passing the executor lets you wait to ensure that all lines have been gobbled, since it happens
       * asynchronously.
       *
       * @param `is` input stream
       * @param consumer consumer
       * @param caller name of caller
       * @param mdcScopeBuilder mdc scope to be used during consumption
       * @param executor executor to run gobbling
       */
      @JvmStatic
      fun gobble(
        inputStream: InputStream?,
        consumer: Consumer<String>,
        caller: String = GENERIC,
        mdcScopeBuilder: MdcScope.Builder = MdcScope.DEFAULT_BUILDER,
        executor: ExecutorService = Executors.newSingleThreadExecutor(),
      ) {
        if (inputStream != null) {
          val mdc: MutableMap<String, String>? = MDC.getCopyOfContextMap()
          val gobbler = LineGobbler(inputStream, consumer, executor, mdc, caller, mdcScopeBuilder)
          executor.submit(gobbler)
        } else {
          log.warn { "Unable to gobble line(s) from input stream provided by $caller:  input stream is null." }
        }
      }

      /**
       * Connect a message to be consumed by LOGGER.info.
       *
       * @param message message to be consumed
       */
      private fun gobble(message: String) {
        gobble(message) { msg: String? -> log.info { msg } }
      }

      /**
       * Used to emit a visual separator in the user-facing logs indicating a start of a meaningful
       * temporal activity.
       *
       * @param message message to emphasize
       */
      @Deprecated("use info logging with correct mdc context instead")
      fun startSection(message: String) {
        gobble(formatStartSection(message))
      }

      fun formatStartSection(message: String): String = "\r\n----- START $message -----\r\n\r\n"

      /**
       * Used to emit a visual separator in the user-facing logs indicating a end of a meaningful
       * temporal. activity
       *
       * @param message message to emphasize
       */
      @Deprecated("use info logging with correct mdc context instead")
      fun endSection(message: String) {
        gobble(formatEndSection(message))
      }

      fun formatEndSection(message: String): String = "\r\n----- END $message -----\r\n\r\n"
    }
  }
