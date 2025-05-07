/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.fixtures

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.workers.testutils.AirbyteMessageUtils
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.io.PipedInputStream
import java.io.PipedOutputStream
import java.nio.charset.Charset
import java.time.Duration
import java.time.Instant
import java.util.Optional
import java.util.concurrent.Executors

/**
 * Basic Airbyte Source that emits [LimitedFatRecordSourceProcess.TOTAL_RECORDS] before
 * finishing. Intended for performance testing.
 */
class LimitedFatRecordSourceProcess : Process() {
  private var currRecs = 0
  private val `is` = PipedInputStream()

  override fun getOutputStream(): OutputStream? = null

  override fun getInputStream(): InputStream {
    val os: OutputStream
    // start writing to the input stream
    try {
      os = PipedOutputStream(`is`)
    } catch (e: IOException) {
      throw RuntimeException(e)
    }

    Executors.newSingleThreadExecutor().submit(
      Runnable {
        try {
          while (currRecs != TOTAL_RECORDS) {
            val msg =
              AirbyteMessageUtils.createRecordMessage(
                "s1",
                "data",
                "This is a fairly long sentence to provide some bytes here. More bytes is better as it helps us measure performance." +
                  "Random append to prevent dead code generation :",
              )
            os.write(MAPPER.writeValueAsString(msg).toByteArray(Charset.defaultCharset()))
            os.write(System.lineSeparator().toByteArray(Charset.defaultCharset()))
            currRecs++
          }
          os.flush()
          os.close()
        } catch (e: IOException) {
          throw RuntimeException(e)
        }
      },
    )

    return `is`
  }

  override fun getErrorStream(): InputStream = PipedInputStream()

  @Throws(InterruptedException::class)
  override fun waitFor(): Int {
    while (exitValue() != 0) {
      Thread.sleep((1000 * 10).toLong())
    }
    return exitValue()
  }

  override fun exitValue(): Int {
    if (currRecs == TOTAL_RECORDS) {
      try {
        `is`.close()
      } catch (e: IOException) {
        throw RuntimeException(e)
      }
      return 0
    }

    throw IllegalThreadStateException("process hasn't exited")
  }

  override fun destroy() {
    currRecs = TOTAL_RECORDS

    try {
      `is`.close()
    } catch (e: IOException) {
      throw RuntimeException(e)
    }
  }

  override fun info(): ProcessHandle.Info = TestProcessInfo()

  internal class TestProcessInfo : ProcessHandle.Info {
    override fun command(): Optional<String> = Optional.of<String>("test process")

    override fun commandLine(): Optional<String> = Optional.of<String>("test process")

    override fun arguments(): Optional<Array<String>> = Optional.empty<Array<String>>()

    override fun startInstant(): Optional<Instant> = Optional.empty<Instant>()

    override fun totalCpuDuration(): Optional<Duration> = Optional.empty<Duration>()

    override fun user(): Optional<String> = Optional.empty<String>()
  }

  companion object {
    private const val TOTAL_RECORDS = 2000000
    private val MAPPER = ObjectMapper()

    @JvmStatic
    fun main(args: Array<String>) {
    }
  }
}
