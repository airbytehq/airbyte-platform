/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.concurrent

import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicLong

/**
 * A builder for creating instances of ThreadFactory with configurable parameters.
 *
 * This class simplifies the creation of threads by allowing customization of thread naming through
 * a formatted name pattern. Users may specify a name format for newly created threads, providing
 * better identification and debugging for threads within multi-threaded applications.
 *
 * Threads created using the built ThreadFactory will have their names formatted based
 * on the specified name pattern if one is provided. If no name format is specified, the default
 * thread naming behavior will apply.
 */
class ThreadFactoryBuilder {
  private var threadNameFormat: String? = null

  fun withThreadNameFormat(nameFormat: String) = apply { threadNameFormat = nameFormat }

  fun build(): ThreadFactory {
    val threadCount = threadNameFormat?.let { AtomicLong(0) }
    return ThreadFactory { runnable ->
      val thread = Thread(runnable)
      threadNameFormat?.let { thread.name = String.format(it, threadCount?.getAndIncrement()) }
      thread
    }
  }
}
