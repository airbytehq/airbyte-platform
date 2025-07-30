/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.concurrency

import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock

class ClosableLinkedBlockingQueue<T>(
  maxQueueSize: Int,
  pollTimeOutDurationInSeconds: Int,
) : ClosableQueue<T> {
  private val queue: BlockingQueue<T>

  private val closed: AtomicBoolean
  private val closedLock: ReadWriteLock
  private val timeOutDuration: Int

  init {
    log.info { "Using ClosableLinkedBlockingQueue" }
    this.queue = LinkedBlockingQueue<T>(maxQueueSize)
    this.timeOutDuration = pollTimeOutDurationInSeconds
    this.closed = AtomicBoolean()
    this.closedLock = ReentrantReadWriteLock()
  }

  @Throws(InterruptedException::class)
  override fun poll(): T? = queue.poll(timeOutDuration.toLong(), TimeUnit.SECONDS)

  @Throws(InterruptedException::class)
  override fun add(e: T): Boolean {
    try {
      // We use a ReadWriteLock to make sure we are not adding to the queue while attempting to close
      // it. This prevents race conditions where we may be finishing an insert while the queue is being
      // closed.
      closedLock.readLock().lock()

      if (closed.get()) {
        return false
      }

      return queue.offer(e, timeOutDuration.toLong(), TimeUnit.SECONDS)
    } finally {
      closedLock.readLock().unlock()
    }
  }

  override fun size(): Int = queue.size

  override fun isDone(): Boolean {
    try {
      closedLock.readLock().lock()
      return size() == 0 && isClosed()
    } finally {
      closedLock.readLock().unlock()
    }
  }

  override fun close() {
    try {
      closedLock.writeLock().lock()
      closed.set(true)
    } finally {
      closedLock.writeLock().unlock()
    }
  }

  override fun isClosed(): Boolean {
    try {
      // Acquiring this lock for safety. closed being an atomic boolean, we may not need this.
      closedLock.readLock().lock()
      return closed.get()
    } finally {
      closedLock.readLock().unlock()
    }
  }

  companion object {
    private val log = KotlinLogging.logger {}
    const val DEFAULT_POLL_TIME_OUT_DURATION_SECONDS: Int = 5
  }
}
