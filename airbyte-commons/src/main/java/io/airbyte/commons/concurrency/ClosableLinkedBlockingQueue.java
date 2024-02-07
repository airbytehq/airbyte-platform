/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.concurrency;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClosableLinkedBlockingQueue<T> implements ClosableQueue<T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClosableLinkedBlockingQueue.class);
  private final LinkedBlockingQueue<T> queue;

  private final AtomicBoolean closed;
  private final ReadWriteLock closedLock;

  public ClosableLinkedBlockingQueue(int maxQueueSize) {
    LOGGER.info("Using ClosableLinkedBlockingQueue");
    this.queue = new LinkedBlockingQueue<>(maxQueueSize);
    this.closed = new AtomicBoolean();
    this.closedLock = new ReentrantReadWriteLock();
  }

  @Override
  public T poll() throws InterruptedException {
    return queue.poll(5, TimeUnit.SECONDS);
  }

  @Override
  public boolean add(final T e) throws InterruptedException {
    try {
      // We use a ReadWriteLock to make sure we are not adding to the queue while attempting to close
      // it. This prevents race conditions where we may be finishing an insert while the queue is being
      // closed.
      closedLock.readLock().lock();

      if (closed.get()) {
        return false;
      }

      queue.put(e);
      return true;
    } finally {
      closedLock.readLock().unlock();
    }
  }

  @Override
  public int size() {
    return queue.size();
  }

  @Override
  public boolean isDone() {
    try {
      closedLock.readLock().lock();
      return size() == 0 && isClosed();
    } finally {
      closedLock.readLock().unlock();
    }
  }

  @Override
  public void close() {
    try {
      closedLock.writeLock().lock();
      closed.set(true);
    } finally {
      closedLock.writeLock().unlock();
    }
  }

  @Override
  public boolean isClosed() {
    try {
      // Acquiring this lock for safety. closed being an atomic boolean, we may not need this.
      closedLock.readLock().lock();
      return closed.get();
    } finally {
      closedLock.readLock().unlock();
    }
  }

}
