/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.concurrency;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A Wrapper around a ConcurrentLinkedQueue that adds a constant time size lookup and the ability to
 * close the queue.
 * <p>
 * We are using a ConcurrentLinkedQueue over a BlockingQueue due to the performance impact. The
 * default implementation of a BlockingQueue has a single lock while the ConcurrentLinkedQueue has
 * two locks, one each end of the queue hence reducing the contention.
 */
public class BoundedConcurrentLinkedQueue<T> {

  private final Queue<T> queue;
  private final AtomicInteger size;
  private final AtomicBoolean closed;
  private final ReadWriteLock closedLock;
  private final int maxSize;

  public BoundedConcurrentLinkedQueue(final int maxSize) {
    this.queue = new ConcurrentLinkedQueue<>();
    this.size = new AtomicInteger();
    this.closed = new AtomicBoolean();
    this.closedLock = new ReentrantReadWriteLock();
    this.maxSize = maxSize;
  }

  /**
   * Retrieves and removes the head of this queue, or returns null if this queue is empty.
   *
   * @return the head of this queue, or null if this queue is empty
   */
  public T poll() {
    final T e = queue.poll();
    if (e != null) {
      size.decrementAndGet();
    }
    return e;
  }

  /**
   * Inserts the specified element into this queue if it is possible to do so immediately without
   * violating capacity restrictions, returning true upon success. Throws: IllegalStateException – if
   * the element cannot be added at this time due to capacity restrictions ClassCastException – if the
   * class of the specified element prevents it from being added to this queue NullPointerException –
   * if the specified element is null and this queue does not permit null elements
   * IllegalArgumentException – if some property of this element prevents it from being added to this
   * queue
   *
   * @param e the element to add
   * @return true if the insertion was successful
   */
  public boolean add(final T e) {
    try {
      // We use a ReadWriteLock to make sure we are not adding to the queue while attempting to close
      // it. This prevents race conditions where we may be finishing an insert while the queue is being
      // closed.
      closedLock.readLock().lock();

      if (closed.get() || size.get() >= this.maxSize) {
        return false;
      }

      final boolean insertResult = queue.add(e);
      if (insertResult) {
        size.incrementAndGet();
      }
      return insertResult;
    } finally {
      closedLock.readLock().unlock();
    }
  }

  public int size() {
    return size.get();
  }

  /**
   * Returns true if the queue is done. A queue is done when closed and empty.
   */
  public boolean isDone() {
    try {
      closedLock.readLock().lock();
      return size() == 0 && isClosed();
    } finally {
      closedLock.readLock().unlock();
    }
  }

  /**
   * Close the queue.
   */
  public void close() {
    try {
      closedLock.writeLock().lock();
      closed.set(true);
    } finally {
      closedLock.writeLock().unlock();
    }
  }

  /**
   * Returns true if the queue is closed.
   */
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
