/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.concurrency;

public interface ClosableQueue<T> {

  T poll() throws InterruptedException;

  boolean add(final T e) throws InterruptedException;

  int size();

  boolean isDone();

  void close();

  boolean isClosed();

}
