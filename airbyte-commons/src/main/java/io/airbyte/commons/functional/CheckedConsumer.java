/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.functional;

/**
 * Consumer that throws a checked exception.
 *
 * @param <T> type it consumes
 * @param <E> exception it throws
 */
@FunctionalInterface
public interface CheckedConsumer<T, E extends Throwable> {

  /**
   * Accept a value into the consumer.
   *
   * @param t value to accept
   * @throws E checked exception can be thrown on acceptance
   */
  void accept(T t) throws E;

}
