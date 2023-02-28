/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.functional;

/**
 * BiConsumer that can throw a checked exception.
 *
 * @param <T> type of first arg that the consumer accepts
 * @param <R> type of second arg that the consumer accepts
 * @param <E> type of checked exception
 */
@FunctionalInterface
public interface CheckedBiConsumer<T, R, E extends Throwable> {

  /**
   * Accept values into the consumer.
   *
   * @param t first value
   * @param r second value
   * @throws E checked exception can be thrown on acceptance
   */
  void accept(T t, R r) throws E;

}
