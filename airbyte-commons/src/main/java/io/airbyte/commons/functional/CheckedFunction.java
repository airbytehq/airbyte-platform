/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.functional;

/**
 * {@link java.util.function.Function} that can throw a checked exception.
 *
 * @param <T> type of arg that the function accepts
 * @param <R> type returned from function
 * @param <E> type of checked exception
 */
@FunctionalInterface
public interface CheckedFunction<T, R, E extends Throwable> {

  /**
   * Apply function.
   *
   * @param t arg
   * @return return value
   * @throws E checked exception that can be thrown while attempting to apply
   */
  R apply(T t) throws E;

}
