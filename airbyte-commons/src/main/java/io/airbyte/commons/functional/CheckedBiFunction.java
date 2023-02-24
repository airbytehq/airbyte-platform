/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.functional;

/**
 * BiFunction that can throw a checked exception.
 *
 * @param <A1> type of first arg that the consumer accepts
 * @param <A2> type of second arg that the consumer accepts
 * @param <RESULT> type returned from function
 * @param <E> type of checked exception
 */
public interface CheckedBiFunction<A1, A2, RESULT, E extends Throwable> {

  /**
   * Apply function.
   *
   * @param first arg
   * @param second arg
   * @return value
   * @throws E checked exception that can be thrown while attempting to apply
   */
  RESULT apply(A1 first, A2 second) throws E;

}
