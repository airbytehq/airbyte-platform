/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.functional;

/**
 * Supplier that can throw a checked exception.
 *
 * @param <T> type it supplies
 * @param <E> type of checked exception
 */
@FunctionalInterface
public interface CheckedSupplier<T, E extends Throwable> {

  /**
   * Supply value.
   *
   * @return supplied value
   * @throws E checked exception that can be thrown while attempting to supply
   */
  T get() throws E;

}
