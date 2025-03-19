/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.lang;

/**
 * Shared code for handling booleans.
 */
public class MoreBooleans {

  /**
   * Safely handles converting boxed Booleans to booleans, even when they are null. Evaluates null as
   * false.
   *
   * @param bool boxed
   * @return unboxed
   */
  public static boolean isTruthy(final Boolean bool) {
    return bool != null && bool;
  }

}
