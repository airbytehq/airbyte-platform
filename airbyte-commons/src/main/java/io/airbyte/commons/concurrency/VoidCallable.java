/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.concurrency;

import java.util.concurrent.Callable;

/**
 * Callable that has no return. Useful for coercing the same behavior for lambdas that have a return
 * value and those that don't.
 */
@FunctionalInterface
public interface VoidCallable extends Callable<Void> {

  default @Override Void call() throws Exception {
    voidCall();
    return null;
  }

  void voidCall() throws Exception;

}
