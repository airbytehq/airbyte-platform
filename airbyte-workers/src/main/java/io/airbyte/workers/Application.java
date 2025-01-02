/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers;

import io.micronaut.runtime.Micronaut;

/**
 * Worker micronaut application.
 */
public class Application {

  public static void main(final String[] args) {
    Micronaut.run(Application.class, args);
  }

}
