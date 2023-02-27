/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_atelier;

import io.micronaut.runtime.Micronaut;

/**
 * Micronaut server responsible of running scheduled method. The methods need to be separated in
 * Bean based on what they are cleaning and contain a method annotated with `@Scheduled`
 *
 * Injected object looks unused but they are not
 */
public class MicronautConnectorAtelierServerRunner {

  public static void main(final String[] args) {
    Micronaut.build(args)
        .mainClass(MicronautConnectorAtelierServerRunner.class)
        .start();
  }

}
