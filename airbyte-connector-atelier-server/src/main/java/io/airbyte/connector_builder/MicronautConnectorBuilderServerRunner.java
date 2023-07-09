/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder;

import io.micronaut.runtime.Micronaut;

/**
 * Micronaut server responsible for running the Connector Builder Server which is used to service
 * requests to build and test low-code connector manifests.
 *
 * Injected object looks unused but they are not
 */
public class MicronautConnectorBuilderServerRunner {

  public static void main(final String[] args) {
    Micronaut.build(args)
        .mainClass(MicronautConnectorBuilderServerRunner.class)
        .start();
  }

}
