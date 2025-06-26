/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder;

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
        .deduceCloudEnvironment(false)
        .deduceEnvironment(false)
        .mainClass(MicronautConnectorBuilderServerRunner.class)
        .start();
  }

}
