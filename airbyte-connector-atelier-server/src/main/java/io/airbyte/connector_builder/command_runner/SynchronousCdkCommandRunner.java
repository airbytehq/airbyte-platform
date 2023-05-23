/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.command_runner;

import io.airbyte.protocol.models.AirbyteRecordMessage;
import java.io.IOException;

/**
 * Exposes a way of running synchronous processes via an Airbyte `read` command.
 */
public interface SynchronousCdkCommandRunner {

  /**
   * Launch a CDK process responsible for handling requests.
   */
  AirbyteRecordMessage runCommand(final String command, final String config, final String catalog) throws IOException;

}
