/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.commandrunner;

import io.airbyte.protocol.models.v0.AirbyteRecordMessage;
import java.io.IOException;

/**
 * Exposes a way of running synchronous processes via an Airbyte `read` command.
 */
public interface SynchronousCdkCommandRunner {

  /**
   * Launch a CDK process responsible for handling requests.
   */
  AirbyteRecordMessage runCommand(final String command, final String config, final String catalog, final String state) throws IOException;

}
