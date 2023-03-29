/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.command_runner;

import java.io.IOException;

/**
 * Abstract class for wrapping a process launched for CDK requests.
 */
public interface AirbyteCdkProcess extends AutoCloseable {

  Process getProcess();

  Process start() throws IOException;

  @Override
  void close();

}
