/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.requester;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.airbyte.connector_builder.api.model.generated.ResolveManifest;
import io.airbyte.connector_builder.command_runner.SynchronousCdkCommandRunner;
import io.airbyte.connector_builder.exceptions.AirbyteCdkInvalidInputException;
import io.airbyte.connector_builder.exceptions.CdkProcessException;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Construct and send requests to the CDK's Connector Builder handler.
 */
@Singleton
public class AirbyteCdkRequesterImpl implements AirbyteCdkRequester {

  private static final String commandKey = "__command";
  private static final String manifestKey = "__injected_declarative_manifest";
  private static final String resolveManifestCommand = "resolve_manifest";
  private static final Logger LOGGER = LoggerFactory.getLogger(AirbyteCdkRequesterImpl.class);

  private final SynchronousCdkCommandRunner commandRunner;

  public AirbyteCdkRequesterImpl(final SynchronousCdkCommandRunner commandRunner) {
    this.commandRunner = commandRunner;
  }

  /**
   * Launch a CDK process responsible for handling resolve_manifest requests.
   */
  @Override
  public ResolveManifest resolveManifest(final JsonNode manifest)
      throws IOException, AirbyteCdkInvalidInputException, CdkProcessException {
    final AirbyteRecordMessage record = request(manifest, resolveManifestCommand);
    return new ResolveManifest().manifest(record.getData().get("manifest"));
  }

  /**
   * Launch a CDK process responsible for handling requests.
   */
  private AirbyteRecordMessage request(
                                       final JsonNode manifest,
                                       final String cdkCommand)
      throws IOException, AirbyteCdkInvalidInputException, CdkProcessException {
    LOGGER.debug("Creating CDK process: {}.", cdkCommand);
    return this.commandRunner.runCommand(
        cdkCommand, this.getConfig(manifest, cdkCommand), this.getCatalog());
  }

  String getCatalog() {
    return "";
  }

  String getConfig(final JsonNode manifest, final String command) throws IOException {
    final Map<String, Object> configMap = new HashMap<>();
    configMap.put(manifestKey, manifest);
    configMap.put(commandKey, command);
    final ObjectWriter writer = new ObjectMapper().writer().withDefaultPrettyPrinter();
    return writer.writeValueAsString(configMap);
  }

}
