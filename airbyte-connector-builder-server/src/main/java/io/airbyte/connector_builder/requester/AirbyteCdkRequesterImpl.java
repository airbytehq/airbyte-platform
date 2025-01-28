/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.requester;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.annotations.VisibleForTesting;
import datadog.trace.api.Trace;
import io.airbyte.commons.json.Jsons;
import io.airbyte.connector_builder.TracingHelper;
import io.airbyte.connector_builder.api.model.generated.ResolveManifest;
import io.airbyte.connector_builder.api.model.generated.StreamRead;
import io.airbyte.connector_builder.api.model.generated.StreamReadAuxiliaryRequestsInner;
import io.airbyte.connector_builder.api.model.generated.StreamReadLogsInner;
import io.airbyte.connector_builder.api.model.generated.StreamReadSlicesInner;
import io.airbyte.connector_builder.command_runner.SynchronousCdkCommandRunner;
import io.airbyte.connector_builder.exceptions.AirbyteCdkInvalidInputException;
import io.airbyte.connector_builder.exceptions.CdkProcessException;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Construct and send requests to the CDK's Connector Builder handler.
 */
@Singleton
public class AirbyteCdkRequesterImpl implements AirbyteCdkRequester {

  static final Integer maxRecordLimit = 5000;
  static final Integer maxSliceLimit = 20;
  static final Integer maxPageLimit = 20;
  private static final String commandKey = "__command";
  private static final String commandConfigKey = "__test_read_config";
  private static final String manifestKey = "__injected_declarative_manifest";
  private static final String recordLimitKey = "max_records";
  private static final String sliceLimitKey = "max_slices";
  private static final String pageLimitKey = "max_pages_per_slice";
  private static final String resolveManifestCommand = "resolve_manifest";
  private static final String readStreamCommand = "test_read";
  private static final String catalogTemplate = """
                                                {
                                                  "streams": [
                                                    {
                                                      "stream": {
                                                        "name": "%s",
                                                        "json_schema": {},
                                                        "supported_sync_modes": ["full_refresh", "incremental"]
                                                      },
                                                      "sync_mode": "incremental",
                                                      "destination_sync_mode": "overwrite"
                                                    }
                                                  ]
                                                }""";
  private static final ObjectWriter OBJECT_WRITER = new ObjectMapper().writer().withDefaultPrettyPrinter();
  private static final ObjectNode CONFIG_NODE = new ObjectMapper().createObjectNode();
  private static final Logger LOGGER = LoggerFactory.getLogger(AirbyteCdkRequesterImpl.class);

  private final SynchronousCdkCommandRunner commandRunner;

  public AirbyteCdkRequesterImpl(final SynchronousCdkCommandRunner commandRunner) {
    this.commandRunner = commandRunner;
  }

  /**
   * Launch a CDK process responsible for handling resolve_manifest requests.
   */
  @Override
  @Trace(operationName = TracingHelper.CONNECTOR_BUILDER_OPERATION_NAME)
  public StreamRead readStream(final JsonNode manifest,
                               final JsonNode config,
                               final List<JsonNode> state,
                               final String stream,
                               final Integer recordLimit,
                               final Integer pageLimit,
                               final Integer sliceLimit)
      throws IOException, AirbyteCdkInvalidInputException, CdkProcessException {
    if (stream == null) {
      throw new AirbyteCdkInvalidInputException("Missing required `stream` field.");
    }
    final AirbyteRecordMessage record = request(manifest, config, state, readStreamCommand, stream, recordLimit, pageLimit, sliceLimit);
    return recordToResponse(record);
  }

  private StreamRead recordToResponse(final AirbyteRecordMessage record) {
    final StreamRead response = new StreamRead();
    final JsonNode data = record.getData();
    final List<StreamReadLogsInner> logList = convertToList(data.get("logs"), new TypeReference<>() {});
    final List<StreamReadSlicesInner> sliceList = convertToList(data.get("slices"), new TypeReference<>() {});
    response.setLogs(logList);
    response.setSlices(sliceList);
    response.setInferredSchema(data.get("inferred_schema"));
    response.setTestReadLimitReached(data.get("test_read_limit_reached").asBoolean());
    response.setLatestConfigUpdate(data.get("latest_config_update"));
    response.setInferredDatetimeFormats(data.get("inferred_datetime_formats"));
    final List<StreamReadAuxiliaryRequestsInner> auxiliaryRequests = convertToList(data.get("auxiliary_requests"), new TypeReference<>() {});
    response.setAuxiliaryRequests(auxiliaryRequests);
    return response;
  }

  /**
   * Launch a CDK process responsible for handling resolve_manifest requests.
   */
  @Override
  @Trace(operationName = TracingHelper.CONNECTOR_BUILDER_OPERATION_NAME)
  public ResolveManifest resolveManifest(final JsonNode manifest)
      throws IOException, AirbyteCdkInvalidInputException, CdkProcessException {
    final AirbyteRecordMessage record = request(manifest, CONFIG_NODE, resolveManifestCommand);
    return new ResolveManifest().manifest(record.getData().get("manifest"));
  }

  /**
   * Launch a CDK process responsible for handling requests.
   */
  private AirbyteRecordMessage request(final JsonNode manifest,
                                       final JsonNode config,
                                       final String cdkCommand)
      throws IOException, AirbyteCdkInvalidInputException, CdkProcessException {
    LOGGER.debug("Creating CDK process: {}.", cdkCommand);
    return this.commandRunner.runCommand(cdkCommand, this.adaptConfig(manifest, config, cdkCommand), "", "");
  }

  private AirbyteRecordMessage request(final JsonNode manifest,
                                       final JsonNode config,
                                       final List<JsonNode> state,
                                       final String cdkCommand,
                                       final String stream,
                                       final Integer recordLimit,
                                       final Integer pageLimit,
                                       final Integer sliceLimit)
      throws IOException, AirbyteCdkInvalidInputException, CdkProcessException {
    LOGGER.debug("Creating CDK process: {}.", cdkCommand);
    return this.commandRunner.runCommand(cdkCommand,
        this.adaptConfig(manifest, config, cdkCommand, recordLimit, pageLimit, sliceLimit),
        this.adaptCatalog(stream), this.adaptState(state));
  }

  private String adaptCatalog(final String stream) {
    return String.format(catalogTemplate, stream);
  }

  @VisibleForTesting
  String adaptState(final List<JsonNode> state) throws IOException {
    if (state == null || state.isEmpty()) {
      return OBJECT_WRITER.writeValueAsString(Collections.emptyList());
    } else {
      return OBJECT_WRITER.writeValueAsString(state);
    }
  }

  private String adaptConfig(final JsonNode manifest, final JsonNode config, final String command) throws IOException {
    final JsonNode adaptedConfig = Jsons.deserializeIfText(config).deepCopy();
    ((ObjectNode) adaptedConfig).set(manifestKey, Jsons.deserializeIfText(manifest));
    ((ObjectNode) adaptedConfig).put(commandKey, command);

    return OBJECT_WRITER.writeValueAsString(adaptedConfig);
  }

  private String adaptConfig(final JsonNode manifest,
                             final JsonNode config,
                             final String command,
                             final Integer userProvidedRecordLimit,
                             final Integer userProvidedPageLimit,
                             final Integer userProvidedSliceLimit)
      throws IOException {
    final JsonNode adaptedConfig = Jsons.deserializeIfText(config).deepCopy();
    ((ObjectNode) adaptedConfig).set(manifestKey, Jsons.deserializeIfText(manifest));
    ((ObjectNode) adaptedConfig).put(commandKey, command);

    final ObjectMapper mapper = new ObjectMapper();
    final ObjectNode commandConfig = mapper.createObjectNode();

    // TODO it would be nicer to collect all applicable error messages and throw a single time with all
    // violations
    // listed than the current strategy of throwing at the first issue you happen to check
    if (userProvidedRecordLimit != null) {
      if (userProvidedRecordLimit > maxRecordLimit) {
        throw new AirbyteCdkInvalidInputException(
            "Requested record limit of " + userProvidedRecordLimit + " exceeds maximum of " + maxRecordLimit + ".");
      }
      commandConfig.put(recordLimitKey, userProvidedRecordLimit);
    }

    if (userProvidedPageLimit != null) {
      if (userProvidedPageLimit > maxPageLimit) {
        throw new AirbyteCdkInvalidInputException("Requested page limit of " + userProvidedPageLimit + " exceeds maximum of " + maxPageLimit + ".");
      }
      commandConfig.put(pageLimitKey, userProvidedPageLimit);
    }

    if (userProvidedSliceLimit != null) {
      if (userProvidedSliceLimit > maxSliceLimit) {
        throw new AirbyteCdkInvalidInputException(
            "Requested slice limit of " + userProvidedSliceLimit + " exceeds maximum of " + maxSliceLimit + ".");
      }
      commandConfig.put(sliceLimitKey, userProvidedSliceLimit);
    }

    ((ObjectNode) adaptedConfig).set(commandConfigKey, commandConfig);

    return OBJECT_WRITER.writeValueAsString(adaptedConfig);
  }

  private <T> List<T> convertToList(final JsonNode object, final TypeReference<List<T>> typeReference) {
    final ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.registerModule(new JavaTimeModule());
    return objectMapper.convertValue(object, typeReference);
  }

}
