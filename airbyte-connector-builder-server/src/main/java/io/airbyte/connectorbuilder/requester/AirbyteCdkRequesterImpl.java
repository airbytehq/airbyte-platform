/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.requester;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import datadog.trace.api.Trace;
import io.airbyte.commons.json.Jsons;
import io.airbyte.connectorbuilder.TracingHelper;
import io.airbyte.connectorbuilder.api.model.generated.AuxiliaryRequest;
import io.airbyte.connectorbuilder.api.model.generated.ResolveManifest;
import io.airbyte.connectorbuilder.api.model.generated.StreamRead;
import io.airbyte.connectorbuilder.api.model.generated.StreamReadLogsInner;
import io.airbyte.connectorbuilder.api.model.generated.StreamReadSlicesInner;
import io.airbyte.connectorbuilder.commandrunner.SynchronousCdkCommandRunner;
import io.airbyte.connectorbuilder.exceptions.AirbyteCdkInvalidInputException;
import io.airbyte.connectorbuilder.exceptions.CdkProcessException;
import io.airbyte.protocol.models.v0.AirbyteRecordMessage;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
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
  static final Integer maxStreamLimit = 100;
  private static final String commandKey = "__command";
  private static final String commandConfigKey = "__test_read_config";
  private static final String manifestKey = "__injected_declarative_manifest";
  private static final String customComponentsKey = "__injected_components_py";
  private static final String customComponentsChecksumsKey = "__injected_components_py_checksums";
  private static final String recordLimitKey = "max_records";
  private static final String sliceLimitKey = "max_slices";
  private static final String streamLimitKey = "max_streams";
  private static final String pageLimitKey = "max_pages_per_slice";
  private static final String resolveManifestCommand = "resolve_manifest";
  private static final String fullResolveManifestCommand = "full_resolve_manifest";
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
                               final String customComponentsCode,
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
    final AirbyteRecordMessage record =
        request(manifest, customComponentsCode, config, state, readStreamCommand, stream, recordLimit, pageLimit, sliceLimit);
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
    final List<AuxiliaryRequest> auxiliaryRequests = convertToList(data.get("auxiliary_requests"), new TypeReference<>() {});
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
    final AirbyteRecordMessage record = request(manifest,
        null, // As of now, we don't validate custom python when resolving manifests.
        CONFIG_NODE,
        resolveManifestCommand);
    return new ResolveManifest().manifest(record.getData().get("manifest"));
  }

  /**
   * Launch a CDK process responsible for handling full_resolve_manifest requests.
   */
  @Override
  @Trace(operationName = TracingHelper.CONNECTOR_BUILDER_OPERATION_NAME)
  public ResolveManifest fullResolveManifest(final JsonNode manifest, final JsonNode config, final Integer streamLimit)
      throws IOException, AirbyteCdkInvalidInputException, CdkProcessException {
    final AirbyteRecordMessage record = request(manifest,
        null, // As of now, we don't validate custom python when resolving manifests.
        config,
        fullResolveManifestCommand,
        streamLimit);
    return new ResolveManifest().manifest(record.getData().get("manifest"));
  }

  /**
   * Launch a CDK process responsible for handling requests.
   */
  private AirbyteRecordMessage request(final JsonNode manifest,
                                       final String customComponentsCode,
                                       final JsonNode config,
                                       final String cdkCommand)
      throws IOException, AirbyteCdkInvalidInputException, CdkProcessException {
    LOGGER.debug("Creating CDK process: {}.", cdkCommand);
    return this.commandRunner.runCommand(cdkCommand, this.adaptConfig(manifest, customComponentsCode, config, cdkCommand), "", "");
  }

  private AirbyteRecordMessage request(final JsonNode manifest,
                                       final String customComponentsCode,
                                       final JsonNode config,
                                       final String cdkCommand,
                                       final Integer streamLimit)
      throws IOException, AirbyteCdkInvalidInputException, CdkProcessException {
    LOGGER.debug("Creating CDK process: {}.", cdkCommand);
    return this.commandRunner.runCommand(cdkCommand, this.adaptConfig(manifest, customComponentsCode, config, cdkCommand, streamLimit), "", "");
  }

  private AirbyteRecordMessage request(final JsonNode manifest,
                                       final String customComponentsCode,
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
        this.adaptConfig(manifest, customComponentsCode, config, cdkCommand, recordLimit, pageLimit, sliceLimit),
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

  /**
   * Calculates the MD5 checksum of the provided custom components code.
   *
   * Currently, this only calculates the MD5 but in future it may calculate other checksums in
   * addition or instead.
   *
   * @param customComponentsCode the custom components code to calculate the checksum for
   * @return a JsonNode containing the checksum(s)
   */
  private JsonNode calculateChecksums(final String customComponentsCode) {
    final HashFunction hashFunction = Hashing.md5();
    final String md5_checksum = hashFunction.hashString(customComponentsCode, StandardCharsets.UTF_8).toString();
    return Jsons.jsonNode(Collections.singletonMap("md5", md5_checksum));
  }

  private String adaptConfig(final JsonNode manifest,
                             final String customComponentsCode,
                             final JsonNode config,
                             final String command)
      throws IOException {
    final JsonNode adaptedConfig = Jsons.deserializeIfText(config).deepCopy();
    ((ObjectNode) adaptedConfig).set(manifestKey, Jsons.deserializeIfText(manifest));
    if (!StringUtils.isBlank(customComponentsCode)) {
      ((ObjectNode) adaptedConfig).put(customComponentsKey, customComponentsCode);
      ((ObjectNode) adaptedConfig).set(customComponentsChecksumsKey, calculateChecksums(customComponentsCode));
    }
    ((ObjectNode) adaptedConfig).put(commandKey, command);

    return OBJECT_WRITER.writeValueAsString(adaptedConfig);
  }

  private String adaptConfig(final JsonNode manifest,
                             final String customComponentsCode,
                             final JsonNode config,
                             final String command,
                             final Integer userProvidedStreamLimit)
      throws IOException {
    final JsonNode adaptedConfig = Jsons.deserializeIfText(config).deepCopy();
    ((ObjectNode) adaptedConfig).set(manifestKey, Jsons.deserializeIfText(manifest));
    ((ObjectNode) adaptedConfig).put(commandKey, command);
    if (!StringUtils.isBlank(customComponentsCode)) {
      ((ObjectNode) adaptedConfig).put(customComponentsKey, customComponentsCode);
      ((ObjectNode) adaptedConfig).set(customComponentsChecksumsKey, calculateChecksums(customComponentsCode));
    }
    final ObjectMapper mapper = new ObjectMapper();
    final ObjectNode commandConfig = mapper.createObjectNode();

    if (userProvidedStreamLimit != null) {
      if (userProvidedStreamLimit > maxStreamLimit) {
        throw new AirbyteCdkInvalidInputException(
            "Requested stream limit of " + userProvidedStreamLimit + " exceeded maximum of " + maxStreamLimit + ".");
      }
      commandConfig.put(streamLimitKey, userProvidedStreamLimit);
    }

    ((ObjectNode) adaptedConfig).set(commandConfigKey, commandConfig);

    return OBJECT_WRITER.writeValueAsString(adaptedConfig);
  }

  private String adaptConfig(final JsonNode manifest,
                             final String customComponentsCode,
                             final JsonNode config,
                             final String command,
                             final Integer userProvidedRecordLimit,
                             final Integer userProvidedPageLimit,
                             final Integer userProvidedSliceLimit)
      throws IOException {
    final JsonNode adaptedConfig = Jsons.deserializeIfText(config).deepCopy();
    ((ObjectNode) adaptedConfig).set(manifestKey, Jsons.deserializeIfText(manifest));
    ((ObjectNode) adaptedConfig).put(commandKey, command);
    if (!StringUtils.isBlank(customComponentsCode)) {
      ((ObjectNode) adaptedConfig).put(customComponentsKey, customComponentsCode);
      ((ObjectNode) adaptedConfig).set(customComponentsChecksumsKey, calculateChecksums(customComponentsCode));
    }
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
