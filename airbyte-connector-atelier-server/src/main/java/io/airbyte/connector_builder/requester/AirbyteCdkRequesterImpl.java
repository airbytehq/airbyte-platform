/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.requester;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.airbyte.connector_builder.api.model.generated.ResolveManifest;
import io.airbyte.connector_builder.api.model.generated.StreamRead;
import io.airbyte.connector_builder.api.model.generated.StreamReadSlicesInner;
import io.airbyte.connector_builder.api.model.generated.StreamsListRead;
import io.airbyte.connector_builder.api.model.generated.StreamsListReadStreamsInner;
import io.airbyte.connector_builder.command_runner.SynchronousCdkCommandRunner;
import io.airbyte.connector_builder.exceptions.AirbyteCdkInvalidInputException;
import io.airbyte.connector_builder.exceptions.CdkProcessException;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.List;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Construct and send requests to the CDK's Connector Builder handler.
 */
@Singleton
public class AirbyteCdkRequesterImpl implements AirbyteCdkRequester {

  private static final String commandKey = "__command";
  private static final String commandConfigKey = "__test_read_config";
  private static final String manifestKey = "__injected_declarative_manifest";
  private static final String recordLimitKey = "max_records";
  private static final String resolveManifestCommand = "resolve_manifest";
  private static final String listStreamsCommand = "list_streams";
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
                                                      "sync_mode": "full_refresh",
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
  public StreamRead readStream(final JsonNode manifest, final JsonNode config, final String stream, final Integer recordLimit)
      throws IOException, AirbyteCdkInvalidInputException, CdkProcessException {
    if (stream == null) {
      throw new CdkProcessException("Missing required `stream` field.");
    }
    final AirbyteRecordMessage record = request(manifest, config, readStreamCommand, stream, recordLimit);
    final StreamRead response = new StreamRead();
    final JsonNode data = record.getData();
    final List<Object> logList = convertToList(data.get("logs"), new TypeReference<List<Object>>() {});
    final List<StreamReadSlicesInner> sliceList = convertToList(data.get("slices"), new TypeReference<List<StreamReadSlicesInner>>() {});
    response.setLogs(logList);
    response.setSlices(sliceList);
    response.setInferredSchema(data.get("inferred_schema"));
    response.setTestReadLimitReached(data.get("test_read_limit_reached").asBoolean());
    return response;
  }

  /**
   * Launch a CDK process responsible for handling resolve_manifest requests.
   */
  @Override
  public ResolveManifest resolveManifest(final JsonNode manifest)
      throws IOException, AirbyteCdkInvalidInputException, CdkProcessException {
    final AirbyteRecordMessage record = request(manifest, CONFIG_NODE, resolveManifestCommand);
    return new ResolveManifest().manifest(record.getData().get("manifest"));
  }

  @Override
  public StreamsListRead listStreams(final JsonNode manifest, final JsonNode config)
      throws IOException, AirbyteCdkInvalidInputException, CdkProcessException {
    return new StreamsListRead().streams(
        StreamSupport.stream(request(manifest, config, listStreamsCommand).getData().get("streams").spliterator(), false).map(this::adaptStream)
            .toList());
  }

  private StreamsListReadStreamsInner adaptStream(final JsonNode stream) {
    if (isNull(stream, "name")) {
      throw new CdkProcessException(String.format(
          "Unexpected fatal error: streams are expected to have field 'name' but could not find it in %s. Please open a GitHub issue with Airbyte",
          stream));
    }
    if (isNull(stream, "url")) {
      throw new CdkProcessException(String.format(
          "Unexpected fatal error: streams are expected to have field 'url' but could not find it in %s. Please open a GitHub issue with Airbyte",
          stream));
    }

    return new StreamsListReadStreamsInner().name(stream.get("name").asText()).url(stream.get("url").asText());
  }

  private static boolean isNull(final JsonNode jsonNode, final String fieldName) {
    return jsonNode.get(fieldName) == null || jsonNode.get(fieldName).isNull();
  }

  /**
   * Launch a CDK process responsible for handling requests.
   */
  private AirbyteRecordMessage request(final JsonNode manifest,
                                       final JsonNode config,
                                       final String cdkCommand)
      throws IOException, AirbyteCdkInvalidInputException, CdkProcessException {
    LOGGER.debug("Creating CDK process: {}.", cdkCommand);
    return this.commandRunner.runCommand(cdkCommand, this.adaptConfig(manifest, config, cdkCommand), "");
  }

  private AirbyteRecordMessage request(final JsonNode manifest,
                                       final JsonNode config,
                                       final String cdkCommand,
                                       final String stream,
                                       final Integer recordLimit)
      throws IOException, AirbyteCdkInvalidInputException, CdkProcessException {
    LOGGER.debug("Creating CDK process: {}.", cdkCommand);
    return this.commandRunner.runCommand(cdkCommand, this.adaptConfig(manifest, config, cdkCommand, recordLimit), this.adaptCatalog(stream));
  }

  private String adaptCatalog(final String stream) {
    return String.format(catalogTemplate, stream);
  }

  private String adaptConfig(final JsonNode manifest, final JsonNode config, final String command) throws IOException {
    final JsonNode adaptedConfig = config.deepCopy();
    ((ObjectNode) adaptedConfig).set(manifestKey, manifest);
    ((ObjectNode) adaptedConfig).put(commandKey, command);

    return OBJECT_WRITER.writeValueAsString(adaptedConfig);
  }

  private String adaptConfig(final JsonNode manifest, final JsonNode config, final String command, final Integer recordLimit) throws IOException {
    final JsonNode adaptedConfig = config.deepCopy();
    ((ObjectNode) adaptedConfig).set(manifestKey, manifest);
    ((ObjectNode) adaptedConfig).put(commandKey, command);

    final ObjectMapper mapper = new ObjectMapper();
    final ObjectNode commandConfig = mapper.createObjectNode();
    commandConfig.put(recordLimitKey, recordLimit);
    ((ObjectNode) adaptedConfig).set(commandConfigKey, commandConfig);

    return OBJECT_WRITER.writeValueAsString(adaptedConfig);
  }

  private <T> List<T> convertToList(final JsonNode object, final TypeReference<List<T>> typeReference) {
    final ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.registerModule(new JavaTimeModule());
    return objectMapper.convertValue(object, typeReference);
  }

}
