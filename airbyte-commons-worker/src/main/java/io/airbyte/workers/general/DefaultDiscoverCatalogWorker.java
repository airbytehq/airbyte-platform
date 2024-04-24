/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.general;

import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.CONNECTOR_VERSION_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ROOT_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.SOURCE_ID_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.WORKER_OPERATION_NAME;

import com.fasterxml.jackson.databind.JsonNode;
import datadog.trace.api.Trace;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.model.generated.DiscoverCatalogResult;
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaWriteRequestBody;
import io.airbyte.commons.constants.WorkerConstants;
import io.airbyte.commons.converters.CatalogClientConverters;
import io.airbyte.commons.converters.ConnectorConfigUpdater;
import io.airbyte.commons.io.LineGobbler;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.config.ConnectorJobOutput.OutputType;
import io.airbyte.config.FailureReason;
import io.airbyte.config.FailureReason.FailureOrigin;
import io.airbyte.config.StandardDiscoverCatalogInput;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.AirbyteControlConnectorConfigMessage;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.workers.WorkerUtils;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.internal.AirbyteStreamFactory;
import io.airbyte.workers.process.IntegrationLauncher;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default catalog worker.
 */
public class DefaultDiscoverCatalogWorker implements DiscoverCatalogWorker {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultDiscoverCatalogWorker.class);
  private static final String WRITE_DISCOVER_CATALOG_LOGS_TAG = "call to write discover schema result";

  private final IntegrationLauncher integrationLauncher;
  private final AirbyteStreamFactory streamFactory;
  private final ConnectorConfigUpdater connectorConfigUpdater;
  private final AirbyteApiClient airbyteApiClient;
  private volatile Process process;

  public DefaultDiscoverCatalogWorker(final AirbyteApiClient airbyteApiClient,
                                      final IntegrationLauncher integrationLauncher,
                                      final ConnectorConfigUpdater connectorConfigUpdater,
                                      final AirbyteStreamFactory streamFactory) {
    this.airbyteApiClient = airbyteApiClient;
    this.integrationLauncher = integrationLauncher;
    this.streamFactory = streamFactory;
    this.connectorConfigUpdater = connectorConfigUpdater;
  }

  @Trace(operationName = WORKER_OPERATION_NAME)
  @Override
  public ConnectorJobOutput run(final StandardDiscoverCatalogInput discoverSchemaInput, final Path jobRoot) throws WorkerException {
    ApmTraceUtils.addTagsToTrace(generateTraceTags(discoverSchemaInput, jobRoot));
    try {
      final JsonNode inputConfig = discoverSchemaInput.getConnectionConfiguration();
      process = integrationLauncher.discover(
          jobRoot,
          WorkerConstants.SOURCE_CONFIG_JSON_FILENAME,
          Jsons.serialize(inputConfig));

      final ConnectorJobOutput jobOutput = new ConnectorJobOutput()
          .withOutputType(OutputType.DISCOVER_CATALOG_ID);

      LineGobbler.gobble(process.getErrorStream(), LOGGER::error);

      final Map<Type, List<AirbyteMessage>> messagesByType = WorkerUtils.getMessagesByType(process, streamFactory, 30);

      final Optional<AirbyteCatalog> catalog = messagesByType
          .getOrDefault(Type.CATALOG, new ArrayList<>()).stream()
          .map(AirbyteMessage::getCatalog)
          .findFirst();

      final Optional<AirbyteControlConnectorConfigMessage> optionalConfigMsg = WorkerUtils.getMostRecentConfigControlMessage(messagesByType);
      if (optionalConfigMsg.isPresent() && WorkerUtils.getDidControlMessageChangeConfig(inputConfig, optionalConfigMsg.get())) {
        connectorConfigUpdater.updateSource(
            UUID.fromString(discoverSchemaInput.getSourceId()),
            optionalConfigMsg.get().getConfig());
        jobOutput.setConnectorConfigurationUpdated(true);
      }

      final Optional<FailureReason> failureReasonOptional =
          WorkerUtils.getJobFailureReasonFromMessages(OutputType.DISCOVER_CATALOG_ID, messagesByType);
      if (failureReasonOptional.isPresent()) {
        final FailureReason failureReason = failureReasonOptional.get();
        // any failure from a discover job's connector message is guaranteed to be a source failure,
        // so mark it as such.
        failureReason.setFailureOrigin(FailureOrigin.SOURCE);
        jobOutput.setFailureReason(failureReasonOptional.get());
      }

      final int exitCode = process.exitValue();
      if (exitCode != 0) {
        LOGGER.warn("Discover job subprocess finished with exit codee {}", exitCode);
      }

      if (catalog.isPresent()) {
        final String error = validateCatalog(catalog.get());
        if (!error.isEmpty()) {
          WorkerUtils.throwWorkerException(error, process);
        }
        final DiscoverCatalogResult result =
            AirbyteApiClient.retryWithJitter(() -> airbyteApiClient.getSourceApi()
                .writeDiscoverCatalogResult(buildSourceDiscoverSchemaWriteRequestBody(discoverSchemaInput, catalog.get())),
                WRITE_DISCOVER_CATALOG_LOGS_TAG);
        jobOutput.setDiscoverCatalogId(result.getCatalogId());
      } else if (failureReasonOptional.isEmpty()) {
        WorkerUtils.throwWorkerException("Integration failed to output a catalog struct and did not output a failure reason", process);
      }
      return jobOutput;
    } catch (final WorkerException e) {
      ApmTraceUtils.addExceptionToTrace(e);
      throw e;
    } catch (final Exception e) {
      ApmTraceUtils.addExceptionToTrace(e);
      throw new WorkerException("Error while discovering schema", e);
    }
  }

  // NOTE: This logic is getting moved as part of a bug fix, but appears to be missing functionality.
  // TODO: This logic should validate the path is traversable and not just that the keys exist in the
  // schema.
  private String validatePath(final JsonNode jsonSchema, final List<String> path) {
    for (final String key : path) {
      if (!jsonSchema.has(key)) {
        return String.format("key: '%s' of path: '%s' not found in schema", key, path);
      }
    }

    return "";
  }

  private String validateCatalog(final AirbyteCatalog persistenceCatalog) {
    final StringJoiner streamsWithFaultySchema = new StringJoiner(",");
    for (final AirbyteStream s : persistenceCatalog.getStreams()) {
      if (s.getJsonSchema() != null && s.getJsonSchema().has("properties") && s.getJsonSchema().get("properties") != null) {
        final JsonNode jsonSchema = s.getJsonSchema().get("properties");

        final List<String> defaultCursorPath = Objects.requireNonNullElse(s.getDefaultCursorField(), List.of());
        final var maybePathError = validatePath(jsonSchema, defaultCursorPath);
        if (!maybePathError.isEmpty()) {
          streamsWithFaultySchema.add(String.format(
              "Source defined cursor validation failed for stream: %s. Error: %s",
              s.getName(),
              maybePathError));
        }

        final List<List<String>> sourceDefinedPrimaryKeyPaths = Objects.requireNonNullElse(s.getSourceDefinedPrimaryKey(), List.of());
        for (final var path : sourceDefinedPrimaryKeyPaths) {
          final List<String> nonNullPath = Objects.requireNonNullElse(path, List.of());
          final var maybeErrorMsg = validatePath(jsonSchema, nonNullPath);
          if (!maybeErrorMsg.isEmpty()) {
            streamsWithFaultySchema.add(
                String.format(
                    "Source defined primary key validation failed for stream: %s. Error: %s",
                    s.getName(),
                    maybeErrorMsg));
          }
        }
      }
    }

    return streamsWithFaultySchema.toString();
  }

  private SourceDiscoverSchemaWriteRequestBody buildSourceDiscoverSchemaWriteRequestBody(final StandardDiscoverCatalogInput discoverSchemaInput,
                                                                                         final AirbyteCatalog catalog) {
    return new SourceDiscoverSchemaWriteRequestBody().catalog(
        CatalogClientConverters.toAirbyteCatalogClientApi(catalog)).sourceId(
            // NOTE: sourceId is marked required in the OpenAPI config but the code generator doesn't enforce
            // it, so we check again here.
            discoverSchemaInput.getSourceId() == null ? null : UUID.fromString(discoverSchemaInput.getSourceId()))
        .connectorVersion(
            discoverSchemaInput.getConnectorVersion())
        .configurationHash(
            discoverSchemaInput.getConfigHash());
  }

  private Map<String, Object> generateTraceTags(final StandardDiscoverCatalogInput discoverSchemaInput, final Path jobRoot) {
    final Map<String, Object> tags = new HashMap<>();

    tags.put(JOB_ROOT_KEY, jobRoot);

    if (discoverSchemaInput != null) {
      if (discoverSchemaInput.getSourceId() != null) {
        tags.put(SOURCE_ID_KEY, discoverSchemaInput.getSourceId());
      }
      if (discoverSchemaInput.getConnectorVersion() != null) {
        tags.put(CONNECTOR_VERSION_KEY, discoverSchemaInput.getConnectorVersion());
      }
    }

    return tags;
  }

  @Trace(operationName = WORKER_OPERATION_NAME)
  @Override
  public void cancel() {
    WorkerUtils.cancelProcess(process);
  }

}
