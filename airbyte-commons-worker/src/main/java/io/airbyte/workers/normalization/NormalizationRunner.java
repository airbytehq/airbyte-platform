/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.normalization;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.protocol.models.AirbyteTraceMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import java.nio.file.Path;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Normalization Runner. Executes normalization.
 */
public interface NormalizationRunner extends AutoCloseable {

  /**
   * After this method is called, the caller must call close. Previous to this method being called a
   * NormalizationRunner can be instantiated and not worry about close being called.
   *
   * @throws Exception - any exception thrown from normalization will be handled gracefully by the
   *         caller.
   */
  default void start() throws Exception {
    // no-op.
  }

  /**
   * Executes normalization of the data in the destination.
   *
   * @param jobId - id of the job that launched normalization
   * @param attempt - current attempt
   * @param jobRoot - root dir available for the runner to use.
   * @param config - configuration for connecting to the destination
   * @param catalog - the schema of the json blob in the destination. it is used normalize the blob
   *        into typed columns.
   * @param resourceRequirements - resource requirements
   * @return true of normalization succeeded. otherwise false.
   * @throws Exception - any exception thrown from normalization will be handled gracefully by the
   *         caller.
   */
  boolean normalize(String jobId,
                    int attempt,
                    final UUID connectionId,
                    final UUID workspaceId,
                    Path jobRoot,
                    JsonNode config,
                    ConfiguredAirbyteCatalog catalog,
                    ResourceRequirements resourceRequirements)
      throws Exception;

  Stream<AirbyteTraceMessage> getTraceMessages();

}
