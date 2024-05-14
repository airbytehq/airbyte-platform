/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Blob.BlobSourceOption;
import com.google.cloud.storage.Storage;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import io.airbyte.featureflag.FeatureFlagClient;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GCS Logs.
 */
@SuppressWarnings({"PMD.AvoidFileStream", "PMD.ShortVariable", "PMD.CloseResource", "PMD.AvoidInstantiatingObjectsInLoops"})
public class GcsLogs implements CloudLogs {

  private static final Logger LOGGER = LoggerFactory.getLogger(GcsLogs.class);

  private static Storage gcs;
  private final Supplier<Storage> gcsClientFactory;

  public GcsLogs(final Supplier<Storage> gcsClientFactory) {
    this.gcsClientFactory = gcsClientFactory;
  }

  private static final int TEMP_FILE_COUNT = 1000;
  private static final ExecutorService executor =
      Executors.newVirtualThreadPerTaskExecutor();

  @Override
  public List<String> tailCloudLog(final LogConfigs configs,
                                   final String logPath,
                                   final int numLines,
                                   final FeatureFlagClient featureFlagClient)
      throws IOException {
    LOGGER.debug("Tailing {} lines from logs from GCS path: {}", numLines, logPath);
    final Storage gcsClient = getOrCreateGcsClient();

    LOGGER.debug("Start GCS list request.");

    final LinkedList<Blob> descending = new LinkedList<>();
    gcsClient.list(
        configs.getStorageConfig().getBuckets().getLog(),
        Storage.BlobListOption.prefix(logPath))
        .iterateAll()
        .forEach(descending::addFirst);

    LOGGER.debug("Start getting GCS objects.");
    return tailCloudLogSerially(descending, logPath, numLines);
  }

  private List<String> tailCloudLogSerially(final List<Blob> descendingTimestampBlobs, final String logPath, final int numLines) throws IOException {
    final var inMemoryData = new ByteArrayOutputStream();
    final List<String> lines = new ArrayList<>();
    // iterate through blobs in descending order (oldest first)
    for (Blob blob : descendingTimestampBlobs) {
      inMemoryData.reset();

      blob.downloadTo(inMemoryData);

      final String[] currFileLines = inMemoryData.toString(StandardCharsets.UTF_8).split("\n");
      // Iterate through the lines in reverse order. This ensures we keep the newer messages over the
      // older messages if we hit the numLines limit.
      for (var j = currFileLines.length - 1; j >= 0; j--) {
        lines.add(currFileLines[j]);
        if (lines.size() >= numLines) {
          break;
        }
      }

      if (lines.size() >= numLines) {
        break;
      }
    }

    LOGGER.debug("Done retrieving GCS logs: {}.", logPath);
    // finally reverse the lines so they're returned in ascending order
    return Lists.reverse(lines);
  }

  @Override
  public void deleteLogs(final LogConfigs configs, final String logPath) {
    LOGGER.info("Retrieving logs from GCS path: {}", logPath);
    final Storage gcsClient = getOrCreateGcsClient();

    LOGGER.info("Start GCS list and delete request.");
    final Page<Blob> blobs = gcsClient.list(configs.getStorageConfig().getBuckets().getLog(), Storage.BlobListOption.prefix(logPath));
    blobs.iterateAll().forEach(blob -> blob.delete(BlobSourceOption.generationMatch()));
    LOGGER.info("Finished all deletes.");
  }

  private Storage getOrCreateGcsClient() {
    if (gcs == null) {
      gcs = gcsClientFactory.get();
    }
    return gcs;
  }

  /**
   * This method exists only for unit testing purposes.
   */
  @VisibleForTesting
  static void resetGcs() {
    gcs = null;
  }

}
