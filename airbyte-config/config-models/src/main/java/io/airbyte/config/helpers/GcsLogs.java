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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
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

  @Override
  public List<String> tailCloudLog(final LogConfigs configs, final String logPath, final int numLines) throws IOException {
    LOGGER.debug("Tailing logs from GCS path: {}", logPath);
    final Storage gcsClient = getOrCreateGcsClient();

    LOGGER.debug("Start GCS list request.");

    final var ascendingTimestampBlobs = new ArrayList<Blob>();
    gcsClient.list(
        configs.getStorageConfig().getBuckets().getLog(),
        Storage.BlobListOption.prefix(logPath))
        .iterateAll()
        .forEach(ascendingTimestampBlobs::add);

    final var lines = new ArrayList<String>();

    LOGGER.debug("Start getting GCS objects.");
    final var inMemoryData = new ByteArrayOutputStream();
    // iterate through blobs in descending order (oldest first)
    for (var i = ascendingTimestampBlobs.size() - 1; i >= 0; i--) {
      inMemoryData.reset();

      final var blob = ascendingTimestampBlobs.get(i);
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
    LOGGER.debug("Retrieving logs from GCS path: {}", logPath);
    final Storage gcsClient = getOrCreateGcsClient();

    LOGGER.debug("Start GCS list and delete request.");
    final Page<Blob> blobs = gcsClient.list(configs.getStorageConfig().getBuckets().getLog(), Storage.BlobListOption.prefix(logPath));
    blobs.iterateAll().forEach(blob -> blob.delete(BlobSourceOption.generationMatch()));
    LOGGER.debug("Finished all deletes.");
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
