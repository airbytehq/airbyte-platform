/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers;

import static io.airbyte.featureflag.ContextKt.ANONYMOUS;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Blob.BlobSourceOption;
import com.google.cloud.storage.Storage;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import datadog.trace.api.Trace;
import io.airbyte.featureflag.DownloadGcsLogsInParallel;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.Workspace;
import io.opentracing.util.GlobalTracer;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
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

  private static class BlobQueue implements AutoCloseable {

    static final BlockingQueue<Path> fileQueue = new LinkedBlockingQueue<>();

    BlockingQueue<Future<BufferedReader>> blobContents = new LinkedBlockingQueue<>();
    private volatile boolean isClosed = false;
    private volatile boolean hasMore = true;
    private final Thread enqueuerThread;

    // The BlobQueue objects allows to download several files almost-in-order, because that's how we're
    // going to parse them. We also want to limit the number of files we write on disk, to avoid any
    // issues with large directories.
    BlobQueue(Iterable<Blob> blobs) {
      try {
        LOGGER.info("Initiating creation of {} temporary files", TEMP_FILE_COUNT);
        Path dir = Files.createTempDirectory("gcslogs-");
        for (int i = 0; i < TEMP_FILE_COUNT; i++) {
          int index = i;
          fileQueue.add(dir.resolve(index + ".log"));
        }
        LOGGER.info("Completed creation of {} temporary files", TEMP_FILE_COUNT);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      this.enqueuerThread = Thread.ofVirtual().name("GcsLogsBlobQueue-enqueuer").start(() -> {
        int tasksEnqueued = 0;
        try {
          for (Blob blob : blobs) {
            if (isClosed) {
              LOGGER.info("BlobQueue was closed. Exiting");
              return;
            }
            Path file = fileQueue.poll(1, TimeUnit.SECONDS);
            if (file != null) {
              LOGGER.debug("enqueuing downloading task #{}", tasksEnqueued);
              blobContents.add(executor.submit(() -> {
                LOGGER.debug("Downloading blob into {}", file.getFileName());
                blob.downloadTo(file);
                LOGGER.debug("Downloaded blob into {}. Size={}kB", file.getFileName(), file.toFile().length() / 1024);

                return new BufferedReader(new FileReader(file.toFile(), StandardCharsets.UTF_8) {

                  public void close() throws IOException {
                    super.close();
                    fileQueue.add(file);
                  }

                });
              }));
              tasksEnqueued++;
            } else {
              LOGGER.debug("lock expired. Trying again");
            }
          }
        } catch (InterruptedException e) {
          LOGGER.info("Exception in enqueuer: " + e);
          throw new RuntimeException(e);
        } finally {
          LOGGER.debug("completing enqueue. Enqueued {} tasks total", tasksEnqueued);
          hasMore = false;
        }
      });
    }

    public void close() {
      try {
        LOGGER.info("closing BlobQueue");
        isClosed = true;
        enqueuerThread.join();
        for (Future<BufferedReader> task : blobContents) {
          task.get().close();
        }

        for (int i = 0; i < TEMP_FILE_COUNT; i++) {
          try {
            var file = fileQueue.take();
            Files.delete(file);
          } catch (InterruptedException | NoSuchFileException e) {
            return;
          }
        }
      } catch (IOException | InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

  }

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
    final List<String> retVal;
    if (featureFlagClient.boolVariation(DownloadGcsLogsInParallel.INSTANCE, new Workspace(ANONYMOUS))) {
      return tailCloudLogInParallel(descending, logPath, numLines);
    } else {
      return tailCloudLogSerially(descending, logPath, numLines);
    }
  }

  @Trace
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
    final var activeSpan = GlobalTracer.get().activeSpan();
    activeSpan.setTag("airbyte.metadata.logPath", logPath);
    activeSpan.setTag("airbyte.metadata.requestedLineCount", numLines);
    activeSpan.setTag("airbyte.metadata.returnedLineCount", lines.size());
    activeSpan.setTag("airbyte.metadata.blobCount", descendingTimestampBlobs.size());
    return Lists.reverse(lines);
  }

  @Trace
  private List<String> tailCloudLogInParallel(final List<Blob> descendingTimestampBlobs, final String logPath, final int numLines)
      throws IOException {
    LOGGER.debug("Tailing {} lines from logs from GCS path: {}", numLines, logPath);
    final Storage gcsClient = getOrCreateGcsClient();

    LOGGER.debug("Start GCS list request.");

    final var lines = new ArrayList<String>();

    LOGGER.debug("Start getting GCS objects.");
    try (BlobQueue blobQueue = new BlobQueue(descendingTimestampBlobs)) {
      int i = 0;
      while (blobQueue.hasMore || !blobQueue.blobContents.isEmpty()) {
        LOGGER.debug("reading GCS object {}/{}. Got {} lines so far. Need {}", i++, descendingTimestampBlobs.size(), lines.size(), numLines);
        if (lines.size() >= numLines) {
          LOGGER.debug("exiting. Got {} lines, needed {}", lines.size(), numLines);
          break;
        }
        final var linesInCurrentBlob = new LinkedList<String>();
        // Iterate through the lines in reverse order. This ensures we keep the newer messages over the
        // older messages if we hit the numLines limit.
        try (final var fileReader = blobQueue.blobContents.take().get()) {
          fileReader.lines().forEach(s -> {
            // The current line is the newest in the file, we add it to end of the list
            linesInCurrentBlob.add(s);
            if (linesInCurrentBlob.size() > numLines - lines.size()) {
              // The first line in our linked list is the oldest in the current file, so that's the one we remove
              linesInCurrentBlob.removeFirst();
            }
          });
        } catch (ExecutionException | InterruptedException e) {
          throw new RuntimeException(e);
        }
        // We insert the lines in the current blob at the beginning of the linked list,
        // as they're older than the ones already in the list
        lines.addAll(0, linesInCurrentBlob);
        LOGGER.debug("read {} lines so far", lines.size());
      }

      final var activeSpan = GlobalTracer.get().activeSpan();
      activeSpan.setTag("airbyte.metadata.logPath", logPath);
      activeSpan.setTag("airbyte.metadata.requestedLineCount", numLines);
      activeSpan.setTag("airbyte.metadata.returnedLineCount", lines.size());
      activeSpan.setTag("airbyte.metadata.blobCount", descendingTimestampBlobs.size());
    }

    LOGGER.debug("Done retrieving GCS logs: {}. Read {} lines, needed {}", logPath, lines.size(), numLines);
    return lines;
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
