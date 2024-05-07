/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Blob.BlobSourceOption;
import com.google.cloud.storage.Storage;
import io.airbyte.config.storage.GcsStorageConfig;
import io.airbyte.featureflag.DownloadGcsLogsInParallel;
import io.airbyte.featureflag.TestClient;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import kotlin.Pair;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/**
 * GcsLogTest are unit tests that know a little too much about the internal workings of the GcsLogs
 * class. Ideally this would integrate into some kind of GCS localstack or testcontainers. However,
 * there doesn't appear to be a GCS localstack option and testcontainers does not have GCS Bucket
 * support.
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
@Timeout(60)
class GcsLogsTest {

  // parallel-downloading 10k blobs with 10ms latency takes 4sec on my laptop.
  // serially downloading 10 blobs with 10ms latency will take about 1min40s (100_000ms == 1min40s).
  // So by setting the timeout to 60 seconds, we can make sure the test fails is the parallelism is
  // removed.
  private static final int LARGE_BLOB_COUNT = 10_000;

  private static final String bucketName = "bucket";
  private static final String logPath = "/log/path";
  @Mock
  Storage storage;
  @Mock
  LogConfigs logConfigs;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  GcsStorageConfig storageConfig;
  @Mock
  Page<Blob> page;

  private AutoCloseable closeable;

  @BeforeEach
  void setup() {
    closeable = MockitoAnnotations.openMocks(this);
    when(logConfigs.getStorageConfig()).thenReturn(storageConfig);
    when(storageConfig.getBuckets().getLog()).thenReturn(bucketName);
  }

  @AfterEach
  void teardown() throws Exception {
    GcsLogs.resetGcs();
    closeable.close();
  }

  private static AtomicInteger blobIndex = new AtomicInteger(0);

  private Blob mockBlob(String content, int latencyMs) {
    Blob blob = mock(Blob.class);
    doAnswer(i -> {
      Thread.sleep(latencyMs);
      Files.writeString((Path) (i.getArgument(0)), content, StandardCharsets.UTF_8);
      return null;
    }).when(blob).downloadTo(Mockito.any(Path.class));
    doAnswer(i -> {
      Thread.sleep(latencyMs);
      ((OutputStream) i.getArgument(0)).write(content.getBytes(StandardCharsets.UTF_8));
      return null;
    }).when(blob).downloadTo(Mockito.any(OutputStream.class));
    when(blob.getName()).thenReturn("blob" + blobIndex.incrementAndGet());
    return blob;
  }

  @Test
  void testTailCloudLogNoLatency() throws IOException {
    checkCloudLogTail(3, 3, 0);
  }

  @Test
  void testTailLargeCloudLogNoLatency() throws IOException {
    checkCloudLogTail(1, LARGE_BLOB_COUNT, 0);
  }

  @Test
  void testTailLargeCloudLogWithLatency() throws IOException {
    checkCloudLogTail(1, LARGE_BLOB_COUNT, 10);
  }

  private Pair<List<String>, List<Blob>> createBlobs(int linesPerBlob, int blobCount, int latencyMs) {
    List<String> lines = IntStream.rangeClosed(1, linesPerBlob * blobCount).boxed().map(i -> ("lines " + i)).toList();
    int lastLineIndex = 0;
    List<Blob> blobs = new ArrayList<>(blobCount);
    for (int i = 0; i < blobCount; i++) {
      String stringInBlob = StringUtils.join(lines.subList(lastLineIndex, lastLineIndex + linesPerBlob), "\n") + "\n";
      blobs.add(mockBlob(stringInBlob, latencyMs));
      lastLineIndex = lastLineIndex + linesPerBlob;
    }
    return new Pair<>(lines, blobs);
  }

  void checkCloudLogTail(int linesPerBlob, int blobCount, int latencyMs) throws IOException {
    int totalLineCount = linesPerBlob * blobCount;
    int logsToTail = 2 * totalLineCount / 3;
    Pair<List<String>, List<Blob>> linesAndBlobs = createBlobs(linesPerBlob, blobCount, latencyMs);

    when(storage.list(bucketName, Storage.BlobListOption.prefix(logPath))).thenReturn(page);
    when(page.iterateAll()).thenReturn(linesAndBlobs.getSecond());

    final var gcsLogs = new GcsLogs(() -> storage);

    checkCloudLogTail(gcsLogs, linesAndBlobs.getFirst().subList(totalLineCount - logsToTail, totalLineCount),
        logsToTail, latencyMs);

    checkCloudLogTail(gcsLogs, linesAndBlobs.getFirst().subList(totalLineCount - 1, totalLineCount),
        1, latencyMs);

    checkCloudLogTail(gcsLogs, linesAndBlobs.getFirst().subList(0, totalLineCount),
        totalLineCount * 2, latencyMs);
  }

  void checkCloudLogTail(GcsLogs gcsLogs, List<String> expectedLogs, int numLines, int latencyMs) throws IOException {
    assertEquals(expectedLogs,
        gcsLogs.tailCloudLog(logConfigs, logPath, numLines, new TestClient(Map.of(DownloadGcsLogsInParallel.INSTANCE.getKey(), true))),
        "items should have been returned in the correct order");
    if (latencyMs == 0 || expectedLogs.size() < 100) {
      assertEquals(expectedLogs,
          gcsLogs.tailCloudLog(logConfigs, logPath, numLines, new TestClient(Map.of(DownloadGcsLogsInParallel.INSTANCE.getKey(), false))),
          "items should have been returned in the correct order");
    }
  }

  @Test
  void testDeleteLogs() {
    final var blob1 = mock(Blob.class);
    final var blob2 = mock(Blob.class);
    final var blob3 = mock(Blob.class);

    when(storage.list(bucketName, Storage.BlobListOption.prefix(logPath))).thenReturn(page);
    when(page.iterateAll()).thenReturn(List.of(blob1, blob2, blob3));

    final var gcsLogs = new GcsLogs(() -> storage);
    gcsLogs.deleteLogs(logConfigs, logPath);
    // each Blob should have delete called on it
    verify(blob1).delete(BlobSourceOption.generationMatch());
    verify(blob2).delete(BlobSourceOption.generationMatch());
    verify(blob3).delete(BlobSourceOption.generationMatch());
  }

  @Test
  void testDownloadCloudLogNoLatency() throws IOException {
    checkDownloadedCloudLogs(3, 3, 0);
  }

  @Test
  void testDownloadLargeCloudLogNoLatency() throws IOException {
    checkDownloadedCloudLogs(1, LARGE_BLOB_COUNT, 0);
  }

  @Test
  void testDownloadLargeCloudLogWithLatency() throws IOException {
    checkDownloadedCloudLogs(1, LARGE_BLOB_COUNT, 10);
  }

  void checkDownloadedCloudLogs(int linesPerBlob, int blobCount, int latencyMs) throws IOException {
    Pair<List<String>, List<Blob>> linesAndBlobs = createBlobs(linesPerBlob, blobCount, latencyMs);

    when(storage.list(bucketName, Storage.BlobListOption.prefix(logPath))).thenReturn(page);

    when(page.iterateAll()).thenReturn(linesAndBlobs.getSecond());

    final var gcsLogs = new GcsLogs(() -> storage);
    final var configKey = DownloadGcsLogsInParallel.INSTANCE.getKey();
    var logs = gcsLogs.tailCloudLog(logConfigs, logPath, Integer.MAX_VALUE, new TestClient(Map.of(configKey, true)));
    assertNotNull(logs, "log must not be null");
    assertEquals(linesAndBlobs.getFirst(), logs);

    if (latencyMs == 0 || blobCount < 100) {
      logs = gcsLogs.tailCloudLog(logConfigs, logPath, Integer.MAX_VALUE, new TestClient(Map.of(configKey, false)));
      assertNotNull(logs, "log must not be null");
      assertEquals(linesAndBlobs.getFirst(), logs);
    }

  }

}
