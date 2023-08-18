/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Blob.BlobSourceOption;
import com.google.cloud.storage.Storage;
import com.google.common.io.Files;
import io.airbyte.config.storage.CloudStorageConfigs;
import io.airbyte.config.storage.CloudStorageConfigs.GcsConfig;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
class GcsLogsTest {

  private static final String bucketName = "bucket";
  private static final String logPath = "/log/path";
  @Mock
  Storage storage;
  @Mock
  LogConfigs logConfigs;
  @Mock
  CloudStorageConfigs cloudStorageConfigs;
  @Mock
  GcsConfig gcsConfig;
  @Mock
  Page<Blob> page;
  @Mock
  Iterable<Blob> iterable;

  private AutoCloseable closeable;

  @BeforeEach
  void setup() {
    closeable = MockitoAnnotations.openMocks(this);
    when(logConfigs.getStorageConfigs()).thenReturn(cloudStorageConfigs);
    when(cloudStorageConfigs.getGcsConfig()).thenReturn(gcsConfig);
    when(gcsConfig.getBucketName()).thenReturn(bucketName);
  }

  @AfterEach
  void teardown() throws Exception {
    GcsLogs.resetGcs();
    closeable.close();
  }

  @Test
  void testTailCloudLog() throws IOException {
    final var blob1 = mock(Blob.class);
    final var blob2 = mock(Blob.class);
    final var blob3 = mock(Blob.class);

    // Ensure the Blob mocks write to the outputstream that is passed to their downloadTo method.
    // The first blob will contain the file contents:
    // line 1
    // line 2
    // line 3
    // the second blob will contain the file contents:
    // line 4
    // line 5
    // line 6
    // the third, and final, blob will contain the file contents:
    // line 7
    // line 8
    // line 9
    doAnswer(i -> {
      ((OutputStream) i.getArgument(0)).write("line 1\nline 2\nline 3\n".getBytes(StandardCharsets.UTF_8));
      return null;
    }).when(blob1).downloadTo(Mockito.any(OutputStream.class));

    doAnswer(i -> {
      ((OutputStream) i.getArgument(0)).write("line 4\nline 5\nline 6\n".getBytes(StandardCharsets.UTF_8));
      return null;
    }).when(blob2).downloadTo(Mockito.any(OutputStream.class));

    doAnswer(i -> {
      ((OutputStream) i.getArgument(0)).write("line 7\nline 8\nline 9\n".getBytes(StandardCharsets.UTF_8));
      return null;
    }).when(blob3).downloadTo(Mockito.any(OutputStream.class));

    when(storage.list(bucketName, Storage.BlobListOption.prefix(logPath))).thenReturn(page);
    when(page.iterateAll()).thenReturn(iterable);
    // Ensure the mock iterable's forEach method returns all three mocked blobs.
    doAnswer(i -> {
      ((Consumer<Blob>) i.getArgument(0)).accept(blob1);
      ((Consumer<Blob>) i.getArgument(0)).accept(blob2);
      ((Consumer<Blob>) i.getArgument(0)).accept(blob3);
      return null;
    }).when(iterable).forEach(Mockito.any(Consumer.class));

    final var gcsLogs = new GcsLogs(() -> storage);

    assertEquals(List.of("line 4", "line 5", "line 6", "line 7", "line 8", "line 9"),
        gcsLogs.tailCloudLog(logConfigs, logPath, 6),
        "the last 6 items should have been returned in the correct order");

    assertEquals(List.of("line 9"),
        gcsLogs.tailCloudLog(logConfigs, logPath, 1),
        "the last item should have been returned in the correct order");

    assertEquals(List.of("line 1", "line 2", "line 3", "line 4", "line 5", "line 6", "line 7", "line 8", "line 9"),
        gcsLogs.tailCloudLog(logConfigs, logPath, 1000),
        "all 9 items should have been returned in the correct order");
  }

  @Test
  void testDeleteLogs() {
    final var blob1 = mock(Blob.class);
    final var blob2 = mock(Blob.class);
    final var blob3 = mock(Blob.class);

    when(storage.list(bucketName, Storage.BlobListOption.prefix(logPath))).thenReturn(page);
    when(page.iterateAll()).thenReturn(iterable);
    // Ensure the mock iterable's forEach method returns all three mocked blobs.
    doAnswer(i -> {
      ((Consumer<Blob>) i.getArgument(0)).accept(blob1);
      ((Consumer<Blob>) i.getArgument(0)).accept(blob2);
      ((Consumer<Blob>) i.getArgument(0)).accept(blob3);
      return null;
    }).when(iterable).forEach(Mockito.any(Consumer.class));

    final var gcsLogs = new GcsLogs(() -> storage);
    gcsLogs.deleteLogs(logConfigs, logPath);
    // each Blob should have delete called on it
    verify(blob1).delete(BlobSourceOption.generationMatch());
    verify(blob2).delete(BlobSourceOption.generationMatch());
    verify(blob3).delete(BlobSourceOption.generationMatch());
  }

  @Test
  void testDownloadCloudLog() throws IOException {
    final var blob1 = mock(Blob.class);
    final var blob2 = mock(Blob.class);
    final var blob3 = mock(Blob.class);

    // Ensure the Blob mocks write to the outputstream that is passed to their downloadTo method.
    // The first blob will contain the file contents:
    // line 1
    // line 2
    // line 3
    // the second blob will contain the file contents:
    // line 4
    // line 5
    // line 6
    // the third, and final, blob will contain the file contents:
    // line 7
    // line 8
    // line 9
    doAnswer(i -> {
      ((OutputStream) i.getArgument(0)).write("line 1\nline 2\nline 3\n".getBytes(StandardCharsets.UTF_8));
      return null;
    }).when(blob1).downloadTo(Mockito.any(OutputStream.class));

    doAnswer(i -> {
      ((OutputStream) i.getArgument(0)).write("line 4\nline 5\nline 6\n".getBytes(StandardCharsets.UTF_8));
      return null;
    }).when(blob2).downloadTo(Mockito.any(OutputStream.class));

    doAnswer(i -> {
      ((OutputStream) i.getArgument(0)).write("line 7\nline 8\nline 9\n".getBytes(StandardCharsets.UTF_8));
      return null;
    }).when(blob3).downloadTo(Mockito.any(OutputStream.class));

    when(storage.list(
        bucketName,
        Storage.BlobListOption.prefix(logPath),
        Storage.BlobListOption.pageSize(LogClientSingleton.DEFAULT_PAGE_SIZE))).thenReturn(page);

    when(page.iterateAll()).thenReturn(iterable);
    // Ensure the mock iterable's forEach method returns all three mocked blobs.
    doAnswer(i -> {
      ((Consumer<Blob>) i.getArgument(0)).accept(blob1);
      ((Consumer<Blob>) i.getArgument(0)).accept(blob2);
      ((Consumer<Blob>) i.getArgument(0)).accept(blob3);
      return null;
    }).when(iterable).forEach(Mockito.any(Consumer.class));

    final var gcsLogs = new GcsLogs(() -> storage);
    final var logs = gcsLogs.downloadCloudLog(logConfigs, logPath);
    assertNotNull(logs, "log must not be null");

    final var expected = List.of("line 1", "line 2", "line 3", "line 4", "line 5", "line 6", "line 7", "line 8", "line 9");
    assertEquals(expected, Files.readLines(logs, StandardCharsets.UTF_8));
  }

}
