/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container_orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.workers.process.AsyncKubePodStatus;
import io.airbyte.workers.process.KubeContainerInfo;
import io.airbyte.workers.process.KubePodInfo;
import io.airbyte.workers.storage.StorageClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AsyncStateManagerTest {

  public static final String FAKE_IMAGE = "fake_image";
  private static final KubePodInfo KUBE_POD_INFO = new KubePodInfo("default", "pod1",
      new KubeContainerInfo(FAKE_IMAGE, "IfNotPresent"));
  private static final String OUTPUT = "some output value";

  private StorageClient storageClient;
  private AsyncStateManager stateManager;

  @BeforeEach
  void setup() {
    storageClient = mock(StorageClient.class);
    stateManager = new AsyncStateManager(storageClient, KUBE_POD_INFO);
  }

  @Test
  void testEmptyWrite() {
    stateManager.write(AsyncKubePodStatus.INITIALIZING);

    // test for overwrite (which should be allowed)
    stateManager.write(AsyncKubePodStatus.INITIALIZING);

    final var key = stateManager.getDocumentStoreKey(AsyncKubePodStatus.INITIALIZING);
    verify(storageClient, times(2)).write(key, "");
  }

  @Test
  void testContentfulWrite() {
    stateManager.write(AsyncKubePodStatus.SUCCEEDED, OUTPUT);

    final var key = stateManager.getDocumentStoreKey(AsyncKubePodStatus.SUCCEEDED);
    verify(storageClient, times(1)).write(key, OUTPUT);
  }

  @Test
  void testReadingOutputWhenItExists() {
    final var key = stateManager.getDocumentStoreKey(AsyncKubePodStatus.SUCCEEDED);
    when(storageClient.read(key)).thenReturn(OUTPUT);
    assertEquals(OUTPUT, stateManager.getOutput());
  }

  @Test
  void testReadingOutputWhenItDoesNotExist() {
    // getting the output should throw an exception when there is no record in the document store
    assertThrows(IllegalArgumentException.class, () -> {
      stateManager.getOutput();
    });
  }

  @Test
  void testSuccessfulStatusRetrievalLifecycle() {
    when(storageClient.read(stateManager.getDocumentStoreKey(AsyncKubePodStatus.INITIALIZING))).thenReturn(null);
    final var beforeInitializingStatus = stateManager.getStatus();
    assertEquals(AsyncKubePodStatus.NOT_STARTED, beforeInitializingStatus);

    when(storageClient.read(stateManager.getDocumentStoreKey(AsyncKubePodStatus.INITIALIZING))).thenReturn("");
    final var initializingStatus = stateManager.getStatus();
    assertEquals(AsyncKubePodStatus.INITIALIZING, initializingStatus);

    when(storageClient.read(stateManager.getDocumentStoreKey(AsyncKubePodStatus.RUNNING))).thenReturn("");
    final var runningStatus = stateManager.getStatus();
    assertEquals(AsyncKubePodStatus.RUNNING, runningStatus);

    when(storageClient.read(stateManager.getDocumentStoreKey(AsyncKubePodStatus.SUCCEEDED))).thenReturn("output");
    final var succeededStatus = stateManager.getStatus();
    assertEquals(AsyncKubePodStatus.SUCCEEDED, succeededStatus);
  }

  @Test
  void testFailureStatusRetrievalLifecycle() {
    when(storageClient.read(stateManager.getDocumentStoreKey(AsyncKubePodStatus.INITIALIZING))).thenReturn(null);
    final var beforeInitializingStatus = stateManager.getStatus();
    assertEquals(AsyncKubePodStatus.NOT_STARTED, beforeInitializingStatus);

    when(storageClient.read(stateManager.getDocumentStoreKey(AsyncKubePodStatus.INITIALIZING))).thenReturn("");
    final var initializingStatus = stateManager.getStatus();
    assertEquals(AsyncKubePodStatus.INITIALIZING, initializingStatus);

    when(storageClient.read(stateManager.getDocumentStoreKey(AsyncKubePodStatus.RUNNING))).thenReturn("");
    final var runningStatus = stateManager.getStatus();
    assertEquals(AsyncKubePodStatus.RUNNING, runningStatus);

    when(storageClient.read(stateManager.getDocumentStoreKey(AsyncKubePodStatus.FAILED))).thenReturn("output");
    final var failedStatus = stateManager.getStatus();
    assertEquals(AsyncKubePodStatus.FAILED, failedStatus);
  }

}
