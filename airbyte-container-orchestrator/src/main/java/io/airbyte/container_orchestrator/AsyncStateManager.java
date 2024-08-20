/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container_orchestrator;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.workers.process.AsyncKubePodStatus;
import io.airbyte.workers.process.KubePodInfo;
import io.airbyte.workers.storage.StorageClient;
import io.airbyte.workers.workload.WorkloadConstants;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.lang.invoke.MethodHandles;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The state manager writes the "truth" for states of the async pod process. If the store isn't
 * updated by the underlying pod, it will appear as failed.
 * <p>
 * It doesn't have a single value for a state. Instead, in a location on cloud storage or disk, it
 * writes every state it's encountered.
 *
 * @deprecated This class is used by Worker V1 and has been disabled in Worker V2. It will be
 *             removed once all releases are converted to use Worker V2.
 */
@Deprecated
@Singleton
@Requires(notEnv = WorkloadConstants.WORKER_V2_MICRONAUT_ENV)
public class AsyncStateManager {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final List<AsyncKubePodStatus> STATUS_CHECK_ORDER = List.of(
      // terminal states first
      AsyncKubePodStatus.FAILED,
      AsyncKubePodStatus.SUCCEEDED,
      // then check in progress state
      AsyncKubePodStatus.RUNNING,
      // then check for initialization state
      AsyncKubePodStatus.INITIALIZING);

  private final StorageClient storageClient;
  private final KubePodInfo kubePodInfo;

  public AsyncStateManager(@Named("stateDocumentStore") final StorageClient storageClient, final KubePodInfo kubePodInfo) {
    this.storageClient = storageClient;
    this.kubePodInfo = kubePodInfo;
  }

  /**
   * Writes an empty file to a location designated by the input status.
   */
  public void write(final AsyncKubePodStatus status, final String value) {
    final var key = getDocumentStoreKey(status);
    log.info("Writing async status {} for {}...", status, kubePodInfo);
    storageClient.write(key, value);
  }

  /**
   * Writes a file containing a string value to a location designated by the input status.
   */
  public void write(final AsyncKubePodStatus status) {
    write(status, "");
  }

  /**
   * Interprets the state given all written state messages for the pod.
   * <p>
   * Checks terminal states first, then running, then initialized. Defaults to not started.
   * <p>
   * The order matters here!
   */
  public AsyncKubePodStatus getStatus() {
    return STATUS_CHECK_ORDER.stream()
        .filter(this::statusFileExists)
        .findFirst()
        .orElse(AsyncKubePodStatus.NOT_STARTED);
  }

  /**
   * Get output.
   *
   * @return the output stored in the success file. This can be an empty string.
   * @throws IllegalArgumentException if no success file exists
   */
  public String getOutput() throws IllegalArgumentException {
    final var key = getDocumentStoreKey(AsyncKubePodStatus.SUCCEEDED);
    final var output = storageClient.read(key);

    if (output == null) {
      throw new IllegalArgumentException("Expected to retrieve output from a successfully completed pod!");
    }

    return output;

  }

  /**
   * IMPORTANT: Changing the storage location will orphan already existing kube pods when the new
   * version is deployed.
   */
  @VisibleForTesting
  String getDocumentStoreKey(final AsyncKubePodStatus status) {
    return kubePodInfo.namespace() + "/" + kubePodInfo.name() + "/" + status.name();
  }

  private boolean statusFileExists(final AsyncKubePodStatus status) {
    final var key = getDocumentStoreKey(status);
    return storageClient.read(key) != null;
  }

}
