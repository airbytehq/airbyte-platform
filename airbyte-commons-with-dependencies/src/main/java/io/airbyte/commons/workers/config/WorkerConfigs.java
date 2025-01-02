/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.workers.config;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.config.Configs;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.TolerationPOJO;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Configuration object for a worker.
 */
public class WorkerConfigs {

  private final ResourceRequirements resourceRequirements;
  private final List<TolerationPOJO> workerKubeTolerations;
  private final Map<String, String> workerKubeNodeSelectors;
  private final Optional<Map<String, String>> workerIsolatedKubeNodeSelectors;
  private final Map<String, String> workerKubeAnnotations;
  private final Map<String, String> workerKubeLabels;
  private final List<String> jobImagePullSecrets;
  private final String jobImagePullPolicy;

  public WorkerConfigs(
                       final ResourceRequirements resourceRequirements,
                       final List<TolerationPOJO> workerKubeTolerations,
                       final Map<String, String> workerKubeNodeSelectors,
                       final Optional<Map<String, String>> workerIsolatedKubeNodeSelectors,
                       final Map<String, String> workerKubeAnnotations,
                       final Map<String, String> workerKubeLabels,
                       final List<String> jobImagePullSecrets,
                       final String jobImagePullPolicy) {
    this.resourceRequirements = resourceRequirements;
    this.workerKubeTolerations = workerKubeTolerations;
    this.workerKubeNodeSelectors = workerKubeNodeSelectors;
    this.workerIsolatedKubeNodeSelectors = workerIsolatedKubeNodeSelectors;
    this.workerKubeAnnotations = workerKubeAnnotations;
    this.workerKubeLabels = workerKubeLabels;
    this.jobImagePullSecrets = jobImagePullSecrets;
    this.jobImagePullPolicy = jobImagePullPolicy;
  }

  /**
   * Constructs a job-type-agnostic WorkerConfigs. For WorkerConfigs customized for specific
   * job-types, use static `build*JOBTYPE*WorkerConfigs` method if one exists.
   */
  @VisibleForTesting
  public WorkerConfigs(final Configs configs) {
    this(
        new ResourceRequirements(),
        configs.getJobKubeTolerations(),
        configs.getJobKubeNodeSelectors(),
        configs.getUseCustomKubeNodeSelector() ? Optional.of(configs.getIsolatedJobKubeNodeSelectors()) : Optional.empty(),
        configs.getJobKubeAnnotations(),
        configs.getJobKubeLabels(),
        configs.getJobKubeMainContainerImagePullSecrets(),
        configs.getJobKubeMainContainerImagePullPolicy());
  }

  public ResourceRequirements getResourceRequirements() {
    return resourceRequirements;
  }

  public List<TolerationPOJO> getWorkerKubeTolerations() {
    return workerKubeTolerations;
  }

  public Map<String, String> getworkerKubeNodeSelectors() {
    return workerKubeNodeSelectors;
  }

  public Optional<Map<String, String>> getWorkerIsolatedKubeNodeSelectors() {
    return workerIsolatedKubeNodeSelectors;
  }

  public Map<String, String> getWorkerKubeAnnotations() {
    return workerKubeAnnotations;
  }

  public Map<String, String> getWorkerKubeLabels() {
    return workerKubeLabels;
  }

  public List<String> getJobImagePullSecrets() {
    return jobImagePullSecrets;
  }

  public String getJobImagePullPolicy() {
    return jobImagePullPolicy;
  }

}
