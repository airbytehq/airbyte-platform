/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.workers.config;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.config.Configs;
import io.airbyte.config.Configs.WorkerEnvironment;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.TolerationPOJO;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Configuration object for a worker.
 */
public class WorkerConfigs {

  private final Configs.WorkerEnvironment workerEnvironment;
  private final ResourceRequirements resourceRequirements;
  private final List<TolerationPOJO> workerKubeTolerations;
  private final Map<String, String> workerKubeNodeSelectors;
  private final Optional<Map<String, String>> workerIsolatedKubeNodeSelectors;
  private final Map<String, String> workerKubeAnnotations;
  private final Map<String, String> workerKubeLabels;
  private final List<String> jobImagePullSecrets;
  private final String jobImagePullPolicy;
  private final String sidecarImagePullPolicy;
  private final String jobSocatImage;
  private final String jobBusyboxImage;
  private final String jobCurlImage;
  private final Map<String, String> envMap;

  public WorkerConfigs(final WorkerEnvironment workerEnvironment,
                       final ResourceRequirements resourceRequirements,
                       final List<TolerationPOJO> workerKubeTolerations,
                       final Map<String, String> workerKubeNodeSelectors,
                       final Optional<Map<String, String>> workerIsolatedKubeNodeSelectors,
                       final Map<String, String> workerKubeAnnotations,
                       final Map<String, String> workerKubeLabels,
                       final List<String> jobImagePullSecrets,
                       final String jobImagePullPolicy,
                       final String sidecarImagePullPolicy,
                       final String jobSocatImage,
                       final String jobBusyboxImage,
                       final String jobCurlImage,
                       final Map<String, String> envMap) {
    this.workerEnvironment = workerEnvironment;
    this.resourceRequirements = resourceRequirements;
    this.workerKubeTolerations = workerKubeTolerations;
    this.workerKubeNodeSelectors = workerKubeNodeSelectors;
    this.workerIsolatedKubeNodeSelectors = workerIsolatedKubeNodeSelectors;
    this.workerKubeAnnotations = workerKubeAnnotations;
    this.workerKubeLabels = workerKubeLabels;
    this.jobImagePullSecrets = jobImagePullSecrets;
    this.jobImagePullPolicy = jobImagePullPolicy;
    this.sidecarImagePullPolicy = sidecarImagePullPolicy;
    this.jobSocatImage = jobSocatImage;
    this.jobBusyboxImage = jobBusyboxImage;
    this.jobCurlImage = jobCurlImage;
    this.envMap = envMap;
  }

  /**
   * Constructs a job-type-agnostic WorkerConfigs. For WorkerConfigs customized for specific
   * job-types, use static `build*JOBTYPE*WorkerConfigs` method if one exists.
   */
  @VisibleForTesting
  public WorkerConfigs(final Configs configs) {
    this(
        configs.getWorkerEnvironment(),
        new ResourceRequirements(),
        configs.getJobKubeTolerations(),
        configs.getJobKubeNodeSelectors(),
        configs.getUseCustomKubeNodeSelector() ? Optional.of(configs.getIsolatedJobKubeNodeSelectors()) : Optional.empty(),
        configs.getJobKubeAnnotations(),
        configs.getJobKubeLabels(),
        configs.getJobKubeMainContainerImagePullSecrets(),
        configs.getJobKubeMainContainerImagePullPolicy(),
        configs.getJobKubeSidecarContainerImagePullPolicy(),
        configs.getJobKubeSocatImage(),
        configs.getJobKubeBusyboxImage(),
        configs.getJobKubeCurlImage(),
        configs.getJobDefaultEnvMap());
  }

  public Configs.WorkerEnvironment getWorkerEnvironment() {
    return workerEnvironment;
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

  public String getSidecarImagePullPolicy() {
    return sidecarImagePullPolicy;
  }

  public String getJobSocatImage() {
    return jobSocatImage;
  }

  public String getJobBusyboxImage() {
    return jobBusyboxImage;
  }

  public String getJobCurlImage() {
    return jobCurlImage;
  }

  public Map<String, String> getEnvMap() {
    return envMap;
  }

}
