/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.workers.config;

import io.micronaut.context.annotation.EachProperty;
import io.micronaut.context.annotation.Parameter;

/**
 * Encapsulates the configuration that is specific to Kubernetes. This is meant for the
 * WorkerConfigsProvider to be reading configs, not for direct use as fallback logic isn't
 * implemented here.
 */
@EachProperty("airbyte.worker.kube-job-configs")
public final class KubeResourceConfig {

  private final String name;
  private String annotations;
  private String labels;
  private String nodeSelectors;
  private String cpuLimit;
  private String cpuRequest;
  private String memoryLimit;
  private String memoryRequest;
  private String ephemeralStorageLimit;
  private String ephemeralStorageRequest;

  public KubeResourceConfig(@Parameter final String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public String getAnnotations() {
    return annotations;
  }

  public String getLabels() {
    return labels;
  }

  public String getNodeSelectors() {
    return nodeSelectors;
  }

  public String getCpuLimit() {
    return cpuLimit;
  }

  public String getCpuRequest() {
    return cpuRequest;
  }

  public String getMemoryLimit() {
    return memoryLimit;
  }

  public String getMemoryRequest() {
    return memoryRequest;
  }

  public String getEphemeralStorageLimit() {
    return ephemeralStorageLimit;
  }

  public String getEphemeralStorageRequest() {
    return ephemeralStorageRequest;
  }

  public void setAnnotations(final String annotations) {
    this.annotations = annotations;
  }

  public void setLabels(final String labels) {
    this.labels = labels;
  }

  public void setNodeSelectors(final String nodeSelectors) {
    this.nodeSelectors = nodeSelectors;
  }

  public void setCpuLimit(final String cpuLimit) {
    this.cpuLimit = cpuLimit;
  }

  public void setCpuRequest(final String cpuRequest) {
    this.cpuRequest = cpuRequest;
  }

  public void setMemoryLimit(final String memoryLimit) {
    this.memoryLimit = memoryLimit;
  }

  public void setMemoryRequest(final String memoryRequest) {
    this.memoryRequest = memoryRequest;
  }

  public void setEphemeralStorageLimit(final String ephemeralStorageLimit) {
    this.ephemeralStorageLimit = ephemeralStorageLimit;
  }

  public void setEphemeralStorageRequest(final String ephemeralStorageRequest) {
    this.ephemeralStorageRequest = ephemeralStorageRequest;
  }

}
