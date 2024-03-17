/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.workers.config;

import io.micronaut.context.annotation.EachProperty;
import io.micronaut.context.annotation.Parameter;
import java.util.Objects;
import java.util.Optional;

/**
 * Encapsulates the configuration that is specific to Kubernetes. This is meant for the
 * WorkerConfigsProvider to be reading configs, not for direct use as merge logic isn't implemented
 * here. Important: we cannot distinguish between empty and non-existent environment variables in
 * this context, so we treat empty and non-existing strings as the same for our update logic. We use
 * the "&lt;EMPTY&gt;" literal to represent an empty string.
 */
@EachProperty("airbyte.worker.kube-job-configs")
public final class KubeResourceConfig implements Cloneable {

  public static final String EMPTY_VALUE = "<EMPTY>";

  private final String name;
  private String annotations;
  private String labels;
  private String nodeSelectors;
  private String cpuLimit;
  private String cpuRequest;
  private String memoryLimit;
  private String memoryRequest;

  public KubeResourceConfig(@Parameter final String name) {
    this.name = name;
  }

  public KubeResourceConfig clone() {
    try {
      return (KubeResourceConfig) super.clone();
    } catch (final CloneNotSupportedException e) {
      // Unlikely, but in the worst case, we will get this error when running tests.
      throw new RuntimeException(e);
    }
  }

  public KubeResourceConfig update(KubeResourceConfig other) {
    annotations = useOtherIfEmpty(other.annotations, annotations);
    labels = useOtherIfEmpty(other.labels, labels);
    nodeSelectors = useOtherIfEmpty(other.nodeSelectors, nodeSelectors);
    cpuLimit = useOtherIfEmpty(other.cpuLimit, cpuLimit);
    cpuRequest = useOtherIfEmpty(other.cpuRequest, cpuRequest);
    memoryLimit = useOtherIfEmpty(other.memoryLimit, memoryLimit);
    memoryRequest = useOtherIfEmpty(other.memoryRequest, memoryRequest);

    return this;
  }

  public String getName() {
    return name;
  }

  public String getAnnotations() {
    return resolveEmpty(annotations);
  }

  public String getLabels() {
    return resolveEmpty(labels);
  }

  public String getNodeSelectors() {
    return resolveEmpty(nodeSelectors);
  }

  public String getCpuLimit() {
    return resolveEmpty(cpuLimit);
  }

  public String getCpuRequest() {
    return resolveEmpty(cpuRequest);
  }

  public String getMemoryLimit() {
    return resolveEmpty(memoryLimit);
  }

  public String getMemoryRequest() {
    return resolveEmpty(memoryRequest);
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

  private static String useOtherIfEmpty(final String value, final String defaultValue) {
    return (value == null || value.isBlank()) ? defaultValue : value;
  }

  private static String resolveEmpty(final String value) {
    // Let's no return null values as it can be ambiguous
    return (Objects.equals(value, EMPTY_VALUE)) ? "" : Optional.ofNullable(value).orElse("");
  }

}
