/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.config;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import io.airbyte.config.Configs.WorkerEnvironment;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.TolerationPOJO;
import io.airbyte.workers.WorkerConfigs;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Provide WorkerConfigs.
 * <p>
 * This provider gathers the configuration from the application.yml that are nested under the
 * `airbyte.worker.kube-job-configs` key.
 */
@Singleton
public class WorkerConfigsProvider {

  /**
   * Set of known resource types.
   */
  public enum ResourceType {

    DEFAULT("default"),
    CHECK("check"),
    DISCOVER("discover"),
    NORMALIZATION("normalization"),
    REPLICATION("replication"),
    SPEC("spec");

    private final String value;
    private static final Map<String, ResourceType> CONSTANTS = new HashMap<>();

    static {
      for (final ResourceType r : values()) {
        CONSTANTS.put(r.value, r);
      }
    }

    ResourceType(final String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return this.value;
    }

    /**
     * Get a ResourceType from a string.
     *
     * @param value the string
     * @return the ResourceType
     */
    public static ResourceType fromValue(final String value) {
      final ResourceType type = CONSTANTS.get(value);
      if (type == null) {
        throw new IllegalArgumentException(String.format("Unknown ResourceType \"%s\"", value));
      }
      return type;
    }

  }

  @Singleton
  record WorkerConfigsDefaults(WorkerEnvironment workerEnvironment,
                               @Named("default") KubeResourceConfig defaultKubeResourceConfig,
                               List<TolerationPOJO> jobKubeTolerations,
                               @Value("${airbyte.worker.isolated.kube.node-selectors}") String isolatedNodeSelectors,
                               @Value("${airbyte.worker.isolated.kube.use-custom-node-selector}") boolean useCustomNodeSelector,
                               @Value("${airbyte.worker.job.kube.main.container.image-pull-secret}") List<String> mainContainerImagePullSecret,
                               @Value("${airbyte.worker.job.kube.main.container.image-pull-policy}") String mainContainerImagePullPolicy,
                               @Value("${airbyte.worker.job.kube.sidecar.container.image-pull-policy}") String sidecarContainerImagePullPolicy,
                               @Value("${airbyte.worker.job.kube.images.socat}") String socatImage,
                               @Value("${airbyte.worker.job.kube.images.busybox}") String busyboxImage,
                               @Value("${airbyte.worker.job.kube.images.curl}") String curlImage,
                               @Named("jobDefaultEnvMap") Map<String, String> jobDefaultEnvMap) {

  }

  private final Map<ResourceType, KubeResourceConfig> kubeResourceConfigsByName;
  private final WorkerConfigsDefaults workerConfigsDefaults;

  public WorkerConfigsProvider(final List<KubeResourceConfig> kubeResourceConfigs, final WorkerConfigsDefaults defaults) {
    this.kubeResourceConfigsByName = kubeResourceConfigs.stream()
        .collect(Collectors.toMap(c -> ResourceType.fromValue(c.getName()), Function.identity()));
    this.workerConfigsDefaults = defaults;
  }

  /**
   * Get the WorkerConfigs associated to a given task.
   *
   * @param name of the Task.
   * @return the WorkerConfig.
   */
  public WorkerConfigs getConfig(final ResourceType name) {
    final KubeResourceConfig kubeResourceConfig = getKubeResourceConfig(name).orElseThrow();

    final Map<String, String> isolatedNodeSelectors = splitKVPairsFromEnvString(workerConfigsDefaults.isolatedNodeSelectors);
    validateIsolatedPoolConfigInitialization(workerConfigsDefaults.useCustomNodeSelector(), isolatedNodeSelectors);
    return new WorkerConfigs(
        workerConfigsDefaults.workerEnvironment(),
        getResourceRequirementsFrom(kubeResourceConfig, workerConfigsDefaults.defaultKubeResourceConfig()),
        workerConfigsDefaults.jobKubeTolerations(),
        splitKVPairsFromEnvString(kubeResourceConfig.getNodeSelectors()),
        workerConfigsDefaults.useCustomNodeSelector() ? Optional.of(isolatedNodeSelectors) : Optional.empty(),
        splitKVPairsFromEnvString(kubeResourceConfig.getAnnotations()),
        splitKVPairsFromEnvString(kubeResourceConfig.getLabels()),
        workerConfigsDefaults.mainContainerImagePullSecret(),
        workerConfigsDefaults.mainContainerImagePullPolicy(),
        workerConfigsDefaults.sidecarContainerImagePullPolicy(),
        workerConfigsDefaults.socatImage(),
        workerConfigsDefaults.busyboxImage(),
        workerConfigsDefaults.curlImage(),
        workerConfigsDefaults.jobDefaultEnvMap());
  }

  private Optional<KubeResourceConfig> getKubeResourceConfig(final ResourceType name) {
    return Optional.ofNullable(kubeResourceConfigsByName.get(name));
  }

  private void validateIsolatedPoolConfigInitialization(boolean useCustomNodeSelector, Map<String, String> isolatedNodeSelectors) {
    if (useCustomNodeSelector && isolatedNodeSelectors.isEmpty()) {
      throw new RuntimeException("Isolated Node selectors is empty while useCustomNodeSelector is set to true.");
    }
  }

  /**
   * Splits key value pairs from the input string into a map. Each kv-pair is separated by a ','. The
   * key and the value are separated by '='.
   * <p>
   * For example:- The following represents two map entries
   * </p>
   * key1=value1,key2=value2
   *
   * @param input string
   * @return map containing kv pairs
   */
  private Map<String, String> splitKVPairsFromEnvString(final String input) {
    if (input == null || input.isBlank()) {
      return Map.of();
    }
    return Splitter.on(",")
        .splitToStream(input)
        .filter(s -> !Strings.isNullOrEmpty(s) && s.contains("="))
        .map(s -> s.split("="))
        .collect(Collectors.toMap(s -> s[0].trim(), s -> s[1].trim()));
  }

  private ResourceRequirements getResourceRequirementsFrom(final KubeResourceConfig kubeResourceConfig, final KubeResourceConfig defaultConfig) {
    return new ResourceRequirements()
        .withCpuLimit(useDefaultIfEmpty(kubeResourceConfig.getCpuLimit(), defaultConfig.getCpuLimit()))
        .withCpuRequest(useDefaultIfEmpty(kubeResourceConfig.getCpuRequest(), defaultConfig.getCpuRequest()))
        .withMemoryLimit(useDefaultIfEmpty(kubeResourceConfig.getMemoryLimit(), defaultConfig.getMemoryLimit()))
        .withMemoryRequest(useDefaultIfEmpty(kubeResourceConfig.getMemoryRequest(), defaultConfig.getMemoryRequest()));
  }

  private static String useDefaultIfEmpty(final String value, final String defaultValue) {
    return (value == null || value.isBlank()) ? defaultValue : value;
  }

}
