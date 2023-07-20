/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.config;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import io.airbyte.config.Configs.WorkerEnvironment;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.ResourceRequirementsType;
import io.airbyte.config.TolerationPOJO;
import io.airbyte.config.provider.ResourceRequirementsProvider;
import io.airbyte.workers.WorkerConfigs;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Provide WorkerConfigs.
 * <p>
 * This provider gathers the configuration from the application.yml that are nested under the
 * `airbyte.worker.kube-job-configs` key.
 */
@Singleton
@Slf4j
public class WorkerConfigsProvider implements ResourceRequirementsProvider {

  /**
   * Set of known resource types.
   */
  public enum ResourceType {

    // Global default
    DEFAULT("default"),

    // Command specific resources
    CHECK("check"),
    DISCOVER("discover"),
    NORMALIZATION("normalization"),
    REPLICATION("replication"),
    SPEC("spec"),

    // Sync related resources
    DESTINATION("destination"),
    DESTINATION_API("destination-api"),
    ORCHESTRATOR("orchestrator"),
    ORCHESTRATOR_API("orchestrator-api"),
    SOURCE("source"),
    SOURCE_DATABASE("source-database");

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

  private final Map<String, Map<ResourceType, KubeResourceConfig>> kubeResourceConfigsByVariantAndName;
  private final WorkerConfigsDefaults workerConfigsDefaults;

  public WorkerConfigsProvider(final List<KubeResourceConfig> kubeResourceConfigs, final WorkerConfigsDefaults defaults) {
    this.kubeResourceConfigsByVariantAndName = new HashMap<>();
    for (final var config : kubeResourceConfigs) {
      final Map.Entry<String, ResourceType> variantAndType = getVariantAndType(config.getName());
      final Map<ResourceType, KubeResourceConfig> typeToConfigMap =
          kubeResourceConfigsByVariantAndName.computeIfAbsent(variantAndType.getKey(), k -> new HashMap<>());
      typeToConfigMap.put(variantAndType.getValue(), config);
    }
    this.workerConfigsDefaults = defaults;
  }

  private static Map.Entry<String, ResourceType> getVariantAndType(final String value) {
    try {
      final ResourceType type = ResourceType.fromValue(value);
      return Map.entry(DEFAULT_VARIANT, type);
    } catch (final IllegalArgumentException e) {
      // See if it's a variant
      // Note: micronaut normalizes delimiters into a singe '-'
      final String[] splitValue = value.split("-", 2);
      final boolean hasVariantDelimiter = splitValue.length != 2;
      if (hasVariantDelimiter) {
        // No variant delimiter found, rethrow the initial exception.
        throw e;
      }

      final String variant = splitValue[0];
      final ResourceType type = ResourceType.fromValue(splitValue[1]);
      return Map.entry(variant, type);
    }
  }

  /**
   * Get the WorkerConfigs associated to a given task.
   *
   * @param name of the Task.
   * @return the WorkerConfig.
   */
  public WorkerConfigs getConfig(final ResourceType name) {
    return getConfig(name, DEFAULT_VARIANT);
  }

  /**
   * Get the WorkerConfigs associated to a given task.
   *
   * @param name of the Task.
   * @param variant of the Config.
   * @return the WorkerConfig.
   */
  private WorkerConfigs getConfig(final ResourceType name, final String variant) {
    final KubeResourceConfig kubeResourceConfig = getKubeResourceConfig(name, variant).orElseThrow();

    final Map<String, String> isolatedNodeSelectors = splitKVPairsFromEnvString(workerConfigsDefaults.isolatedNodeSelectors);
    validateIsolatedPoolConfigInitialization(workerConfigsDefaults.useCustomNodeSelector(), isolatedNodeSelectors);

    // if annotations are not defined for this specific resource, then fallback to the default
    // resource's annotations
    final Map<String, String> annotations;
    if (Strings.isNullOrEmpty(kubeResourceConfig.getAnnotations())) {
      annotations = splitKVPairsFromEnvString(workerConfigsDefaults.defaultKubeResourceConfig.getAnnotations());
    } else {
      annotations = splitKVPairsFromEnvString(kubeResourceConfig.getAnnotations());
    }

    return new WorkerConfigs(
        workerConfigsDefaults.workerEnvironment(),
        getResourceRequirementsFrom(kubeResourceConfig, workerConfigsDefaults.defaultKubeResourceConfig()),
        workerConfigsDefaults.jobKubeTolerations(),
        splitKVPairsFromEnvString(kubeResourceConfig.getNodeSelectors()),
        workerConfigsDefaults.useCustomNodeSelector() ? Optional.of(isolatedNodeSelectors) : Optional.empty(),
        annotations,
        workerConfigsDefaults.mainContainerImagePullSecret(),
        workerConfigsDefaults.mainContainerImagePullPolicy(),
        workerConfigsDefaults.sidecarContainerImagePullPolicy(),
        workerConfigsDefaults.socatImage(),
        workerConfigsDefaults.busyboxImage(),
        workerConfigsDefaults.curlImage(),
        workerConfigsDefaults.jobDefaultEnvMap());
  }

  @Override
  public ResourceRequirements getResourceRequirements(final ResourceRequirementsType type, final Optional<String> subType) {
    return getResourceRequirements(type, subType, DEFAULT_VARIANT);
  }

  @Override
  public ResourceRequirements getResourceRequirements(final ResourceRequirementsType type,
                                                      final Optional<String> subType,
                                                      final String variant) {
    for (final var kvp : getLookupList(type, subType, variant)) {
      try {
        return getConfig(kvp.getValue(), kvp.getKey()).getResourceRequirements();
      } catch (final Exception e) {
        // No such config, try the next candidate.
        continue;
      }
    }

    log.info("unable to find resource requirements for ({}, {}, {}), falling back to default", type, subType.orElse(""), variant);
    return getConfig(ResourceType.DEFAULT).getResourceRequirements();
  }

  /**
   * Build the ordered lookup list for resource requirements.
   * <p>
   * List is as follows [(variant, exact type), (variant, fallback type), (variant, default)]. For a
   * non default variant, we append [(default, exact type), (default, fallback type), (default,
   * default)] to that list.
   */
  private List<Map.Entry<String, ResourceType>> getLookupList(final ResourceRequirementsType type,
                                                              final Optional<String> subType,
                                                              final String variant) {
    final List<Map.Entry<String, ResourceType>> lookupList = new ArrayList<>();

    final ResourceType actualType = inferResourceRequirementsType(type, subType);
    final ResourceType fallbackType = inferResourceRequirementsType(type, Optional.empty());

    lookupList.add(Map.entry(variant, actualType));
    lookupList.add(Map.entry(variant, fallbackType));
    lookupList.add(Map.entry(variant, ResourceType.DEFAULT));
    if (!variant.equals(DEFAULT_VARIANT)) {
      lookupList.add(Map.entry(DEFAULT_VARIANT, actualType));
      lookupList.add(Map.entry(DEFAULT_VARIANT, fallbackType));
      lookupList.add(Map.entry(DEFAULT_VARIANT, ResourceType.DEFAULT));
    }
    return lookupList;
  }

  @SuppressWarnings("PMD.EmptyCatchBlock")
  private ResourceType inferResourceRequirementsType(final ResourceRequirementsType type, final Optional<String> subType) {
    final String primaryTypeString = type.toString().toLowerCase();
    final String subTypeString = subType.map(String::toLowerCase).orElse(null);

    // if we have specific type-subtype ResourceType use it.
    if (subTypeString != null) {
      try {
        return ResourceType.fromValue(String.format("%s-%s", primaryTypeString, subTypeString));
      } catch (final IllegalArgumentException e) {
        // PrimaryType-SubType is unknown, safe to ignore since we are checking for existence
        // of an override
      }
    }

    // fallback to primary type if it exists, other use default
    try {
      return ResourceType.fromValue(primaryTypeString);
    } catch (final IllegalArgumentException e) {
      // primaryType is unknown, falling back to default
      return ResourceType.DEFAULT;
    }
  }

  private Optional<KubeResourceConfig> getKubeResourceConfig(final ResourceType name, final String variant) {
    final Map<ResourceType, KubeResourceConfig> resourceMap = kubeResourceConfigsByVariantAndName.get(variant);
    return resourceMap != null ? Optional.ofNullable(resourceMap.get(name)) : Optional.empty();
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
