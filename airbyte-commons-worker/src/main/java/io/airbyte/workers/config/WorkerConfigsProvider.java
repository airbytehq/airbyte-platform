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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
    ORCHESTRATOR("orchestrator"),
    SOURCE("source");

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

  /**
   * Set of known resource sub types.
   */
  public enum ResourceSubType {

    API("api"),
    CUSTOM("custom"),
    DATABASE("database"),
    DEFAULT("default"),
    FILE("file");

    private final String value;
    private static final Map<String, ResourceSubType> CONSTANTS = new HashMap<>();

    static {
      for (final ResourceSubType r : values()) {
        CONSTANTS.put(r.value, r);
      }
    }

    ResourceSubType(final String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return this.value;
    }

    /**
     * Get a ResourceSubType from a string.
     *
     * @param value the string
     * @return the ResourceType
     */
    public static ResourceSubType fromValue(final String value) {
      final ResourceSubType type = CONSTANTS.get(value);
      if (type == null) {
        throw new IllegalArgumentException(String.format("Unknown ResourceSubType \"%s\"", value));
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

  private final Pattern kubeResourceKeyPattern;
  private final Map<String, Map<ResourceType, Map<ResourceSubType, KubeResourceConfig>>> kubeResourceConfigs;
  private final WorkerConfigsDefaults workerConfigsDefaults;

  record KubeResourceKey(String variant, ResourceType type, ResourceSubType subType) {}

  public WorkerConfigsProvider(final List<KubeResourceConfig> kubeResourceConfigs, final WorkerConfigsDefaults defaults) {
    this.kubeResourceKeyPattern = Pattern.compile(String.format("^((?<variant>[a-z]+)-)?(?<type>%s)(-(?<subtype>%s))?$",
        String.join("|", Arrays.stream(ResourceType.values()).map(ResourceType::toString).toList()),
        String.join("|", Arrays.stream(ResourceSubType.values()).map(ResourceSubType::toString).toList())),
        Pattern.CASE_INSENSITIVE);

    this.kubeResourceConfigs = new HashMap<>();
    for (final var config : kubeResourceConfigs) {
      final KubeResourceKey key = parseKubeResourceKey(config.getName())
          .orElseThrow(() -> new IllegalArgumentException(
              "Unsupported config name " + config.getName() + " doesn't match the (<variant>--)?<type>-<subtype> format"));
      final Map<ResourceType, Map<ResourceSubType, KubeResourceConfig>> typeMap =
          this.kubeResourceConfigs.computeIfAbsent(key.variant, k -> new HashMap<>());
      final Map<ResourceSubType, KubeResourceConfig> subTypeMap = typeMap.computeIfAbsent(key.type, k -> new HashMap<>());
      subTypeMap.put(key.subType, config);
    }

    this.workerConfigsDefaults = defaults;
  }

  /**
   * Get the WorkerConfigs associated to a given task.
   *
   * @param name of the Task.
   * @return the WorkerConfig.
   */
  public WorkerConfigs getConfig(final ResourceType name) {
    return getConfig(new KubeResourceKey(DEFAULT_VARIANT, name, ResourceSubType.DEFAULT));
  }

  /**
   * Get the WorkerConfigs associated to a given task.
   *
   * @return the WorkerConfig.
   */
  private WorkerConfigs getConfig(final KubeResourceKey key) {
    final KubeResourceConfig kubeResourceConfig = getKubeResourceConfig(key).orElseThrow();

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
    final ResourceSubType typedSubType =
        subType.map(s -> Objects.requireNonNullElse(ResourceSubType.CONSTANTS.get(s), ResourceSubType.DEFAULT)).orElse(ResourceSubType.DEFAULT);
    final KubeResourceKey key = new KubeResourceKey(
        variant,
        ResourceType.fromValue(type.toString().toLowerCase()),
        typedSubType);
    return getConfig(key).getResourceRequirements();
  }

  private Optional<KubeResourceConfig> getKubeResourceConfig(final KubeResourceKey key) {
    final Map<ResourceType, Map<ResourceSubType, KubeResourceConfig>> typeMap = getOrElseGet(kubeResourceConfigs, key.variant, DEFAULT_VARIANT);
    if (typeMap == null) {
      return Optional.empty();
    }

    final Map<ResourceSubType, KubeResourceConfig> subTypeMap = getOrElseGet(typeMap, key.type, ResourceType.DEFAULT);
    if (subTypeMap == null) {
      return Optional.empty();
    }

    return Optional.ofNullable(getOrElseGet(subTypeMap, key.subType, ResourceSubType.DEFAULT));
  }

  private void validateIsolatedPoolConfigInitialization(boolean useCustomNodeSelector, Map<String, String> isolatedNodeSelectors) {
    if (useCustomNodeSelector && isolatedNodeSelectors.isEmpty()) {
      throw new RuntimeException("Isolated Node selectors is empty while useCustomNodeSelector is set to true.");
    }
  }

  /**
   * Parses a kubeResourceKey.
   * <p>
   * The expected format is defined as follows `variant-type-subtype`. Type is mandatory, missing
   * variant will be replaced by DEFAULT_VARIANT, missing sub type will be replaced by
   * ResourceSubType.DEFAULT.
   */
  private Optional<KubeResourceKey> parseKubeResourceKey(final String value) {
    final Matcher matcher = kubeResourceKeyPattern.matcher(value.toLowerCase(Locale.getDefault()));
    if (matcher.matches()) {
      final String matchedSubType = matcher.group("subtype");
      return Optional.of(new KubeResourceKey(
          Objects.requireNonNullElse(matcher.group("variant"), DEFAULT_VARIANT),
          ResourceType.fromValue(matcher.group("type")),
          matchedSubType != null ? ResourceSubType.fromValue(matchedSubType) : ResourceSubType.DEFAULT));
    }
    return Optional.empty();
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

  /**
   * Helper function to get from a map.
   * <p>
   * Returns map.get(key) if key is present else returns map.get(fallbackKey)
   */
  private static <KeyT, ValueT> ValueT getOrElseGet(final Map<KeyT, ValueT> map, final KeyT key, final KeyT fallbackKey) {
    final ValueT lookup1 = map.get(key);
    return lookup1 != null ? lookup1 : map.get(fallbackKey);
  }

  private static String useDefaultIfEmpty(final String value, final String defaultValue) {
    return (value == null || value.isBlank()) ? defaultValue : value;
  }

}
