/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.workers.config;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import io.airbyte.config.ResourceRequirements;
import io.airbyte.config.ResourceRequirementsType;
import io.airbyte.config.TolerationPOJO;
import io.airbyte.config.provider.ResourceRequirementsProvider;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Provide WorkerConfigs.
 * <p>
 * This provider gathers the configuration from the application.yml that are nested under the
 * `airbyte.worker.kube-job-configs` key.
 */
@Singleton
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

  /**
   * Map used for converting ResourceRequirementsType into ResourceType.
   * <p>
   * A mapping is required because we may some slight naming misalignment. Eventually, we should only
   * have one enum which would remove the need for this map entirely.
   */
  private static final Map<ResourceRequirementsType, ResourceType> RSS_REQ_MAPPING;

  static {
    RSS_REQ_MAPPING = Map.of(
        ResourceRequirementsType.DESTINATION, ResourceType.DESTINATION,
        ResourceRequirementsType.ORCHESTRATOR, ResourceType.ORCHESTRATOR,
        ResourceRequirementsType.SOURCE, ResourceType.SOURCE);
  }

  @Singleton
  record WorkerConfigsDefaults(
                               @Named("default") KubeResourceConfig defaultKubeResourceConfig,
                               @Value("${airbyte.worker.job.kube.tolerations}") String jobKubeTolerations,
                               @Value("${airbyte.worker.isolated.kube.node-selectors}") String isolatedNodeSelectors,
                               @Value("${airbyte.worker.isolated.kube.use-custom-node-selector}") boolean useCustomNodeSelector,
                               @Value("${airbyte.worker.job.kube.main.container.image-pull-secret}") List<String> mainContainerImagePullSecret,
                               @Value("${airbyte.worker.job.kube.main.container.image-pull-policy}") String mainContainerImagePullPolicy) {

  }

  private final Pattern kubeResourceKeyPattern;
  private final Map<String, Map<ResourceType, Map<ResourceSubType, KubeResourceConfig>>> kubeResourceConfigs;
  private final WorkerConfigsDefaults workerConfigsDefaults;

  record KubeResourceKey(String variant, ResourceType type, ResourceSubType subType) {}

  public WorkerConfigsProvider(final List<KubeResourceConfig> kubeResourceConfigs, final WorkerConfigsDefaults defaults) {
    // In the variant name, we do not support uppercase. This is because micronaut normalizes uppercases
    // with dashes (CamelCase becomes camel-case) which is confusing because the variant name no longer
    // matches the config file.
    this.kubeResourceKeyPattern = Pattern.compile(String.format("^((?<variant>[a-z0-9]+)-)?(?<type>%s)(-(?<subtype>%s))?$",
        String.join("|", Arrays.stream(ResourceType.values()).map(ResourceType::toString).toList()),
        String.join("|", Arrays.stream(ResourceSubType.values()).map(ResourceSubType::toString).toList())),
        Pattern.CASE_INSENSITIVE);

    this.kubeResourceConfigs = new HashMap<>();
    for (final var config : kubeResourceConfigs) {
      final KubeResourceKey key = parseKubeResourceKey(config.getName())
          .orElseThrow(() -> new IllegalArgumentException(
              "Unsupported config name " + config.getName() + " doesn't match the (<variant>--)?<type>-<subtype> format"));
      final Map<ResourceType, Map<ResourceSubType, KubeResourceConfig>> typeMap =
          this.kubeResourceConfigs.computeIfAbsent(key.variant, k -> new EnumMap<>(ResourceType.class));
      final Map<ResourceSubType, KubeResourceConfig> subTypeMap = typeMap.computeIfAbsent(key.type, k -> new EnumMap<>(ResourceSubType.class));
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
    final KubeResourceConfig kubeResourceConfig = getKubeResourceConfig(key)
        .orElseThrow(() -> new NoSuchElementException(String.format("Unable to find config: {variant:%s, type:%s, subtype:%s}",
            key.variant, key.type, key.subType)));

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
        getResourceRequirementsFrom(kubeResourceConfig, workerConfigsDefaults.defaultKubeResourceConfig()),
        TolerationPOJO.getJobKubeTolerations(workerConfigsDefaults.jobKubeTolerations()),
        splitKVPairsFromEnvString(kubeResourceConfig.getNodeSelectors()),
        workerConfigsDefaults.useCustomNodeSelector() ? Optional.of(isolatedNodeSelectors) : Optional.empty(),
        annotations,
        splitKVPairsFromEnvString(kubeResourceConfig.getLabels()),
        workerConfigsDefaults.mainContainerImagePullSecret(),
        workerConfigsDefaults.mainContainerImagePullPolicy());
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
        RSS_REQ_MAPPING.get(type),
        typedSubType);
    return getConfig(key).getResourceRequirements();
  }

  /**
   * Look up resource configs given a key.
   * <p>
   * We are storing configs in a tree like structure. Look up should be handled as such. Keeping in
   * mind that we have defaults we want to fallback to, we should perform a complete scan of the
   * configs until we find a match to make sure we do not overlook a match.
   */
  private Optional<KubeResourceConfig> getKubeResourceConfig(final KubeResourceKey key) {
    // Look up by actual variant
    final var resultWithVariant = getKubeResourceConfigByType(kubeResourceConfigs.get(key.variant), key);
    if (resultWithVariant.isPresent()) {
      return resultWithVariant;
    }

    // no match with exact variant found, try again with the default.
    return getKubeResourceConfigByType(kubeResourceConfigs.get(DEFAULT_VARIANT), key);
  }

  private static Optional<KubeResourceConfig> getKubeResourceConfigByType(
                                                                          final Map<ResourceType, Map<ResourceSubType, KubeResourceConfig>> configs,
                                                                          final KubeResourceKey key) {
    if (configs == null) {
      return Optional.empty();
    }

    // Look up by actual type
    final var resultWithType = getKubeResourceConfigBySubType(configs.get(key.type), key);
    if (resultWithType.isPresent()) {
      return resultWithType;
    }

    // no match with exact type found, try again with the default.
    return getKubeResourceConfigBySubType(configs.get(ResourceType.DEFAULT), key);
  }

  private static Optional<KubeResourceConfig> getKubeResourceConfigBySubType(final Map<ResourceSubType, KubeResourceConfig> configBySubType,
                                                                             final KubeResourceKey key) {
    if (configBySubType == null) {
      return Optional.empty();
    }

    // Lookup by actual sub type
    final var config = configBySubType.get(key.subType);
    // if we didn't find a match, try again with the default
    return Optional.ofNullable(config != null ? config : configBySubType.get(ResourceSubType.DEFAULT));
  }

  private void validateIsolatedPoolConfigInitialization(final boolean useCustomNodeSelector, final Map<String, String> isolatedNodeSelectors) {
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
        .withMemoryRequest(useDefaultIfEmpty(kubeResourceConfig.getMemoryRequest(), defaultConfig.getMemoryRequest()))
        .withEphemeralStorageLimit(useDefaultIfEmpty(kubeResourceConfig.getEphemeralStorageLimit(), defaultConfig.getEphemeralStorageLimit()))
        .withEphemeralStorageRequest(useDefaultIfEmpty(kubeResourceConfig.getEphemeralStorageRequest(), defaultConfig.getEphemeralStorageRequest()));
  }

  private static String useDefaultIfEmpty(final String value, final String defaultValue) {
    return (value == null || value.isBlank()) ? defaultValue : value;
  }

}
