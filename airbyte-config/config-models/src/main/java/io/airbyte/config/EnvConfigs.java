/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import io.airbyte.commons.envvar.EnvVar;
import io.airbyte.commons.lang.Exceptions;
import io.airbyte.commons.map.MoreMaps;
import io.airbyte.commons.version.AirbyteVersion;
import io.airbyte.config.helpers.LogConfigs;
import io.airbyte.config.storage.GcsStorageConfig;
import io.airbyte.config.storage.LocalStorageConfig;
import io.airbyte.config.storage.MinioStorageConfig;
import io.airbyte.config.storage.S3StorageConfig;
import io.airbyte.config.storage.StorageBucketConfig;
import io.airbyte.config.storage.StorageConfig;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configs from environment variables.
 */
@SuppressWarnings({"PMD.LongVariable", "PMD.CyclomaticComplexity", "PMD.AvoidReassigningParameters", "PMD.ConstructorCallsOverridableMethod"})
public class EnvConfigs implements Configs {

  private static final Logger LOGGER = LoggerFactory.getLogger(EnvConfigs.class);
  private static final String DEFAULT_SIDECAR_MEMORY_REQUEST = "25Mi";
  private static final String DEFAULT_SIDECAR_KUBE_MEMORY_LIMIT = "50Mi";
  private static final String DEFAULT_SIDECAR_KUBE_CPU_REQUEST = "0.1";

  // Test show at least 1.5 CPU is required to hit >20 Mb/s. Overprovision to ensure sidecar resources
  // do not cause bottlenecks.
  // This is fine as the limit only affects whether the container is throttled by Kube. It does not
  // affect scheduling.
  private static final String DEFAULT_SIDECAR_KUBE_CPU_LIMIT = "2.0";
  // job-type-specific overrides
  private static final String DEFAULT_JOB_KUBE_NAMESPACE = "default";
  private static final String DEFAULT_JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_POLICY = "IfNotPresent";
  private static final String DEFAULT_JOB_KUBE_SIDECAR_CONTAINER_IMAGE_PULL_POLICY = "IfNotPresent";
  private static final String DEFAULT_JOB_KUBE_SOCAT_IMAGE = "alpine/socat:1.7.4.4-r0";
  public static final int DEFAULT_FAILED_JOBS_IN_A_ROW_BEFORE_CONNECTION_DISABLE = 100;
  public static final int DEFAULT_DAYS_OF_ONLY_FAILED_JOBS_BEFORE_CONNECTION_DISABLE = 14;

  public static final Map<String, Function<EnvConfigs, String>> JOB_SHARED_ENVS = Map.of(
      EnvVar.AIRBYTE_VERSION.name(), (instance) -> instance.getAirbyteVersion().serialize(),
      EnvVar.AIRBYTE_ROLE.name(), EnvConfigs::getAirbyteRole,
      EnvVar.DEPLOYMENT_MODE.name(), (instance) -> instance.getDeploymentMode().name(),
      EnvVar.WORKER_ENVIRONMENT.name(), (instance) -> instance.getWorkerEnvironment().name());

  private final Function<String, String> getEnv;
  private final Supplier<Set<String>> getAllEnvKeys;

  /**
   * Constructs {@link EnvConfigs} from actual environment variables.
   */
  public EnvConfigs() {
    this(System.getenv());
  }

  /**
   * Constructs {@link EnvConfigs} from a provided map. This can be used for testing or getting
   * variables from a non-envvar source.
   */
  public EnvConfigs(final Map<String, String> envMap) {
    this.getEnv = envMap::get;
    this.getAllEnvKeys = envMap::keySet;
  }

  private StorageConfig getLogConfiguration() {
    final var buckets = new StorageBucketConfig(
        getEnsureEnv(EnvVar.STORAGE_BUCKET_LOG),
        getEnsureEnv(EnvVar.STORAGE_BUCKET_STATE),
        getEnsureEnv(EnvVar.STORAGE_BUCKET_WORKLOAD_OUTPUT),
        getEnsureEnv(EnvVar.STORAGE_BUCKET_ACTIVITY_PAYLOAD));

    return switch (getEnsureEnv(EnvVar.STORAGE_TYPE)) {
      case "GCS" -> new GcsStorageConfig(
          buckets,
          getEnsureEnv(EnvVar.GOOGLE_APPLICATION_CREDENTIALS));
      case "LOCAL" -> new LocalStorageConfig(
          buckets,
          getEnv(EnvVar.LOCAL_ROOT));
      case "MINIO" -> new MinioStorageConfig(
          buckets,
          getEnsureEnv(EnvVar.AWS_ACCESS_KEY_ID),
          getEnsureEnv(EnvVar.AWS_ACCESS_KEY_ID),
          getEnsureEnv(EnvVar.AWS_DEFAULT_REGION));
      case "S3" -> new S3StorageConfig(
          buckets,
          getEnsureEnv(EnvVar.AWS_ACCESS_KEY_ID),
          getEnsureEnv(EnvVar.AWS_ACCESS_KEY_ID),
          getEnsureEnv(EnvVar.AWS_DEFAULT_REGION));
      default -> throw new IllegalArgumentException(EnvVar.STORAGE_TYPE.name() + " has is an unsupported value");
    };
  }

  // CORE
  // General
  @Override
  public String getAirbyteRole() {
    return getEnv(EnvVar.AIRBYTE_ROLE);
  }

  @Override
  public AirbyteVersion getAirbyteVersion() {
    return new AirbyteVersion(getEnsureEnv(EnvVar.AIRBYTE_VERSION));
  }

  @Override
  public String getAirbyteVersionOrWarning() {
    return Optional.ofNullable(getEnv(EnvVar.AIRBYTE_VERSION)).orElse("version not set");
  }

  @Override
  public WorkerEnvironment getWorkerEnvironment() {
    return getEnvOrDefault(EnvVar.WORKER_ENVIRONMENT, WorkerEnvironment.DOCKER, s -> WorkerEnvironment.valueOf(s.toUpperCase()));
  }

  @Override
  public Path getWorkspaceRoot() {
    return getPath(EnvVar.WORKSPACE_ROOT);
  }

  // Database
  @Override
  public String getDatabaseUser() {
    return getEnsureEnv(EnvVar.DATABASE_USER);
  }

  @Override
  public String getDatabasePassword() {
    return getEnsureEnv(EnvVar.DATABASE_PASSWORD);
  }

  @Override
  public String getDatabaseUrl() {
    return getEnsureEnv(EnvVar.DATABASE_URL);
  }

  /**
   * Returns worker pod tolerations parsed from its own environment variable. The value of the env is
   * a string that represents one or more tolerations.
   * <ul>
   * <li>Tolerations are separated by a `;`
   * <li>Each toleration contains k=v pairs mentioning some/all of key, effect, operator and value and
   * separated by `,`
   * </ul>
   * <p>
   * For example:- The following represents two tolerations, one checking existence and another
   * matching a value
   * <p>
   * key=airbyte-server,operator=Exists,effect=NoSchedule;key=airbyte-server,operator=Equals,value=true,effect=NoSchedule
   *
   * @return list of WorkerKubeToleration parsed from env
   */
  @Override
  public List<TolerationPOJO> getJobKubeTolerations() {
    final String tolerationsStr = getEnvOrDefault(EnvVar.JOB_KUBE_TOLERATIONS, "");

    final Stream<String> tolerations = Strings.isNullOrEmpty(tolerationsStr) ? Stream.of()
        : Splitter.on(";")
            .splitToStream(tolerationsStr)
            .filter(tolerationStr -> !Strings.isNullOrEmpty(tolerationStr));

    return tolerations
        .map(this::parseToleration)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  private TolerationPOJO parseToleration(final String tolerationStr) {
    final Map<String, String> tolerationMap = Splitter.on(",")
        .splitToStream(tolerationStr)
        .map(s -> s.split("="))
        .collect(Collectors.toMap(s -> s[0], s -> s[1]));

    if (tolerationMap.containsKey("key") && tolerationMap.containsKey("effect") && tolerationMap.containsKey("operator")) {
      return new TolerationPOJO(
          tolerationMap.get("key"),
          tolerationMap.get("effect"),
          tolerationMap.get("value"),
          tolerationMap.get("operator"));
    } else {
      LOGGER.warn(
          "Ignoring toleration {}, missing one of key,effect or operator",
          tolerationStr);
      return null;
    }
  }

  /**
   * Returns a map of node selectors for any job type. Used as a default if a particular job type does
   * not define its own node selector environment variable.
   *
   * @return map containing kv pairs of node selectors, or empty optional if none present.
   */
  @Override
  public Map<String, String> getJobKubeNodeSelectors() {
    return splitKVPairsFromEnvString(getEnvOrDefault(EnvVar.JOB_KUBE_NODE_SELECTORS, ""));
  }

  @Override
  public Map<String, String> getIsolatedJobKubeNodeSelectors() {
    return splitKVPairsFromEnvString(getEnvOrDefault(EnvVar.JOB_ISOLATED_KUBE_NODE_SELECTORS, ""));
  }

  @Override
  public boolean getUseCustomKubeNodeSelector() {
    return getEnvOrDefault(EnvVar.USE_CUSTOM_NODE_SELECTOR, false);
  }

  /**
   * Returns a map of annotations from its own environment variable. The value of the env is a string
   * that represents one or more annotations. Each kv-pair is separated by a `,`
   * <p>
   * For example:- The following represents two annotations
   * <p>
   * airbyte=server,type=preemptive
   *
   * @return map containing kv pairs of annotations
   */
  @Override
  public Map<String, String> getJobKubeAnnotations() {
    return splitKVPairsFromEnvString(getEnvOrDefault(EnvVar.JOB_KUBE_ANNOTATIONS, ""));
  }

  @Override
  public Map<String, String> getJobKubeLabels() {
    return splitKVPairsFromEnvString(getEnvOrDefault(EnvVar.JOB_KUBE_LABELS, ""));
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
  public Map<String, String> splitKVPairsFromEnvString(String input) {
    if (input == null) {
      input = "";
    }
    final Map<String, String> map = Splitter.on(",")
        .splitToStream(input)
        .filter(s -> !Strings.isNullOrEmpty(s) && s.contains("="))
        .map(s -> s.split("="))
        .collect(Collectors.toMap(s -> s[0].trim(), s -> s[1].trim()));
    return map.isEmpty() ? null : map;
  }

  @Override
  public String getJobKubeMainContainerImagePullPolicy() {
    return getEnvOrDefault(EnvVar.JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_POLICY, DEFAULT_JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_POLICY);
  }

  @Override
  public String getJobKubeSidecarContainerImagePullPolicy() {
    return getEnvOrDefault(EnvVar.JOB_KUBE_SIDECAR_CONTAINER_IMAGE_PULL_POLICY, DEFAULT_JOB_KUBE_SIDECAR_CONTAINER_IMAGE_PULL_POLICY);
  }

  /**
   * Returns the name of the secret to be used when pulling down docker images for jobs. Automatically
   * injected in the KubePodProcess class and used in the job pod templates.
   * <p>
   * Can provide multiple strings seperated by comma(,) to indicate pulling from different
   * repositories. The empty string is a no-op value.
   */
  @Override
  public List<String> getJobKubeMainContainerImagePullSecrets() {
    final String secrets = getEnvOrDefault(EnvVar.JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_SECRET, "");
    return Arrays.stream(secrets.split(",")).collect(Collectors.toList());
  }

  @Override
  public String getSidecarKubeCpuRequest() {
    return getEnvOrDefault(EnvVar.SIDECAR_KUBE_CPU_REQUEST, DEFAULT_SIDECAR_KUBE_CPU_REQUEST);
  }

  @Override
  public String getSidecarKubeCpuLimit() {
    return getEnvOrDefault(EnvVar.SIDECAR_KUBE_CPU_LIMIT, DEFAULT_SIDECAR_KUBE_CPU_LIMIT);
  }

  @Override
  public String getSidecarKubeMemoryLimit() {
    return getEnvOrDefault(EnvVar.SIDECAR_KUBE_MEMORY_LIMIT, DEFAULT_SIDECAR_KUBE_MEMORY_LIMIT);
  }

  @Override
  public String getSidecarMemoryRequest() {
    return getEnvOrDefault(EnvVar.SIDECAR_MEMORY_REQUEST, DEFAULT_SIDECAR_MEMORY_REQUEST);
  }

  @Override
  public String getSocatSidecarKubeCpuRequest() {
    return getEnvOrDefault(EnvVar.SOCAT_KUBE_CPU_REQUEST, getSidecarKubeCpuRequest());
  }

  @Override
  public String getSocatSidecarKubeCpuLimit() {
    return getEnvOrDefault(EnvVar.SOCAT_KUBE_CPU_LIMIT, getSidecarKubeCpuLimit());
  }

  /**
   * There are two types of environment variables available to the job container.
   * <ul>
   * <li>Exclusive variables prefixed with JOB_DEFAULT_ENV_PREFIX</li>
   * <li>Shared variables defined in JOB_SHARED_ENVS</li>
   * </ul>
   */
  @Override
  public Map<String, String> getJobDefaultEnvMap() {
    final Map<String, String> jobPrefixedEnvMap = getAllEnvKeys.get().stream()
        .filter(key -> key.startsWith(EnvVar.JOB_DEFAULT_ENV_.name()))
        .collect(Collectors.toMap(key -> key.replace(EnvVar.JOB_DEFAULT_ENV_.name(), ""), getEnv));
    // This method assumes that these shared env variables are not critical to the execution
    // of the jobs, and only serve as metadata. So any exception is swallowed and default to
    // an empty string. Change this logic if this assumption no longer holds.
    final Map<String, String> jobSharedEnvMap = JOB_SHARED_ENVS.entrySet().stream().collect(Collectors.toMap(
        Entry::getKey,
        entry -> Exceptions.swallowWithDefault(() -> Objects.requireNonNullElse(entry.getValue().apply(this), ""), "")));
    return MoreMaps.merge(jobPrefixedEnvMap, jobSharedEnvMap);
  }

  @Override
  public LogConfigs getLogConfigs() {
    return new LogConfigs(getLogConfiguration());
  }

  // Helpers
  public String getEnvOrDefault(final EnvVar envVar, final String defaultValue) {
    return getEnvOrDefault(envVar.name(), defaultValue, Function.identity(), false);
  }

  public boolean getEnvOrDefault(final EnvVar key, final boolean defaultValue) {
    return getEnvOrDefault(key, defaultValue, Boolean::parseBoolean);
  }

  private <T> T getEnvOrDefault(final String key, final T defaultValue, final Function<String, T> parser) {
    return getEnvOrDefault(key, defaultValue, parser, false);
  }

  private <T> T getEnvOrDefault(final EnvVar envVar, final T defaultValue, final Function<String, T> parser) {
    return getEnvOrDefault(envVar.name(), defaultValue, parser, false);
  }

  /**
   * Get env variable or default value.
   *
   * @param key of env variable
   * @param defaultValue to use if env variable is not present
   * @param parser function to parse env variable to desired type
   * @param isSecret is the env variable a secret
   * @param <T> type of env env variable
   * @return env variable
   */
  public <T> T getEnvOrDefault(final String key, final T defaultValue, final Function<String, T> parser, final boolean isSecret) {
    final String value = getEnv.apply(key);
    if (value != null && !value.isEmpty()) {
      return parser.apply(value);
    } else {
      LOGGER.info("Using default value for environment variable {}: '{}'", key, isSecret ? "*****" : defaultValue);
      return defaultValue;
    }
  }

  /**
   * Get env variable as string.
   *
   * @param name of env variable
   * @return value of env variable
   */
  public String getEnv(final String name) {
    return getEnv.apply(name);
  }

  public String getEnv(final EnvVar envVar) {
    return getEnv(envVar.name());
  }

  /**
   * Get env variable or throw if null.
   *
   * @param name of env variable
   * @return value of env variable
   */
  public String getEnsureEnv(final String name) {
    final String value = getEnv(name);
    Preconditions.checkArgument(value != null, "'%s' environment variable cannot be null", name);

    return value;
  }

  public String getEnsureEnv(final EnvVar envVar) {
    return getEnsureEnv(envVar.name());
  }

  private Path getPath(final String name) {
    final String value = getEnv.apply(name);
    if (value == null) {
      throw new IllegalArgumentException("Env variable not defined: " + name);
    }
    return Path.of(value);
  }

  private Path getPath(final EnvVar envVar) {
    return getPath(envVar.name());
  }

  private DeploymentMode getDeploymentMode() {
    return getEnvOrDefault(EnvVar.DEPLOYMENT_MODE.name(), DeploymentMode.OSS, s -> {
      try {
        return DeploymentMode.valueOf(s);
      } catch (final IllegalArgumentException e) {
        LOGGER.info(s + " not recognized, defaulting to " + DeploymentMode.OSS);
        return DeploymentMode.OSS;
      }
    });
  }

}
