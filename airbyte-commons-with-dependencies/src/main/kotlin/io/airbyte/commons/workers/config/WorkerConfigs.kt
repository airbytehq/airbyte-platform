/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.workers.config

import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.config.Configs
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.ResourceRequirementsType
import io.airbyte.config.TolerationPOJO
import io.airbyte.config.provider.ResourceRequirementsProvider
import io.airbyte.config.provider.ResourceRequirementsProvider.Companion.DEFAULT_VARIANT
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.EnumMap
import java.util.regex.Pattern

/** Configuration object for a worker. */
data class WorkerConfigs(
  val resourceRequirements: ResourceRequirements,
  val workerKubeTolerations: List<TolerationPOJO>,
  val workerKubeNodeSelectors: Map<String, String>?,
  val workerIsolatedKubeNodeSelectors: Map<String, String>?,
  val workerKubeAnnotations: Map<String, String>,
  val workerKubeLabels: Map<String, String>,
  val jobImagePullSecrets: List<String>,
  val jobImagePullPolicy: String,
) {
  /**
   * Constructs a job-type-agnostic WorkerConfigs.
   */
  @InternalForTesting
  internal constructor(configs: Configs) : this(
    resourceRequirements = ResourceRequirements(),
    workerKubeTolerations = configs.getJobKubeTolerations(),
    workerKubeNodeSelectors = configs.getJobKubeNodeSelectors(),
    workerIsolatedKubeNodeSelectors = configs.getUseCustomKubeNodeSelector().takeIf { it }?.let { configs.getIsolatedJobKubeNodeSelectors() },
    workerKubeAnnotations = configs.getJobKubeAnnotations(),
    workerKubeLabels = configs.getJobKubeLabels(),
    jobImagePullSecrets = configs.getJobKubeMainContainerImagePullSecrets(),
    jobImagePullPolicy = configs.getJobKubeMainContainerImagePullPolicy(),
  )
}

/**
 * Provide WorkerConfigs.
 *
 * This provider gathers the configuration from the application.yml that are nested under the `airbyte.worker.kube-job-configs` key.
 */
@Singleton
class WorkerConfigsProvider(
  kubeResourceConfigs: List<KubeResourceConfig>,
  private val workerConfigsDefaults: WorkerConfigsDefaults,
) : ResourceRequirementsProvider {
  private val kubeResourceConfigs: Map<String, Map<ResourceType, Map<ResourceSubType, KubeResourceConfig>>>

  init {
    val resourceConfigs = mutableMapOf<String, MutableMap<ResourceType, MutableMap<ResourceSubType, KubeResourceConfig>>>()
    kubeResourceConfigs.forEach { config ->
      val key: KubeResourceKey = parseKubeResourceKey(config.name) ?: throw IllegalArgumentException("Unsupported config name ${config.name}.")

      val typeMap: MutableMap<ResourceType, MutableMap<ResourceSubType, KubeResourceConfig>> =
        resourceConfigs.computeIfAbsent(key.variant) { _ -> EnumMap(ResourceType::class.java) }

      val subTypeMap: MutableMap<ResourceSubType, KubeResourceConfig> =
        typeMap.computeIfAbsent(key.type) { _ -> EnumMap(ResourceSubType::class.java) }

      subTypeMap[key.subType] = config
    }

    this.kubeResourceConfigs = resourceConfigs.toMap()
  }

  /**
   * Get the WorkerConfigs associated to a given task.
   *
   * @param name of the Task.
   * @return the WorkerConfig.
   */
  fun getConfig(name: ResourceType): WorkerConfigs =
    getConfig(KubeResourceKey(variant = DEFAULT_VARIANT, type = name, subType = ResourceSubType.DEFAULT))

  private fun getConfig(key: KubeResourceKey): WorkerConfigs {
    val kubeResourceConfig = getKubeResourceConfig(key) ?: throw NoSuchElementException("Unable to find config: $key")

    val isolatedNodeSelectors: Map<String, String> = workerConfigsDefaults.isolatedNodeSelectors.toKeyValues()
    if (workerConfigsDefaults.useCustomNodeSelector && isolatedNodeSelectors.isEmpty()) {
      throw IllegalStateException("Isolated Node selectors is empty while useCustomNodeSelector is set to true.")
    }

    // if annotations are not defined for this specific resource, fallback to the default resource annotations
    val annotations: Map<String, String> =
      if (kubeResourceConfig.annotations.isNullOrEmpty()) {
        workerConfigsDefaults.defaultKubeResourceConfig.annotations.toKeyValues()
      } else {
        kubeResourceConfig.annotations.toKeyValues()
      }

    return WorkerConfigs(
      resourceRequirements = getResourceRequirementsFrom(kubeResourceConfig, workerConfigsDefaults.defaultKubeResourceConfig),
      workerKubeTolerations = TolerationPOJO.getJobKubeTolerations(workerConfigsDefaults.jobKubeTolerations),
      workerKubeNodeSelectors = kubeResourceConfig.nodeSelectors.toKeyValues(),
      workerIsolatedKubeNodeSelectors = if (workerConfigsDefaults.useCustomNodeSelector) isolatedNodeSelectors else null,
      workerKubeAnnotations = annotations,
      workerKubeLabels = kubeResourceConfig.labels.toKeyValues(),
      jobImagePullSecrets = workerConfigsDefaults.mainContainerImagePullSecret,
      jobImagePullPolicy = workerConfigsDefaults.mainContainerImagePullPolicy,
    )
  }

  /**
   * Look up resource configs given a key.
   *
   * We are storing configs in a tree like structure. Look up should be handled as such.
   * Keeping in mind that we have defaults we want to fallback to, we should perform a complete scan of the
   * configs until we find a match to make sure we do not overlook a match.
   */
  private fun getKubeResourceConfig(key: KubeResourceKey): KubeResourceConfig? {
    // Look up by actual variant
    getKubeResourceConfigByType(kubeResourceConfigs[key.variant], key)?.let { return it }

    // no match with exact variant found, try again with the default.
    return getKubeResourceConfigByType(kubeResourceConfigs[DEFAULT_VARIANT], key)
  }

  override fun getResourceRequirements(
    type: ResourceRequirementsType,
    subType: String?,
  ): ResourceRequirements = getResourceRequirements(type, subType, DEFAULT_VARIANT)

  override fun getResourceRequirements(
    type: ResourceRequirementsType,
    subType: String?,
    variant: String,
  ): ResourceRequirements {
    val typedSubType: ResourceSubType = subType?.let { ResourceSubType.fromValue(it) } ?: ResourceSubType.DEFAULT

    val key =
      KubeResourceKey(
        variant = variant,
        type = RSS_REQ_MAPPING[type] ?: ResourceType.DEFAULT,
        subType = typedSubType,
      )

    return getConfig(key).resourceRequirements
  }

  companion object {
    /**
     * Map used for converting ResourceRequirementsType into ResourceType.
     *
     * A mapping is required because we may some slight naming misalignment.
     * Eventually, we should only have one enum which would remove the need for this map entirely.
     */
    val RSS_REQ_MAPPING =
      mapOf(
        ResourceRequirementsType.DESTINATION to ResourceType.DESTINATION,
        ResourceRequirementsType.ORCHESTRATOR to ResourceType.ORCHESTRATOR,
        ResourceRequirementsType.SOURCE to ResourceType.SOURCE,
      )
  }
}

/** Set of known resource types. */
enum class ResourceType {
  // global default
  DEFAULT,

  // command specific resources
  CHECK,
  DISCOVER,
  REPLICATION,
  SPEC,

  // sync related resources
  DESTINATION,
  ORCHESTRATOR,
  SOURCE,
  ;

  override fun toString(): String = this.name.lowercase()

  companion object {
    fun fromValue(value: String): ResourceType? = runCatching { valueOf(value.uppercase()) }.getOrNull()
  }
}

/** Set of known resource sub types. */
internal enum class ResourceSubType {
  API,
  CUSTOM,
  DATABASE,
  DEFAULT,
  FILE,
  ;

  override fun toString(): String = this.name.lowercase()

  companion object {
    fun fromValue(value: String): ResourceSubType? = runCatching { valueOf(value.uppercase()) }.getOrNull()
  }
}

internal data class KubeResourceKey(
  val variant: String,
  val type: ResourceType,
  val subType: ResourceSubType,
)

private val kubeResourceKeyPattern: Pattern =
  run {
    val types = ResourceType.entries.joinToString(separator = "|")
    val subTypes = ResourceSubType.entries.joinToString(separator = "|")
    "^((?<variant>[a-z0-9]+)-)?(?<type>$types)(-(?<subtype>$subTypes))?$".toPattern(Pattern.CASE_INSENSITIVE)
  }

/**
 * Parses a kubeResourceKey.
 *
 * The expected format is defined as follows `variant-type-subtype`. Type is mandatory, missing
 * variant will be replaced by DEFAULT_VARIANT, missing sub type will be replaced by
 * ResourceSubType.DEFAULT.
 *
 * In the variant name, we do not support uppercase. This is because micronaut normalizes uppercases
 * with dashes (CamelCase becomes camel-case) which is confusing because the variant name no longer
 * matches the config file.
 */
private fun parseKubeResourceKey(value: String): KubeResourceKey? {
  val matcher = kubeResourceKeyPattern.matcher(value.lowercase())
  if (!matcher.matches()) {
    return null
  }

  return KubeResourceKey(
    variant = matcher.group("variant") ?: DEFAULT_VARIANT,
    type = matcher.group("type")?.let { ResourceType.fromValue(it) } ?: throw IllegalArgumentException(),
    subType = matcher.group("subtype")?.let { ResourceSubType.fromValue(it) } ?: ResourceSubType.DEFAULT,
  )
}

@Singleton
data class WorkerConfigsDefaults(
  @Named("default") val defaultKubeResourceConfig: KubeResourceConfig,
  @Value("\${airbyte.worker.job.kube.tolerations}")val jobKubeTolerations: String,
  @Value("\${airbyte.worker.isolated.kube.node-selectors}") val isolatedNodeSelectors: String,
  @Value("\${airbyte.worker.isolated.kube.use-custom-node-selector}") val useCustomNodeSelector: Boolean,
  @Value("\${airbyte.worker.job.kube.main.container.image-pull-secret}")val mainContainerImagePullSecret: List<String>,
  @Value("\${airbyte.worker.job.kube.main.container.image-pull-policy}") val mainContainerImagePullPolicy: String,
)

private fun getKubeResourceConfigByType(
  configs: Map<ResourceType, Map<ResourceSubType, KubeResourceConfig>>?,
  key: KubeResourceKey,
): KubeResourceConfig? {
  if (configs == null) {
    return null
  }

  getKubeResourceConfigBySubType(configs[key.type], key)?.let { return it }

  return getKubeResourceConfigBySubType(configs[ResourceType.DEFAULT], key)
}

private fun getKubeResourceConfigBySubType(
  configBySubtype: Map<ResourceSubType, KubeResourceConfig>?,
  key: KubeResourceKey,
): KubeResourceConfig? {
  if (configBySubtype == null) {
    return null
  }

  return configBySubtype[key.subType] ?: configBySubtype[ResourceSubType.DEFAULT]
}

private fun String?.toKeyValues(): Map<String, String> {
  if (this.isNullOrBlank()) {
    return emptyMap()
  }

  return this
    .split(",")
    .filter { it.isNotEmpty() && it.contains("=") }
    .associate {
      val (key, value) = it.split("=", limit = 2)
      key.trim() to value.trim()
    }
}

private fun getResourceRequirementsFrom(
  cfg: KubeResourceConfig,
  default: KubeResourceConfig,
): ResourceRequirements =
  ResourceRequirements()
    .withCpuLimit(cfg.cpuLimit.ifBlankOrNull { default.cpuLimit })
    .withCpuRequest(cfg.cpuRequest.ifBlankOrNull { default.cpuRequest })
    .withMemoryLimit(cfg.memoryLimit.ifBlankOrNull { default.memoryLimit })
    .withMemoryRequest(cfg.memoryRequest.ifBlankOrNull { default.memoryRequest })
    .withEphemeralStorageLimit(cfg.ephemeralStorageLimit.ifBlankOrNull { default.ephemeralStorageLimit })
    .withEphemeralStorageRequest(cfg.ephemeralStorageRequest.ifBlankOrNull { default.ephemeralStorageRequest })

private fun String?.ifBlankOrNull(default: () -> String?): String? =
  if (this.isNullOrBlank()) {
    default()
  } else {
    this
  }
// private fun String?.ifEmpty(default: String) =
//  if (this.isNullOrBlank()) {
//    default
//  } else {
//    this
//  }
