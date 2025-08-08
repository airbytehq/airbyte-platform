/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import com.google.common.base.Preconditions
import com.google.common.base.Splitter
import com.google.common.base.Strings
import io.airbyte.commons.envvar.EnvVar
import io.airbyte.commons.version.AirbyteVersion
import io.airbyte.config.TolerationPOJO.Companion.getJobKubeTolerations
import io.github.oshai.kotlinlogging.KotlinLogging
import java.nio.file.Path
import java.util.Arrays
import java.util.Optional
import java.util.function.Function
import java.util.function.Supplier
import java.util.stream.Collectors

/**
 * Configs from environment variables.
 *
 * Constructs [EnvConfigs] from a provided map. This can be used for testing or getting
 * variables from a non-envvar source.
 */
class EnvConfigs
  @JvmOverloads
  constructor(
    envMap: Map<String?, String?> = System.getenv(),
  ) : Configs {
    private val getEnv = Function { key: String? -> envMap[key] }
    private val getAllEnvKeys = Supplier { envMap.keys }

    // CORE
    // General
    override fun getAirbyteRole(): String? = getEnv(EnvVar.AIRBYTE_ROLE)

    override fun getAirbyteEdition(): String? = getEnv(EnvVar.AIRBYTE_EDITION)

    override fun getAirbyteVersion(): AirbyteVersion = AirbyteVersion(getEnsureEnv(EnvVar.AIRBYTE_VERSION))

    override fun getAirbyteVersionOrWarning(): String = Optional.ofNullable(getEnv(EnvVar.AIRBYTE_VERSION)).orElse("version not set")

    override fun getWorkspaceRoot(): Path = getPath(EnvVar.WORKSPACE_ROOT)

    // Database
    override fun getDatabaseUser(): String = getEnsureEnv(EnvVar.DATABASE_USER)

    override fun getDatabasePassword(): String = getEnsureEnv(EnvVar.DATABASE_PASSWORD)

    override fun getDatabaseUrl(): String = getEnsureEnv(EnvVar.DATABASE_URL)

    override fun getJobKubeTolerations(): List<TolerationPOJO> {
      val tolerationsStr = getEnvOrDefault(EnvVar.JOB_KUBE_TOLERATIONS, "")
      return getJobKubeTolerations(tolerationsStr)
    }

    /**
     * Returns a map of node selectors for any job type. Used as a default if a particular job type does
     * not define its own node selector environment variable.
     *
     * @return map containing kv pairs of node selectors, or empty optional if none present.
     */
    override fun getJobKubeNodeSelectors(): Map<String, String>? = splitKVPairsFromEnvString(getEnvOrDefault(EnvVar.JOB_KUBE_NODE_SELECTORS, ""))

    override fun getIsolatedJobKubeNodeSelectors(): Map<String, String> =
      splitKVPairsFromEnvString(getEnvOrDefault(EnvVar.JOB_ISOLATED_KUBE_NODE_SELECTORS, ""))!!

    override fun getUseCustomKubeNodeSelector(): Boolean = getEnvOrDefault(EnvVar.USE_CUSTOM_NODE_SELECTOR, false)

    /**
     * Returns a map of annotations from its own environment variable. The value of the env is a string
     * that represents one or more annotations. Each kv-pair is separated by a `,`
     *
     *
     * For example:- The following represents two annotations
     *
     *
     * airbyte=server,type=preemptive
     *
     * @return map containing kv pairs of annotations
     */
    override fun getJobKubeAnnotations(): Map<String, String> = splitKVPairsFromEnvString(getEnvOrDefault(EnvVar.JOB_KUBE_ANNOTATIONS, ""))!!

    override fun getJobKubeLabels(): Map<String, String> = splitKVPairsFromEnvString(getEnvOrDefault(EnvVar.JOB_KUBE_LABELS, ""))!!

    /**
     * Splits key value pairs from the input string into a map. Each kv-pair is separated by a ','. The
     * key and the value are separated by '='.
     *
     *
     * For example:- The following represents two map entries
     *
     * key1=value1,key2=value2
     *
     * @param input string
     * @return map containing kv pairs
     */
    fun splitKVPairsFromEnvString(input: String?): Map<String, String>? {
      var input = input
      if (input == null) {
        input = ""
      }
      val map =
        Splitter
          .on(",")
          .splitToStream(input)
          .filter { s: String -> !Strings.isNullOrEmpty(s) && s.contains("=") }
          .map { s: String -> s.split("=".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray() }
          .collect(
            Collectors.toMap(
              Function { s: Array<String> -> s[0].trim { it <= ' ' } },
              Function { s: Array<String> -> s[1].trim { it <= ' ' } },
            ),
          )
      return if (map.isEmpty()) null else map
    }

    override fun getJobKubeMainContainerImagePullPolicy(): String =
      getEnvOrDefault(EnvVar.JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_POLICY, DEFAULT_JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_POLICY)

    /**
     * Returns the name of the secret to be used when pulling down docker images for jobs. Automatically
     * injected in the KubePodProcess class and used in the job pod templates.
     *
     *
     * Can provide multiple strings seperated by comma(, ) to indicate pulling from different
     * repositories. The empty string is a no-op value.
     */
    override fun getJobKubeMainContainerImagePullSecrets(): List<String> {
      val secrets = getEnvOrDefault(EnvVar.JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_SECRET, "")
      return Arrays.stream(secrets.split(",".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()).collect(Collectors.toList())
    }

    // Helpers
    private fun getEnvOrDefault(
      envVar: EnvVar,
      defaultValue: String,
    ): String = getEnvOrDefault(envVar.name, defaultValue, Function.identity(), false)

    private fun getEnvOrDefault(
      key: EnvVar,
      defaultValue: Boolean,
    ): Boolean = getEnvOrDefault(key, defaultValue) { s: String -> s.toBoolean() }

    private fun <T> getEnvOrDefault(
      envVar: EnvVar,
      defaultValue: T,
      parser: Function<String, T>,
    ): T = getEnvOrDefault(envVar.name, defaultValue, parser, false)

    /**
     * Get env variable or default value.
     *
     * @param key of env variable
     * @param defaultValue to use if env variable is not present
     * @param parser function to parse env variable to desired type
     * @param isSecret is the env variable a secret
     * @param <T> type of env env variable
     * @return env variable
     </T> */
    private fun <T> getEnvOrDefault(
      key: String,
      defaultValue: T,
      parser: Function<String, T>,
      isSecret: Boolean,
    ): T {
      val value = getEnv.apply(key)
      if (value != null && !value.isEmpty()) {
        return parser.apply(value)
      } else {
        log.info { "Using default value for environment variable $key: ${if (isSecret) "*****" else defaultValue}" }
        return defaultValue
      }
    }

    /**
     * Get env variable as string.
     *
     * @param name of env variable
     * @return value of env variable
     */
    private fun getEnv(name: String): String? = getEnv.apply(name)

    private fun getEnv(envVar: EnvVar): String? = getEnv(envVar.name)

    /**
     * Get env variable or throw if null.
     *
     * @param name of env variable
     * @return value of env variable
     */
    private fun getEnsureEnv(name: String): String {
      val value = getEnv(name)
      Preconditions.checkArgument(value != null, "'%s' environment variable cannot be null", name)

      return value!!
    }

    private fun getEnsureEnv(envVar: EnvVar): String = getEnsureEnv(envVar.name)

    private fun getPath(name: String): Path {
      val value = getEnv.apply(name)
      requireNotNull(value) { "Env variable not defined: $name" }
      return Path.of(value)
    }

    private fun getPath(envVar: EnvVar): Path = getPath(envVar.name)

    companion object {
      private val log = KotlinLogging.logger {}

      // job-type-specific overrides
      private const val DEFAULT_JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_POLICY = "IfNotPresent"
    }
  }
