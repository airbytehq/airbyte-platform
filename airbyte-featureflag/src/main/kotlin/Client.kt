/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.featureflag

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.launchdarkly.sdk.ContextKind
import com.launchdarkly.sdk.LDContext
import com.launchdarkly.sdk.server.LDClient
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Secondary
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.slf4j.LoggerFactory
import java.lang.Thread.MIN_PRIORITY
import java.nio.file.Path
import java.nio.file.StandardWatchEventKinds
import java.nio.file.WatchService
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.thread
import kotlin.concurrent.write
import kotlin.io.path.isRegularFile
import kotlin.io.path.notExists

/**
 * Feature-Flag Client interface.
 *
 * Note: Use the [TestClient] if needing to create a mock as Mockito does not currently
 * support mocking a sealed interface, however it does support mocking an implementation
 * of a sealed interface.
 */
sealed interface FeatureFlagClient {
  /**
   * Calculates the boolean value of the [flag] for the given [context].
   *
   * Returns the [flag] default value if the [flag] cannot be evaluated.
   */
  fun boolVariation(
    flag: Flag<Boolean>,
    context: Context,
  ): Boolean

  /**
   * Calculates the string value of the [flag] for the given [context].
   *
   * Returns the [flag] default value if no calculated value exists.
   */
  fun stringVariation(
    flag: Flag<String>,
    context: Context,
  ): String

  /**
   * Calculates the string value of the [flag] for the given [context].
   *
   * Returns the [flag] default value if no calculated value exists.
   */
  fun intVariation(
    flag: Flag<Int>,
    context: Context,
  ): Int
}

/** Config key used to determine which [FeatureFlagClient] to expose. */
internal const val CONFIG_FF_CLIENT = "airbyte.feature-flag.client"

/** If [CONFIG_FF_CLIENT] equals this value, return the [LaunchDarklyClient], otherwise the [ConfigFileClient]. */
internal const val CONFIG_FF_CLIENT_VAL_LAUNCHDARKLY = "launchdarkly"

/** Config key to provide the api-key as required by the [LaunchDarklyClient]. */
internal const val CONFIG_FF_APIKEY = "airbyte.feature-flag.api-key"

/** Config key to provide the location of the flags config file used by the [ConfigFileClient]. */
internal const val CONFIG_FF_PATH = "airbyte.feature-flag.path"

/**
 * Config file based feature-flag client.
 *
 * If no [config] is provided, will return the default state for each [Flag] requested.
 * Supports [EnvVar] flags as well.
 *
 * @param [config] optional location of the yaml config file that contains the feature-flag definitions.
 * If the [config] is provided, it will be watched for changes and the internal representation of the [config] will be updated to match.
 */
@Singleton
@Requires(property = CONFIG_FF_CLIENT, notEquals = CONFIG_FF_CLIENT_VAL_LAUNCHDARKLY)
class ConfigFileClient(
  @Property(name = CONFIG_FF_PATH) config: Path?,
) : FeatureFlagClient {
  /** [flags] holds the mappings of the flag-name to the flag properties */
  private var flags: Map<String, ConfigFileFlag> = mapOf()

  /** lock is used for ensuring access to the flags map is handled correctly when the map is being updated. */
  private val lock = ReentrantReadWriteLock()

  init {
    config?.also { path ->
      when {
        path.notExists() -> log.info("path $path does not exist, will return default flag values")
        !path.isRegularFile() -> log.info("path $path does not reference a file, will return default values")
        else -> {
          flags = readConfig(path)
          path.onChange {
            lock.write { flags = readConfig(config) }
          }
        }
      }
    }
  }

  override fun boolVariation(
    flag: Flag<Boolean>,
    context: Context,
  ): Boolean {
    if (flag is EnvVar) {
      return flag.enabled(context)
    }
    return lock.read {
      flags[flag.key]?.serve(context)?.let { it as? Boolean } ?: flag.default
    }
  }

  override fun stringVariation(
    flag: Flag<String>,
    context: Context,
  ): String {
    return flags[flag.key]?.serve(context)?.let { it as? String } ?: flag.default
  }

  override fun intVariation(
    flag: Flag<Int>,
    context: Context,
  ): Int {
    return flags[flag.key]?.serve(context)?.let { it as? Int } ?: flag.default
  }

  companion object {
    private val log = LoggerFactory.getLogger(ConfigFileClient::class.java)
  }
}

/**
 * LaunchDarkly based feature-flag client. Feature-flags are derived from an external source (the LDClient).
 * Also supports flags defined via environment-variables via the [EnvVar] class.
 *
 * @param [client] the Launch-Darkly client for interfacing with Launch-Darkly.
 */
@Singleton
@Requires(property = CONFIG_FF_CLIENT, value = CONFIG_FF_CLIENT_VAL_LAUNCHDARKLY)
class LaunchDarklyClient(private val client: LDClient) : FeatureFlagClient {
  override fun boolVariation(
    flag: Flag<Boolean>,
    context: Context,
  ): Boolean {
    return when (flag) {
      is EnvVar -> flag.enabled(context)
      else -> client.boolVariation(flag.key, context.toLDContext(), flag.default)
    }
  }

  override fun stringVariation(
    flag: Flag<String>,
    context: Context,
  ): String {
    return client.stringVariation(flag.key, context.toLDContext(), flag.default)
  }

  override fun intVariation(
    flag: Flag<Int>,
    context: Context,
  ): Int {
    return client.intVariation(flag.key, context.toLDContext(), flag.default)
  }
}

/**
 * Test feature-flag client. Only to be used in test scenarios.
 *
 * This class can be mocked and can also be used with Micronaut's @MockBean annotation to replace the [FeatureFlagClient] dependency.
 *
 * To use with the @MockBean annotation define the following method within your @MicronautTest annotated test class:
 * ```java
 * @MockBean(FeatureFlagClient.class)
 * TestClient featureFlagClient() {
 *   return mock(TestClient.class);
 * }
 * ```
 *
 * All [Flag] instances will use the provided [values] map as their source of truth, including [EnvVar] flags.
 *
 * @param [values] is a map of [Flag.key] to its status.
 */
@Secondary
open class TestClient(val values: Map<String, Any>) : FeatureFlagClient {
  @Inject
  constructor() : this(mapOf())

  override fun boolVariation(
    flag: Flag<Boolean>,
    context: Context,
  ): Boolean {
    return when (flag) {
      is EnvVar -> {
        // convert to a EnvVar flag with a custom fetcher that uses the [values] of this Test class
        // instead of fetching from the environment variables
        EnvVar(envVar = flag.key, default = flag.default, attrs = flag.attrs).apply {
          fetcher = { values[flag.key]?.toString() ?: flag.default.toString() }
        }.enabled(context)
      }

      else -> values[flag.key]?.let { it as? Boolean } ?: flag.default
    }
  }

  override fun stringVariation(
    flag: Flag<String>,
    context: Context,
  ): String {
    return values[flag.key]?.let { it as? String } ?: flag.default
  }

  override fun intVariation(
    flag: Flag<Int>,
    context: Context,
  ): Int {
    return values[flag.key]?.let { it as? Int } ?: flag.default
  }
}

/**
 * Data wrapper around OSS feature-flag configuration file.
 *
 * The file has the format of:
 * flags:
 *  - name: feature-one
 *    enabled: true
 *  - name: feature-two
 *    enabled: false
 */
private data class ConfigFileFlags(val flags: List<ConfigFileFlag>)

/**
 * Data wrapper around an individual flag read from the configuration file.
 */
private data class ConfigFileFlag(
  val name: String,
  val serve: Any,
  val context: List<ConfigFileFlagContext>? = null,
) {
  /**
   * Map of context kind to list of contexts.
   *
   * Example:
   * {
   *   "workspace": [
   *     { "serve": "true", include: ["000000-...", "111111-..."] }
   *   ]
   * }
   */
  private val contextsByType: Map<String, List<ConfigFileFlagContext>> =
    context?.groupBy { it.type } ?: mapOf()

  /**
   * Serve checks the [ctx] to see if it matches any contexts that may have
   * been defined in the flags.yml file.  If it does match, the serve value
   * from the matching context section will be returned.  If it does not
   * match, the non-context serve value will be returned.
   */
  fun serve(ctx: Context): Any {
    if (contextsByType.isEmpty()) {
      return serve
    }
    return when (ctx) {
      is Multi ->
        ctx.contexts.map { serve(it) }
          .find { it != serve } ?: serve
      else ->
        contextsByType[ctx.kind]
          ?.findLast { it.include.contains(ctx.key) }
          ?.serve
          ?: serve
    }
  }
}

private data class ConfigFileFlagContext(
  val type: String,
  val serve: Any,
  val include: List<String> = listOf(),
)

/** The yaml mapper is used for reading the feature-flag configuration file. */
private val yamlMapper = ObjectMapper(YAMLFactory()).registerKotlinModule()

/**
 * Reads a yaml configuration file, converting it into a map of flag name to flag configuration.
 *
 * @param [path] to yaml config file
 * @return map of feature-flag name to feature-flag config
 */
private fun readConfig(path: Path): Map<String, ConfigFileFlag> = yamlMapper.readValue<ConfigFileFlags>(path.toFile()).flags.associateBy { it.name }

/**
 * Monitors a [Path] for changes, calling [block] when a change is detected.
 *
 * @receiver Path
 * @param [block] function called anytime a change is detected on this [Path]
 */
private fun Path.onChange(block: () -> Unit) {
  val watcher: WatchService = fileSystem.newWatchService()
  // The watcher service requires a directory to be registered and not an individual file. This Path is an individual file,
  // hence the `parent` reference to register the parent of this file (which is the directory that contains this file).
  // As all files within this directory could send events, any file that doesn't match this Path will need to be filtered out.
  parent.register(watcher, StandardWatchEventKinds.ENTRY_MODIFY, StandardWatchEventKinds.ENTRY_CREATE)

  thread(isDaemon = true, name = "feature-flag-watcher", priority = MIN_PRIORITY) {
    val key = watcher.take()
    // The context on the poll-events for ENTRY_MODIFY and ENTRY_CREATE events should return a Path,
    // however officially `Returns: the event context; may be null`, so there is a null check here
    key.pollEvents().mapNotNull { it.context() as? Path }
      // As events are generated at the directory level and not the file level, any files that do not match the specific file
      // this Path represents must be filtered out.
      // E.g.
      // If this path is "/tmp/dir/flags.yml",
      // the directory registered with the WatchService was "/tmp/dir",
      // and the event's path would be "flags.yml".
      //
      // This filter verifies that "/tmp/dir/flags.yml" ends with "flags.yml" before calling the block method.
      .filter { this.endsWith(it) }
      .forEach { _ -> block() }

    key.reset()
  }
}

/**
 * LaunchDarkly v6 version
 */
private fun Context.toLDContext(): LDContext {
  if (this is Multi) {
    val builder = LDContext.multiBuilder()
    contexts.forEach { builder.add(it.toLDContext()) }
    return builder.build()
  }

  val builder = LDContext.builder(ContextKind.of(kind), key)
  if (key == ANONYMOUS.toString()) {
    builder.anonymous(true)
  }

  return builder.build()
}
