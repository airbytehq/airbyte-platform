/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeType
import com.fasterxml.jackson.databind.node.ObjectNode
import com.google.common.base.Strings
import io.airbyte.commons.json.Jsons
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.function.Consumer

/**
 * OAuth params.
 */
object MoreOAuthParameters {
  private val log = KotlinLogging.logger {}
  const val SECRET_MASK: String = "******"

  /**
   * Flatten config.
   *
   * @param config to flatten
   * @return flattened config
   */
  @JvmStatic
  fun flattenOAuthConfig(config: JsonNode): JsonNode {
    if (config.nodeType == JsonNodeType.OBJECT) {
      return flattenOAuthConfig(Jsons.emptyObject() as ObjectNode, config as ObjectNode)
    } else {
      throw IllegalStateException("Config is not an Object config, unable to flatten")
    }
  }

  private fun flattenOAuthConfig(
    flatConfig: ObjectNode,
    configToFlatten: ObjectNode,
  ): ObjectNode {
    val keysToFlatten: MutableList<String> = ArrayList()
    for (key in Jsons.keys(configToFlatten)) {
      val currentNodeValue = configToFlatten[key]
      if (isSecretNode(currentNodeValue) && !flatConfig.has(key)) {
        // _secret keys are objects, but we want to preserve them.
        flatConfig.set<JsonNode>(key, currentNodeValue)
      } else if (currentNodeValue.nodeType == JsonNodeType.OBJECT) {
        keysToFlatten.add(key)
      } else if (!flatConfig.has(key)) {
        flatConfig.set<JsonNode>(key, currentNodeValue)
      } else {
        log.debug { "configToFlatten: $configToFlatten" }
        throw IllegalStateException(String.format("OAuth Config's key '%s' already exists", key))
      }
    }
    keysToFlatten.forEach(Consumer { key: String? -> flattenOAuthConfig(flatConfig, configToFlatten[key] as ObjectNode) })
    return flatConfig
  }

  private fun isSecretNode(node: JsonNode): Boolean {
    val secretNode = node["_secret"]
    return secretNode != null
  }

  /**
   * Merge JSON configs.
   *
   * @param mainConfig original config
   * @param fromConfig config with overwrites
   * @return merged config
   */
  @JvmStatic
  fun mergeJsons(
    mainConfig: ObjectNode,
    fromConfig: ObjectNode,
  ): JsonNode {
    for (key in Jsons.keys(fromConfig)) {
      // keys with _secret Jsons are objects, but we still want to merge those
      if (fromConfig[key].nodeType == JsonNodeType.OBJECT && !isSecretNode(fromConfig[key])) {
        // nested objects are merged rather than overwrite the contents of the equivalent object in config
        if (mainConfig[key] == null) {
          mergeJsons(mainConfig.putObject(key), fromConfig[key] as ObjectNode)
        } else if (mainConfig[key].nodeType == JsonNodeType.OBJECT) {
          mergeJsons(mainConfig[key] as ObjectNode, fromConfig[key] as ObjectNode)
        } else {
          throw IllegalStateException("Can't merge an object node into a non-object node!")
        }
      } else {
        if (!mainConfig.has(key) || isSecretMask(mainConfig[key].asText())) {
          log.debug { "injecting instance wide parameter $key into config" }
          mainConfig.set<JsonNode>(key, fromConfig[key])
        }
      }
    }
    return mainConfig
  }

  private fun isSecretMask(input: String): Boolean = Strings.isNullOrEmpty(input.replace("\\*".toRegex(), ""))
}
