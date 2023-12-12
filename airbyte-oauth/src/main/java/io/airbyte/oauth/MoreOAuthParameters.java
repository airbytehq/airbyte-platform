/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth;

import static com.fasterxml.jackson.databind.node.JsonNodeType.OBJECT;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;
import io.airbyte.commons.json.Jsons;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OAuth params.
 */
public class MoreOAuthParameters {

  private static final Logger LOGGER = LoggerFactory.getLogger(Jsons.class);
  public static final String SECRET_MASK = "******";

  /**
   * Flatten config.
   *
   * @param config to flatten
   * @return flattened config
   */
  public static JsonNode flattenOAuthConfig(final JsonNode config) {
    if (config.getNodeType() == OBJECT) {
      return flattenOAuthConfig((ObjectNode) Jsons.emptyObject(), (ObjectNode) config);
    } else {
      throw new IllegalStateException("Config is not an Object config, unable to flatten");
    }
  }

  private static ObjectNode flattenOAuthConfig(final ObjectNode flatConfig, final ObjectNode configToFlatten) {
    final List<String> keysToFlatten = new ArrayList<>();
    for (final String key : Jsons.keys(configToFlatten)) {
      final JsonNode currentNodeValue = configToFlatten.get(key);
      if (isSecretNode(currentNodeValue) && !flatConfig.has(key)) {
        // _secret keys are objects but we want to preserve them.
        flatConfig.set(key, currentNodeValue);
      } else if (currentNodeValue.getNodeType() == OBJECT) {
        keysToFlatten.add(key);
      } else if (!flatConfig.has(key)) {
        flatConfig.set(key, currentNodeValue);
      } else {
        LOGGER.debug("configToFlatten: {}", configToFlatten);
        throw new IllegalStateException(String.format("OAuth Config's key '%s' already exists", key));
      }
    }
    keysToFlatten.forEach(key -> flattenOAuthConfig(flatConfig, (ObjectNode) configToFlatten.get(key)));
    return flatConfig;
  }

  private static boolean isSecretNode(final JsonNode node) {
    final JsonNode secretNode = node.get("_secret");
    return secretNode != null;
  }

  /**
   * Merge JSON configs.
   *
   * @param mainConfig original config
   * @param fromConfig config with overwrites
   * @return merged config
   */
  public static JsonNode mergeJsons(final ObjectNode mainConfig, final ObjectNode fromConfig) {
    for (final String key : Jsons.keys(fromConfig)) {
      // keys with _secret Jsons are objects but we still want to merge those
      if (fromConfig.get(key).getNodeType() == OBJECT && !isSecretNode(fromConfig.get(key))) {
        // nested objects are merged rather than overwrite the contents of the equivalent object in config
        if (mainConfig.get(key) == null) {
          mergeJsons(mainConfig.putObject(key), (ObjectNode) fromConfig.get(key));
        } else if (mainConfig.get(key).getNodeType() == OBJECT) {
          mergeJsons((ObjectNode) mainConfig.get(key), (ObjectNode) fromConfig.get(key));
        } else {
          throw new IllegalStateException("Can't merge an object node into a non-object node!");
        }
      } else {
        if (!mainConfig.has(key) || isSecretMask(mainConfig.get(key).asText())) {
          LOGGER.debug(String.format("injecting instance wide parameter %s into config", key));
          mainConfig.set(key, fromConfig.get(key));
        }
      }
    }
    return mainConfig;
  }

  private static boolean isSecretMask(final String input) {
    return Strings.isNullOrEmpty(input.replaceAll("\\*", ""));
  }

}
