/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class OAuthSecretHelper {

  /**
   * Should mimic frontend in using connector specification to figure out what auth properties are
   * necessary for source creation and where to put them.
   */
  @VisibleForTesting
  public static JsonNode setSecretsInConnectionConfiguration(StandardSourceDefinition sourceDefinition,
                                                             final Map<String, Object> hydratedSecret,
                                                             final JsonNode connectionConfiguration)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final ConnectorSpecification spec = sourceDefinition.getSpec();

    // Get the paths from advancedAuth that we need
    final Map<String, List<String>> oAuthPaths = OAuthSecretHelper.getOAuthConfigPaths(spec);

    final ObjectNode newConnectionConfiguration = connectionConfiguration.deepCopy();
    for (final Entry<String, List<String>> entry : oAuthPaths.entrySet()) {
      // Key where we need to stuff things
      final String key = entry.getKey();
      final List<String> jsonPathArray = entry.getValue();
      String jsonPointer = "/" + String.join("/", jsonPathArray);
      connectionConfiguration.withObject(jsonPointer).set(key, (JsonNode) hydratedSecret.get(key));
    }
    return newConnectionConfiguration;
  }

  /**
   * From advanced_auth gets the data we need to put into the connectionConfiguration which we get
   * from both complete_oauth and the actor_oauth_params Returns a map of the property_name:
   * path_in_connector_config i.e. { client_id: ['credentials', 'client_id']}
   */
  @VisibleForTesting
  public static Map<String, List<String>> getOAuthConfigPaths(final ConnectorSpecification connectorSpecification) {
    final JsonNode completeOAuthOutputSpecification =
        connectorSpecification.getAdvancedAuth().getOauthConfigSpecification().getCompleteOauthOutputSpecification();
    final JsonNode completeOAuthServerOutputSpecification =
        connectorSpecification.getAdvancedAuth().getOauthConfigSpecification().getCompleteOauthServerOutputSpecification();

    // Merge all the mappings into one map
    final Map<String, List<String>> result = new HashMap<>(OAuthSecretHelper.buildKeyToPathInConnectorConfigMap(completeOAuthOutputSpecification));
    result.putAll(OAuthSecretHelper.buildKeyToPathInConnectorConfigMap(completeOAuthServerOutputSpecification));

    return result;
  }

  @VisibleForTesting
  public static Map<String, List<String>> buildKeyToPathInConnectorConfigMap(JsonNode specification) {
    final Map<String, List<String>> result = new HashMap<>();
    Iterator<Entry<String, JsonNode>> it = specification.get("properties").fields();
    while (it.hasNext()) {
      Entry<String, JsonNode> node = it.next();
      System.out.println("node: " + node);
      JsonNode pathInConnectorConfigNode = node.getValue().at("/path_in_connector_config");
      System.out.println("path_in_connector_config node: " + pathInConnectorConfigNode);
      List<String> pathList = new ArrayList<String>();
      if (pathInConnectorConfigNode.isArray()) {
        for (final JsonNode pathNode : pathInConnectorConfigNode) {
          // pathList.isValueNode() should == true for all of these.
          System.out.println("pathNode textValue: " + pathNode.textValue());
          pathList.add(pathNode.textValue());
        }
      }
      result.put(node.getKey(), pathList);
    }
    return result;
  }

  public static Map<String, Object> hydrateOAuthResponseSecret(String secretId) {
    return Map.of("test", "test");
  }

}
