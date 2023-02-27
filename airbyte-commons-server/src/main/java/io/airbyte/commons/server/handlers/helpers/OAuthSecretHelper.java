/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.validation.json.JsonValidationException;
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
  public static JsonNode setSecretsInConnectionConfiguration(final StandardSourceDefinition sourceDefinition,
                                                             final JsonNode hydratedSecret,
                                                             final JsonNode connectionConfiguration)
      throws JsonValidationException {
    final ConnectorSpecification spec = sourceDefinition.getSpec();

    // Get the paths from advancedAuth that we need
    final ObjectNode newConnectionConfiguration = connectionConfiguration.deepCopy();
    if (spec.getAdvancedAuth() != null) {
      final Map<String, List<String>> oAuthPaths = getOAuthConfigPaths(spec);
      for (final Entry<String, List<String>> entry : oAuthPaths.entrySet()) {
        // Key where we need to stuff things
        final String key = entry.getKey();
        final List<String> jsonPathList = entry.getValue();

        Jsons.replaceNestedValue(newConnectionConfiguration, jsonPathList, hydratedSecret.get(key));
      }
      return newConnectionConfiguration;
    } else {
      // Just merge, complete_oauth handled setting rootNode for us.
      return Jsons.mergeNodes(connectionConfiguration, hydratedSecret);
    }
  }

  /**
   * From advanced_auth gets the data we need to put into the connectionConfiguration which we get
   * from both complete_oauth and the actor_oauth_params Returns a map of the property_name:
   * path_in_connector_config i.e. { client_id: ['credentials', 'client_id']}
   */
  @VisibleForTesting
  public static Map<String, List<String>> getOAuthConfigPaths(final ConnectorSpecification connectorSpecification) throws JsonValidationException {
    if (connectorSpecification.getAdvancedAuth() != null) {
      final JsonNode completeOAuthOutputSpecification =
          connectorSpecification.getAdvancedAuth().getOauthConfigSpecification().getCompleteOauthOutputSpecification();
      final JsonNode completeOAuthServerOutputSpecification =
          connectorSpecification.getAdvancedAuth().getOauthConfigSpecification().getCompleteOauthServerOutputSpecification();

      // Merge all the mappings into one map
      Map<String, List<String>> result = new HashMap<>(buildKeyToPathInConnectorConfigMap(completeOAuthOutputSpecification));
      result.putAll(buildKeyToPathInConnectorConfigMap(completeOAuthServerOutputSpecification));
      return result;
    } else {
      throw new JsonValidationException(
          String.format("Error parsing advancedAuth - see [%s]", connectorSpecification.getDocumentationUrl()));
    }
  }

  /**
   * Given advanced auth specifications, extract a map of important keys to their path in
   * connectorConfiguration.
   */
  @VisibleForTesting
  public static Map<String, List<String>> buildKeyToPathInConnectorConfigMap(JsonNode specification) {
    final Map<String, List<String>> result = new HashMap<>();
    Iterator<Entry<String, JsonNode>> it = specification.get("properties").fields();
    while (it.hasNext()) {
      Entry<String, JsonNode> node = it.next();
      JsonNode pathInConnectorConfigNode = node.getValue().at("/path_in_connector_config");
      List<String> pathList = new ArrayList<String>();
      if (pathInConnectorConfigNode.isArray()) {
        for (final JsonNode pathNode : pathInConnectorConfigNode) {
          // pathList.isValueNode() should == true for all of these, so we don't need to check it here.
          pathList.add(pathNode.textValue());
        }
      }
      result.put(node.getKey(), pathList);
    }
    return result;
  }

}
