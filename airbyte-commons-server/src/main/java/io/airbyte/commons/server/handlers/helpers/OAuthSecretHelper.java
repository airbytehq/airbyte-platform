/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.json.Jsons;
import io.airbyte.persistence.job.factory.OAuthConfigSupplier;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.validation.json.JsonValidationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Holds helpers to handle OAuth secrets.
 */
public class OAuthSecretHelper {

  /**
   * Should mimic frontend in using connector specification to figure out what auth properties are
   * necessary for source creation and where to put them.
   */
  public static JsonNode setSecretsInConnectionConfiguration(final ConnectorSpecification spec,
                                                             final JsonNode hydratedSecret,
                                                             final JsonNode connectionConfiguration)
      throws JsonValidationException {

    // Get the paths from advancedAuth that we need
    final ObjectNode newConnectionConfiguration = connectionConfiguration.deepCopy();
    if (OAuthConfigSupplier.hasOAuthConfigSpecification(spec)) {
      final Map<String, List<String>> oAuthPaths = getOAuthConfigPaths(spec);
      for (final Entry<String, List<String>> entry : oAuthPaths.entrySet()) {
        // Key where we need to stuff things
        final String key = entry.getKey();
        final List<String> jsonPathList = entry.getValue();

        Jsons.setNestedValue(newConnectionConfiguration, jsonPathList, hydratedSecret.get(key));
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
    if (OAuthConfigSupplier.hasOAuthConfigSpecification(connectorSpecification)) {
      final JsonNode completeOAuthOutputSpecification =
          connectorSpecification.getAdvancedAuth().getOauthConfigSpecification().getCompleteOauthOutputSpecification();
      final JsonNode completeOAuthServerOutputSpecification =
          connectorSpecification.getAdvancedAuth().getOauthConfigSpecification().getCompleteOauthServerOutputSpecification();

      // Merge all the mappings into one map
      Map<String, List<String>> result = new HashMap<>(OAuthPathExtractor.extractOauthConfigurationPaths(completeOAuthOutputSpecification));
      result.putAll(OAuthPathExtractor.extractOauthConfigurationPaths(completeOAuthServerOutputSpecification));
      return result;
    } else {
      throw new JsonValidationException(
          String.format("Error parsing advancedAuth - see [%s]", connectorSpecification.getDocumentationUrl()));
    }
  }

}
