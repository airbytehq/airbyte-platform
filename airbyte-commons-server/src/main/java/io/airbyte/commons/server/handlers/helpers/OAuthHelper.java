/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.airbyte.api.model.generated.CompleteOAuthResponse;
import io.airbyte.commons.json.Jsons;
import io.airbyte.protocol.models.OAuthConfigSpecification;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Static helpers for Oauth-related reading and writing.
 */
public class OAuthHelper {

  private static final String PROPERTIES = "properties";
  private static final String PATH_IN_CONNECTOR_CONFIG = "path_in_connector_config";

  /**
   * Extract fields names from oauth spec.
   *
   * @param oauthSpec oauth spec
   * @return field names in the spec
   */
  public static Map<String, List<String>> extractOauthConfigurationPaths(final JsonNode oauthSpec) {

    if (oauthSpec != null && oauthSpec.has(PROPERTIES) && oauthSpec.get(PROPERTIES).isObject()) {
      final Map<String, List<String>> result = new HashMap<>();

      oauthSpec.get(PROPERTIES).fields().forEachRemaining(entry -> {
        final JsonNode value = entry.getValue();
        if (value.isObject() && value.has(PATH_IN_CONNECTOR_CONFIG) && value.get(PATH_IN_CONNECTOR_CONFIG).isArray()) {
          final List<String> path = new ArrayList<>();
          for (final JsonNode pathPart : value.get(PATH_IN_CONNECTOR_CONFIG)) {
            path.add(pathPart.textValue());
          }
          result.put(entry.getKey(), path);
        }
      });

      return result;
    } else {
      return new HashMap<>();
    }
  }

  /**
   * Map to the result of a completeOauth request to an API response.
   *
   * @param input input
   * @return complete oauth response
   */
  public static CompleteOAuthResponse mapToCompleteOAuthResponse(final Map<String, Object> input) {
    final CompleteOAuthResponse response = new CompleteOAuthResponse();
    response.setAuthPayload(new HashMap<>());

    if (input.containsKey("request_succeeded")) {
      response.setRequestSucceeded("true".equals(input.get("request_succeeded")));
    } else {
      response.setRequestSucceeded(true);
    }

    if (input.containsKey("request_error")) {
      response.setRequestError(input.get("request_error").toString());
    }

    input.forEach((k, v) -> {
      if (!"request_succeeded".equals(k) && !"request_error".equals(k)) {
        response.getAuthPayload().put(k, v);
      }
    });

    return response;
  }

  /**
   * Update the oauthUserInputFromConnectorConfigSpecification to allow for additional properties. The
   * testing values must define the required values, but can send along additional fields from the
   * testing values as well. TODO: Protocolize that this must always be set to true?
   */
  public static void updateOauthConfigToAcceptAdditionalUserInputProperties(
                                                                            final OAuthConfigSpecification oauthConfigSpecification) {
    final JsonNode userInputNode = oauthConfigSpecification.getOauthUserInputFromConnectorConfigSpecification();
    final JsonNode updatedNode = Jsons.getNodeOrEmptyObject(userInputNode);

    Jsons.setNestedValue(updatedNode, List.of("type"), TextNode.valueOf("object"));
    Jsons.setNestedValue(updatedNode, List.of("additionalProperties"), BooleanNode.TRUE);

    oauthConfigSpecification.setOauthUserInputFromConnectorConfigSpecification(updatedNode);
  }

}
