/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.protocol.models.OAuthConfigSpecification;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class OAuthHelperTest {

  @Test
  void testExtract() {
    final JsonNode input = Jsons.deserialize("""
                                             {
                                                     "type": "object",
                                                     "additionalProperties": false,
                                                     "properties": {
                                                       "tenant_id": {
                                                         "type": "string",
                                                         "path_in_connector_config": ["tenant_id"]
                                                       },
                                                       "another_property": {
                                                         "type": "string",
                                                         "path_in_connector_config": ["another", "property"]
                                                       }
                                                     }
                                                   }
                                             """);

    final Map<String, List<String>> expected = Map.ofEntries(
        Map.entry("tenant_id", List.of("tenant_id")),
        Map.entry("another_property", List.of("another", "property")));

    Assertions.assertThat(OAuthHelper.extractOauthConfigurationPaths(input))
        .containsExactlyInAnyOrderEntriesOf(expected);
  }

  @Test
  void testUpdateOauthConfigToAcceptAdditionalUserInputProperties() {
    final OAuthConfigSpecification input = Jsons.object(
        Jsons.deserialize("""
                          {
                                "complete_oauth_output_specification": {},
                                "complete_oauth_server_input_specification": {},
                                "complete_oauth_server_output_specification": {},
                                "oauth_connector_input_specification": {},
                                "oauth_user_input_from_connector_config_specification": {
                                  "type": "object",
                                  "additionalProperties": false,
                                  "properties": {
                                    "subdomain": {
                                      "type": "string",
                                      "path_in_connector_config": ["credentials", "subdomain"]
                                    }
                                  }
                                }
                              }
                          """),
        OAuthConfigSpecification.class);

    final OAuthConfigSpecification expected = Jsons.object(
        Jsons.deserialize("""
                          {
                                "complete_oauth_output_specification": {},
                                "complete_oauth_server_input_specification": {},
                                "complete_oauth_server_output_specification": {},
                                "oauth_connector_input_specification": {},
                                "oauth_user_input_from_connector_config_specification": {
                                  "type": "object",
                                  "additionalProperties": true,
                                  "properties": {
                                    "subdomain": {
                                      "type": "string",
                                      "path_in_connector_config": ["credentials", "subdomain"]
                                    }
                                  }
                                }
                              }
                          """),
        OAuthConfigSpecification.class);
    OAuthHelper.updateOauthConfigToAcceptAdditionalUserInputProperties(input);
    Assertions.assertThat(input).isEqualTo(expected);
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "{\"oauth_user_input_from_connector_config_specification\": {}}",
    "{\"oauth_user_input_from_connector_config_specification\": null}",
    "{}"
  })
  void testUpdateOauthConfigToAcceptAdditionalUserInputPropertiesHandlesEdgeCases(final String jsonStringConfig) {
    final OAuthConfigSpecification input = Jsons.object(
        Jsons.deserialize(jsonStringConfig),
        OAuthConfigSpecification.class);

    final OAuthConfigSpecification expected = Jsons.object(
        Jsons.deserialize("""
                          {
                              "oauth_user_input_from_connector_config_specification": {
                                  "type": "object",
                                  "additionalProperties": true
                              }
                          }
                          """),
        OAuthConfigSpecification.class);

    OAuthHelper.updateOauthConfigToAcceptAdditionalUserInputProperties(input);

    Assertions.assertThat(input).isEqualTo(expected);
  }

}
