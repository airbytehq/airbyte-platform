/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.errors.BadObjectSchemaKnownException;
import io.airbyte.oauth.MoreOAuthParameters;
import io.airbyte.persistence.job.factory.OAuthConfigSupplier;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.validation.json.JsonValidationException;
import java.util.ArrayList;
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
    final Map<String, List<String>> oauthPaths = getOAuthConfigPaths(spec);
    final JsonNode flattenedSecret = MoreOAuthParameters.flattenOAuthConfig(hydratedSecret);

    for (final Entry<String, List<String>> entry : oauthPaths.entrySet()) {
      // Key where we need to stuff things
      final String key = entry.getKey();
      final List<String> jsonPathList = entry.getValue();

      Jsons.setNestedValue(newConnectionConfiguration, jsonPathList, flattenedSecret.get(key));
    }
    return newConnectionConfiguration;
  }

  /**
   * From advanced_auth gets the data we need to put into the connectionConfiguration which we get
   * from both complete_oauth and the actor_oauth_params Returns a map of the property_name:
   * path_in_connector_config i.e. { client_id: ['credentials', 'client_id']}
   */
  @VisibleForTesting
  public static Map<String, List<String>> getAdvancedAuthOAuthPaths(final ConnectorSpecification connectorSpecification,
                                                                    final boolean includeOutputPaths)
      throws JsonValidationException {
    if (OAuthConfigSupplier.hasOAuthConfigSpecification(connectorSpecification)) {
      final JsonNode completeOAuthOutputSpecification =
          connectorSpecification.getAdvancedAuth().getOauthConfigSpecification().getCompleteOauthOutputSpecification();
      final JsonNode completeOAuthServerOutputSpecification =
          connectorSpecification.getAdvancedAuth().getOauthConfigSpecification().getCompleteOauthServerOutputSpecification();

      // Merge all the mappings into one map
      final Map<String, List<String>> result = new HashMap<>(OAuthHelper.extractOauthConfigurationPaths(completeOAuthServerOutputSpecification));
      if (includeOutputPaths) {
        result.putAll(OAuthHelper.extractOauthConfigurationPaths(completeOAuthOutputSpecification));
      }
      return result;
    } else {
      throw new JsonValidationException(
          String.format("Error parsing advancedAuth - see [%s]", connectorSpecification.getDocumentationUrl()));
    }
  }

  /**
   * Standardizes out the return format for getting config paths whether it's a legacy OAuth spec or
   * an advanced_auth one. Returns all output paths, used for setting secrets.
   *
   * @param spec - connector specification to get paths for
   * @return Map where the key = the property and the value = the path to the property in list form.
   */
  public static Map<String, List<String>> getOAuthConfigPaths(final ConnectorSpecification spec) throws JsonValidationException {
    if (OAuthConfigSupplier.hasOAuthConfigSpecification(spec)) {
      return getAdvancedAuthOAuthPaths(spec, true);
    } else {
      throw new IllegalStateException("No OAuth data in specification");
    }
  }

  /**
   * Like getOAuthConfigPaths but does not include the server output paths in case users need to
   * change them independently. Used for validation.
   *
   * @param spec - connector specification to get paths for
   * @return Map where the key = the property and the value = the path to the property in list form.
   */
  public static Map<String, List<String>> getOAuthInputPaths(final ConnectorSpecification spec) throws JsonValidationException {
    if (OAuthConfigSupplier.hasOAuthConfigSpecification(spec)) {
      return getAdvancedAuthOAuthPaths(spec, false);
    } else {
      throw new IllegalStateException("No OAuth data in specification");
    }
  }

  /**
   * Get OAuth secret paths but only for the completeOauthServerOutput portion of the connector
   * specification.
   *
   * @param connectorSpecification connector specification from source/destination version
   * @return Map of property: path in connector config.
   * @throws JsonValidationException if we don't have an oauth config specification to parse.
   */
  public static Map<String, List<String>> getCompleteOauthServerOutputPaths(final ConnectorSpecification connectorSpecification)
      throws JsonValidationException {
    if (OAuthConfigSupplier.hasOAuthConfigSpecification(connectorSpecification)) {
      final JsonNode completeOAuthServerOutputSpecification =
          connectorSpecification.getAdvancedAuth().getOauthConfigSpecification().getCompleteOauthServerOutputSpecification();

      // Merge all the mappings into one map
      return new HashMap<>(OAuthHelper.extractOauthConfigurationPaths(completeOAuthServerOutputSpecification));
    } else {
      throw new JsonValidationException(
          String.format("Error parsing advancedAuth - see [%s]", connectorSpecification.getDocumentationUrl()));
    }
  }

  /**
   * Traverses advancedAuth structure to validate the input oauth param configuration. As it
   * traverses, it builds up a connector specification that we can pass into statefulSplitSecrets to
   * get back an oauth param configuration that has had its secrets stripped out and replaced
   * appropriately. I could have split these into separate functions, but they would have just done
   * the same loop. For advanced auth sources/destinations, we don't actually mark the secrets in the
   * connector configuration For this to work with statefulSplitSecrets we need to spoof the
   * connection configuration so that the oauth params are "secrets" and so that the connection
   * configuration can be fed into statefulSplitSecrets and return us a properly sanitized oauth param
   * config.
   *
   * @param connectorSpecification connector specification
   * @param oauthParamConfiguration the passed in oauth param configuration
   * @return a connector specification that has each path from CompleteOauthServerOutputPaths set to
   *         have airbyte_secret: true
   * @throws JsonValidationException If there is no valid OauthConfig Specification.
   */
  @VisibleForTesting
  public static ConnectorSpecification validateOauthParamConfigAndReturnAdvancedAuthSecretSpec(final ConnectorSpecification connectorSpecification,
                                                                                               final JsonNode oauthParamConfiguration)
      throws JsonValidationException {
    if (OAuthConfigSupplier.hasOAuthConfigSpecification(connectorSpecification)) {
      final JsonNode newConnectorSpecificationNode = Jsons.emptyObject();
      final Map<String, Boolean> airbyteSecret = Map.of("airbyte_secret", true);
      final Map<String, List<String>> oauthPaths = OAuthSecretHelper.getCompleteOauthServerOutputPaths(connectorSpecification);
      for (final Entry<String, List<String>> entry : oauthPaths.entrySet()) {
        final List<String> jsonPathList = entry.getValue();
        if (Jsons.navigateTo(oauthParamConfiguration, jsonPathList) == null) {
          throw new BadObjectSchemaKnownException(String.format("Missing OAuth param for key at %s", jsonPathList));
        }
        Jsons.setNestedValue(newConnectorSpecificationNode, alternatingList("properties", jsonPathList), Jsons.jsonNode(airbyteSecret));
      }

      return new ConnectorSpecification().withConnectionSpecification(newConnectorSpecificationNode);
    }

    throw new BadObjectSchemaKnownException("No valid OAuth config specification");
  }

  /**
   * Create a list with alternating elements of property, list[n]. Used to spoof a connector
   * specification for splitting out secrets.
   *
   * @param property property to put in front of each list element
   * @param list list to insert elements into
   * @return new list with alternating elements
   */
  private static List<String> alternatingList(final String property,
                                              final List<String> list) {
    final List<String> result = new ArrayList<String>(list.size() * 2);

    for (final String item : list) {
      result.add(property);
      result.add(item);
    }
    return result;
  }

  /**
   * Throws an exception if any property is set in the given configuration which corresponds to an
   * airbyte_secret field which might be injected by the server in the spec.
   */
  public static void validateNoSecretsInConfiguration(final ConnectorSpecification spec,
                                                      final JsonNode connectionConfiguration)
      throws JsonValidationException {
    if (OAuthConfigSupplier.hasOAuthConfigSpecification(spec)) {
      final Map<String, List<String>> oauthPaths = getOAuthInputPaths(spec);
      for (final Entry<String, List<String>> entry : oauthPaths.entrySet()) {
        final String key = entry.getKey();
        final List<String> jsonPathList = entry.getValue();

        throwIfKeyExistsInConfig(connectionConfiguration, key, jsonPathList);
      }
    }
  }

  private static void throwIfKeyExistsInConfig(final JsonNode connectionConfiguration, final String key, final List<String> jsonPathList) {
    if (Jsons.navigateTo(connectionConfiguration, jsonPathList) != null) {
      // The API referenced by this message is a Cloud feature and not yet available in the open source
      // project but will be added.
      final String errorMessage = String.format(
          "Cannot set key '%s', please create an OAuth credentials override instead - https://reference.airbyte.com/reference/workspaceoauthcredentials",
          key);
      throw new BadObjectSchemaKnownException(errorMessage);
    }
  }

}
