/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.factory

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.JsonNodeType
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.analytics.TrackingClient
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.lang.Exceptions
import io.airbyte.config.DestinationOAuthParameter
import io.airbyte.config.ScopeType
import io.airbyte.config.SourceOAuthParameter
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.OAuthService
import io.airbyte.data.services.SourceService
import io.airbyte.oauth.MoreOAuthParameters
import io.airbyte.oauth.MoreOAuthParameters.flattenOAuthConfig
import io.airbyte.oauth.MoreOAuthParameters.mergeJsons
import io.airbyte.persistence.job.tracker.TrackingMetadata
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.validation.json.JsonValidationException
import io.github.oshai.kotlinlogging.KotlinLogging
import java.io.IOException
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function.BiConsumer

/**
 * OAuth Configs contain secrets. They also combine the configuration for a single configured source
 * or destination with workspace-wide credentials. This class provides too to handle the operations
 * safely.
 */
class OAuthConfigSupplier(
  private val trackingClient: TrackingClient,
  private val actorDefinitionVersionHelper: ActorDefinitionVersionHelper,
  private val oAuthService: OAuthService,
  private val sourceService: SourceService,
  private val destinationService: DestinationService,
) {
  /**
   * Mask secrets in OAuth params.
   *
   * @param sourceDefinitionId source definition id
   * @param workspaceId workspace id
   * @param sourceConnectorConfig config to mask
   * @param sourceConnectorSpec source connector specification
   * @return masked config
   * @throws IOException while fetching oauth configs
   */
  @Throws(IOException::class)
  fun maskSourceOAuthParameters(
    sourceDefinitionId: UUID,
    workspaceId: UUID,
    sourceConnectorConfig: JsonNode,
    sourceConnectorSpec: ConnectorSpecification?,
  ): JsonNode {
    try {
      val sourceDefinition = sourceService.getStandardSourceDefinition(sourceDefinitionId)
      oAuthService
        .getSourceOAuthParameterOptional(workspaceId, sourceDefinitionId)
        .ifPresent { sourceOAuthParameter: SourceOAuthParameter? ->
          maskOauthParameters(
            sourceDefinition.name,
            sourceConnectorSpec,
            sourceConnectorConfig,
          )
        }
      return sourceConnectorConfig
    } catch (e: JsonValidationException) {
      throw IOException(e)
    } catch (e: ConfigNotFoundException) {
      throw IOException(e)
    }
  }

  /**
   * Mask secrets in OAuth params.
   *
   * @param destinationDefinitionId destination definition id
   * @param workspaceId workspace id
   * @param destinationConnectorConfig config to mask
   * @param destinationConnectorSpec destination connector specification
   * @return masked config
   * @throws IOException while fetching oauth configs
   */
  @Throws(IOException::class)
  fun maskDestinationOAuthParameters(
    destinationDefinitionId: UUID,
    workspaceId: UUID,
    destinationConnectorConfig: JsonNode,
    destinationConnectorSpec: ConnectorSpecification,
  ): JsonNode {
    try {
      val destinationDefinition = destinationService.getStandardDestinationDefinition(destinationDefinitionId)
      oAuthService
        .getDestinationOAuthParameterOptional(workspaceId, destinationDefinitionId)
        .ifPresent { destinationOAuthParameter: DestinationOAuthParameter? ->
          maskOauthParameters(
            destinationDefinition.name,
            destinationConnectorSpec,
            destinationConnectorConfig,
          )
        }
      return destinationConnectorConfig
    } catch (e: JsonValidationException) {
      throw IOException(e)
    } catch (e: ConfigNotFoundException) {
      throw IOException(e)
    }
  }

  /**
   * Inject OAuth params for a source.
   *
   * @param sourceDefinitionId source definition id
   * @param sourceId source id
   * @param workspaceId workspace id
   * @param sourceConnectorConfig source connector config
   * @return config with oauth params injected
   * @throws IOException while fetching oauth configs
   */
  @Throws(IOException::class)
  fun injectSourceOAuthParameters(
    sourceDefinitionId: UUID,
    sourceId: UUID?,
    workspaceId: UUID,
    sourceConnectorConfig: JsonNode,
  ): JsonNode {
    try {
      val sourceDefinition = sourceService.getStandardSourceDefinition(sourceDefinitionId)
      val sourceVersion = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, workspaceId, sourceId)
      oAuthService
        .getSourceOAuthParameterOptional(workspaceId, sourceDefinitionId)
        .ifPresent { sourceOAuthParameter: SourceOAuthParameter ->
          if (injectOAuthParameters(
              sourceDefinition.name,
              sourceVersion.spec,
              sourceOAuthParameter.configuration,
              sourceConnectorConfig,
            )
          ) {
            val metadata = TrackingMetadata.generateSourceDefinitionMetadata(sourceDefinition, sourceVersion)
            Exceptions.swallow {
              trackingClient.track(
                workspaceId,
                ScopeType.WORKSPACE,
                "OAuth Injection - Backend",
                metadata,
              )
            }
          }
        }
      return sourceConnectorConfig
    } catch (e: JsonValidationException) {
      throw IOException(e)
    } catch (e: ConfigNotFoundException) {
      throw IOException(e)
    }
  }

  /**
   * Inject OAuth params for a destination.
   *
   * @param destinationDefinitionId destination definition id
   * @param destinationId destination id
   * @param workspaceId workspace id
   * @param destinationConnectorConfig destination connector config
   * @return config with oauth params injected
   * @throws IOException while fetching oauth configs
   */
  @Throws(IOException::class)
  fun injectDestinationOAuthParameters(
    destinationDefinitionId: UUID,
    destinationId: UUID?,
    workspaceId: UUID,
    destinationConnectorConfig: JsonNode,
  ): JsonNode {
    try {
      val destinationDefinition = destinationService.getStandardDestinationDefinition(destinationDefinitionId)
      val destinationVersion =
        actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, workspaceId, destinationId)
      oAuthService
        .getDestinationOAuthParameterOptional(workspaceId, destinationDefinitionId)
        .ifPresent { destinationOAuthParameter: DestinationOAuthParameter ->
          if (injectOAuthParameters(
              destinationDefinition.name,
              destinationVersion.spec,
              destinationOAuthParameter.configuration,
              destinationConnectorConfig,
            )
          ) {
            val metadata = TrackingMetadata.generateDestinationDefinitionMetadata(destinationDefinition, destinationVersion)
            Exceptions.swallow {
              trackingClient.track(
                workspaceId,
                ScopeType.WORKSPACE,
                "OAuth Injection - Backend",
                metadata,
              )
            }
          }
        }
      return destinationConnectorConfig
    } catch (e: JsonValidationException) {
      throw IOException(e)
    } catch (e: ConfigNotFoundException) {
      throw IOException(e)
    }
  }

  companion object {
    private val log = KotlinLogging.logger {}

    const val PATH_IN_CONNECTOR_CONFIG: String = "path_in_connector_config"
    private const val PROPERTIES = "properties"

    /**
     * Test if a connector spec has oauth configuration.
     *
     * @param spec to check
     * @return true if it has an oauth config. otherwise, false.
     */
    @JvmStatic
    fun hasOAuthConfigSpecification(spec: ConnectorSpecification?): Boolean =
      spec != null && spec.advancedAuth != null && spec.advancedAuth.oauthConfigSpecification != null

    /**
     * Gets the OAuth parameter paths as specified in the connector spec and traverses through them.
     *
     * @param spec of connector
     * @param connectorName name of connector
     * @param consumer to process oauth specification
     */
    private fun traverseOAuthOutputPaths(
      spec: ConnectorSpecification?,
      connectorName: String,
      consumer: BiConsumer<String, List<String>>,
    ) {
      val outputSpecTop = spec?.advancedAuth?.oauthConfigSpecification?.completeOauthServerOutputSpecification
      val outputSpec: JsonNode
      if (outputSpecTop != null && outputSpecTop.has(PROPERTIES)) {
        outputSpec = outputSpecTop[PROPERTIES]
      } else {
        log.error(
          String.format(
            "In %s's advanced_auth spec, completeOAuthServerOutputSpecification does not declare properties.",
            connectorName,
          ),
        )
        return
      }

      for (key in Jsons.keys(outputSpec)) {
        val node = outputSpec[key]
        if (node.nodeType == JsonNodeType.OBJECT) {
          val pathNode = node[PATH_IN_CONNECTOR_CONFIG]
          if (pathNode != null && pathNode.nodeType == JsonNodeType.ARRAY) {
            val propertyPath: MutableList<String> = ArrayList()
            val arrayNode = pathNode as ArrayNode
            for (i in 0..<arrayNode.size()) {
              propertyPath.add(arrayNode[i].asText())
            }
            if (!propertyPath.isEmpty()) {
              consumer.accept(key, propertyPath)
            } else {
              log.error(
                String.format(
                  "In %s's advanced_auth spec, completeOAuthServerOutputSpecification includes an invalid empty %s for %s",
                  connectorName,
                  PATH_IN_CONNECTOR_CONFIG,
                  key,
                ),
              )
            }
          } else {
            log.error(
              String.format(
                "In %s's advanced_auth spec, completeOAuthServerOutputSpecification does not declare an Array<String> %s for %s",
                connectorName,
                PATH_IN_CONNECTOR_CONFIG,
                key,
              ),
            )
          }
        } else {
          log.error(
            String.format(
              "In %s's advanced_auth spec, completeOAuthServerOutputSpecification does not declare an ObjectNode for %s",
              connectorName,
              key,
            ),
          )
        }
      }
    }

    private fun maskOauthParameters(
      connectorName: String,
      spec: ConnectorSpecification?,
      connectorConfig: JsonNode,
    ) {
      if (!hasOAuthConfigSpecification(spec)) {
        return
      }
      if (!checkOAuthPredicate(spec?.advancedAuth?.predicateKey, spec?.advancedAuth?.predicateValue, connectorConfig)) {
        // OAuth is not applicable in this connectorConfig due to the predicate not being verified
        return
      }

      traverseOAuthOutputPaths(
        spec,
        connectorName,
      ) { ignore: String?, propertyPath: List<String> ->
        Jsons.replaceNestedValue(
          connectorConfig,
          propertyPath,
          Jsons.jsonNode(MoreOAuthParameters.SECRET_MASK),
        )
      }
    }

    private fun injectOAuthParameters(
      connectorName: String,
      spec: ConnectorSpecification?,
      oauthParameters: JsonNode,
      connectorConfig: JsonNode,
    ): Boolean {
      if (!hasOAuthConfigSpecification(spec)) {
        // keep backward compatible behavior if connector does not declare an OAuth config spec
        mergeJsons(
          (connectorConfig as ObjectNode),
          (oauthParameters as ObjectNode),
        )
        return true
      }
      if (!checkOAuthPredicate(spec?.advancedAuth?.predicateKey, spec?.advancedAuth?.predicateValue, connectorConfig)) {
        // OAuth is not applicable in this connectorConfig due to the predicate not being verified
        return false
      }

      // TODO: if we write a migration to flatten persisted configs in db, we don't need to flatten
      // here see https://github.com/airbytehq/airbyte/issues/7624
      val flatOAuthParameters = flattenOAuthConfig(oauthParameters)

      val result = AtomicBoolean(false)
      traverseOAuthOutputPaths(
        spec,
        connectorName,
      ) { key: String?, propertyPath: List<String> ->
        Jsons.replaceNestedValue(
          connectorConfig,
          propertyPath,
          flatOAuthParameters[key],
        )
        result.set(true)
      }

      return result.get()
    }

    private fun checkOAuthPredicate(
      predicateKey: List<String>?,
      predicateValue: String?,
      connectorConfig: JsonNode,
    ): Boolean {
      if (predicateKey != null && !predicateKey.isEmpty()) {
        var node = connectorConfig
        for (key in predicateKey) {
          if (node.has(key)) {
            node = node[key]
          } else {
            return false
          }
        }
        return if (predicateValue != null && !predicateValue.isBlank()) {
          node.asText() == predicateValue
        } else {
          true
        }
      }
      return true
    }
  }
}
