/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.amazonaws.util.json.Jackson
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.google.common.annotations.VisibleForTesting
import io.airbyte.analytics.TrackingClient
import io.airbyte.api.client.model.generated.ActorType
import io.airbyte.api.client.model.generated.WorkspaceOverrideOauthParamsRequestBody
import io.airbyte.api.model.generated.ActorTypeEnum
import io.airbyte.api.model.generated.CompleteDestinationOAuthRequest
import io.airbyte.api.model.generated.CompleteOAuthResponse
import io.airbyte.api.model.generated.CompleteSourceOauthRequest
import io.airbyte.api.model.generated.DestinationOauthConsentRequest
import io.airbyte.api.model.generated.OAuthConsentRead
import io.airbyte.api.model.generated.RevokeSourceOauthTokensRequest
import io.airbyte.api.model.generated.SetInstancewideDestinationOauthParamsRequestBody
import io.airbyte.api.model.generated.SetInstancewideSourceOauthParamsRequestBody
import io.airbyte.api.model.generated.SourceOauthConsentRequest
import io.airbyte.commons.constants.AirbyteSecretConstants
import io.airbyte.commons.json.JsonPaths
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.errors.BadObjectSchemaKnownException
import io.airbyte.commons.server.handlers.helpers.OAuthHelper.extractOauthConfigurationPaths
import io.airbyte.commons.server.handlers.helpers.OAuthHelper.mapToCompleteOAuthResponse
import io.airbyte.commons.server.handlers.helpers.OAuthHelper.updateOauthConfigToAcceptAdditionalUserInputProperties
import io.airbyte.commons.server.handlers.helpers.OAuthSecretHelper.validateOauthParamConfigAndReturnAdvancedAuthSecretSpec
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.DestinationConnection
import io.airbyte.config.DestinationOAuthParameter
import io.airbyte.config.ScopeType
import io.airbyte.config.SourceConnection
import io.airbyte.config.SourceOAuthParameter
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.secrets.ConfigWithSecretReferences
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.OAuthService
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.domain.models.ActorDefinitionId
import io.airbyte.domain.models.ActorId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.WorkspaceId
import io.airbyte.domain.services.secrets.SecretHydrationContext
import io.airbyte.domain.services.secrets.SecretHydrationContext.Companion.fromJava
import io.airbyte.domain.services.secrets.SecretPersistenceService
import io.airbyte.domain.services.secrets.SecretReferenceService
import io.airbyte.featureflag.DestinationDefinition
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.FieldSelectionWorkspaces.ConnectorOAuthConsentDisabled
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.SourceDefinition
import io.airbyte.featureflag.UseRuntimeSecretPersistence
import io.airbyte.featureflag.Workspace
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.DESTINATION_DEFINITION_ID_KEY
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.SOURCE_DEFINITION_ID_KEY
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.WORKSPACE_ID_KEY
import io.airbyte.metrics.lib.ApmTraceUtils.addTagsToRootSpan
import io.airbyte.metrics.lib.ApmTraceUtils.addTagsToTrace
import io.airbyte.oauth.MoreOAuthParameters.flattenOAuthConfig
import io.airbyte.oauth.OAuthImplementationFactory
import io.airbyte.persistence.job.factory.OAuthConfigSupplier.Companion.hasOAuthConfigSpecification
import io.airbyte.persistence.job.tracker.TrackingMetadata.generateDestinationDefinitionMetadata
import io.airbyte.persistence.job.tracker.TrackingMetadata.generateSourceDefinitionMetadata
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.validation.json.JsonValidationException
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.io.IOException
import java.util.Optional
import java.util.UUID

/**
 * OAuthHandler. Javadocs suppressed because api docs should be used as source of truth.
 */
@Singleton
open class OAuthHandler(
  @param:Named("oauthImplementationFactory") private val oAuthImplementationFactory: OAuthImplementationFactory,
  private val trackingClient: TrackingClient,
  private val secretsRepositoryWriter: SecretsRepositoryWriter,
  private val secretsRepositoryReader: SecretsRepositoryReader,
  private val actorDefinitionVersionHelper: ActorDefinitionVersionHelper,
  private val featureFlagClient: FeatureFlagClient,
  private val sourceService: SourceService,
  private val destinationService: DestinationService,
  private val oAuthService: OAuthService,
  private val secretPersistenceConfigService: SecretPersistenceConfigService,
  private val secretPersistenceService: SecretPersistenceService,
  private val secretReferenceService: SecretReferenceService,
  private val workspaceService: WorkspaceService,
  private val metricClient: MetricClient,
) {
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun getSourceOAuthConsent(sourceOauthConsentRequest: SourceOauthConsentRequest): OAuthConsentRead {
    val traceTags =
      java.util.Map.of<String?, Any?>(
        WORKSPACE_ID_KEY,
        sourceOauthConsentRequest.workspaceId,
        SOURCE_DEFINITION_ID_KEY,
        sourceOauthConsentRequest.sourceDefinitionId,
      )
    addTagsToTrace(traceTags)
    addTagsToRootSpan(traceTags)

    if (featureFlagClient.boolVariation(
        ConnectorOAuthConsentDisabled,
        Multi(
          java.util.List.of(
            SourceDefinition(sourceOauthConsentRequest.sourceDefinitionId),
            Workspace(sourceOauthConsentRequest.workspaceId),
          ),
        ),
      )
    ) {
      // OAuth temporary disabled for this connector via feature flag
      // Returns a 404 (ConfigNotFoundException) so that the frontend displays the correct error message
      throw ConfigNotFoundException(ConfigNotFoundType.SOURCE_OAUTH_PARAM, "OAuth temporarily disabled")
    }

    val sourceDefinition =
      sourceService.getStandardSourceDefinition(sourceOauthConsentRequest.sourceDefinitionId)
    val sourceVersion =
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        sourceOauthConsentRequest.workspaceId,
        sourceOauthConsentRequest.sourceId,
      )

    val workspaceId = sourceOauthConsentRequest.workspaceId
    val organizationId = workspaceService.getOrganizationIdFromWorkspaceId(workspaceId).get()

    val spec = sourceVersion.spec
    val oAuthFlowImplementation = oAuthImplementationFactory.create(sourceVersion.dockerRepository, spec)
    val metadata: Map<String, Any?> = generateSourceDefinitionMetadata(sourceDefinition, sourceVersion)
    val result: OAuthConsentRead

    val paramOptional =
      oAuthService
        .getSourceOAuthParameterWithSecretsOptional(sourceOauthConsentRequest.workspaceId, sourceOauthConsentRequest.sourceDefinitionId)
    val sourceOAuthParamConfig =
      if (paramOptional.isPresent) {
        flattenOAuthConfig(paramOptional.get().configuration)
      } else {
        Jsons.emptyObject()
      }

    if (hasOAuthConfigSpecification(spec)) {
      val oauthConfigSpecification = spec.advancedAuth.oauthConfigSpecification
      updateOauthConfigToAcceptAdditionalUserInputProperties(oauthConfigSpecification)

      val oAuthInputConfigurationForConsent: JsonNode

      if (sourceOauthConsentRequest.sourceId == null) {
        oAuthInputConfigurationForConsent = sourceOauthConsentRequest.getoAuthInputConfiguration()
      } else {
        val sourceConnection: SourceConnection
        try {
          sourceConnection = sourceService.getSourceConnection(sourceOauthConsentRequest.sourceId)
        } catch (e: ConfigNotFoundException) {
          throw ConfigNotFoundException(e.type, e.configId)
        }

        val configWithRefs =
          secretReferenceService.getConfigWithSecretReferences(
            ActorId(sourceConnection.sourceId),
            sourceConnection.configuration,
            WorkspaceId(workspaceId),
          )
        val hydratedSourceConfig = getHydratedConfiguration(configWithRefs, organizationId, workspaceId)
        oAuthInputConfigurationForConsent =
          getOAuthInputConfigurationForConsent(
            spec,
            hydratedSourceConfig,
            sourceOauthConsentRequest.getoAuthInputConfiguration(),
          )
      }

      val oAuthInputConfigValues = Jsons.mergeNodes(sourceOAuthParamConfig, oAuthInputConfigurationForConsent)

      result =
        OAuthConsentRead().consentUrl(
          oAuthFlowImplementation!!.getSourceConsentUrl(
            sourceOauthConsentRequest.workspaceId,
            sourceOauthConsentRequest.sourceDefinitionId,
            sourceOauthConsentRequest.redirectUrl,
            oAuthInputConfigValues,
            oauthConfigSpecification,
            oAuthInputConfigValues,
          ),
        )
    } else {
      result =
        OAuthConsentRead().consentUrl(
          oAuthFlowImplementation!!.getSourceConsentUrl(
            sourceOauthConsentRequest.workspaceId,
            sourceOauthConsentRequest.sourceDefinitionId,
            sourceOauthConsentRequest.redirectUrl,
            Jsons.emptyObject(),
            null,
            sourceOAuthParamConfig,
          ),
        )
    }
    try {
      trackingClient.track(sourceOauthConsentRequest.workspaceId, ScopeType.WORKSPACE, "Get Oauth Consent URL - Backend", metadata)
    } catch (e: Exception) {
      log.error(ERROR_MESSAGE, e)
    }
    return result
  }

  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun getDestinationOAuthConsent(destinationOauthConsentRequest: DestinationOauthConsentRequest): OAuthConsentRead {
    val traceTags =
      java.util.Map.of<String?, Any?>(
        WORKSPACE_ID_KEY,
        destinationOauthConsentRequest.workspaceId,
        DESTINATION_DEFINITION_ID_KEY,
        destinationOauthConsentRequest.destinationDefinitionId,
      )
    addTagsToTrace(traceTags)
    addTagsToRootSpan(traceTags)

    if (featureFlagClient.boolVariation(
        ConnectorOAuthConsentDisabled,
        Multi(
          java.util.List.of(
            DestinationDefinition(destinationOauthConsentRequest.destinationDefinitionId),
            Workspace(destinationOauthConsentRequest.workspaceId),
          ),
        ),
      )
    ) {
      // OAuth temporary disabled for this connector via feature flag
      // Returns a 404 (ConfigNotFoundException) so that the frontend displays the correct error message
      throw ConfigNotFoundException(ConfigNotFoundType.DESTINATION_OAUTH_PARAM, "OAuth temporarily disabled")
    }

    val destinationDefinition =
      destinationService.getStandardDestinationDefinition(destinationOauthConsentRequest.destinationDefinitionId)
    val destinationVersion =
      actorDefinitionVersionHelper.getDestinationVersion(
        destinationDefinition,
        destinationOauthConsentRequest.workspaceId,
        destinationOauthConsentRequest.destinationId,
      )
    val spec = destinationVersion.spec
    val oAuthFlowImplementation = oAuthImplementationFactory.create(destinationVersion.dockerRepository, spec)
    val metadata: Map<String, Any?> = generateDestinationDefinitionMetadata(destinationDefinition, destinationVersion)
    val result: OAuthConsentRead

    val paramOptional =
      oAuthService.getDestinationOAuthParameterWithSecretsOptional(
        destinationOauthConsentRequest.workspaceId,
        destinationOauthConsentRequest.destinationDefinitionId,
      )
    val destinationOAuthParamConfig =
      if (paramOptional.isPresent) {
        flattenOAuthConfig(paramOptional.get().configuration)
      } else {
        Jsons.emptyObject()
      }

    if (hasOAuthConfigSpecification(spec)) {
      val workspaceId = destinationOauthConsentRequest.workspaceId
      val organizationId = workspaceService.getOrganizationIdFromWorkspaceId(workspaceId).get()

      val oauthConfigSpecification = spec.advancedAuth.oauthConfigSpecification
      updateOauthConfigToAcceptAdditionalUserInputProperties(oauthConfigSpecification)

      val oAuthInputConfigurationForConsent: JsonNode

      if (destinationOauthConsentRequest.destinationId == null) {
        oAuthInputConfigurationForConsent = destinationOauthConsentRequest.getoAuthInputConfiguration()
      } else {
        val destinationConnection: DestinationConnection
        try {
          destinationConnection = destinationService.getDestinationConnection(destinationOauthConsentRequest.destinationId)
        } catch (e: ConfigNotFoundException) {
          throw ConfigNotFoundException(e.type, e.configId)
        }

        val configWithRefs =
          secretReferenceService.getConfigWithSecretReferences(
            ActorId(destinationConnection.destinationId),
            destinationConnection.configuration,
            WorkspaceId(workspaceId),
          )
        val hydratedDestinationConfig = getHydratedConfiguration(configWithRefs, organizationId, workspaceId)
        oAuthInputConfigurationForConsent =
          getOAuthInputConfigurationForConsent(
            spec,
            hydratedDestinationConfig,
            destinationOauthConsentRequest.getoAuthInputConfiguration(),
          )
      }

      val oAuthInputConfigValues = Jsons.mergeNodes(destinationOAuthParamConfig, oAuthInputConfigurationForConsent)

      result =
        OAuthConsentRead().consentUrl(
          oAuthFlowImplementation!!.getDestinationConsentUrl(
            destinationOauthConsentRequest.workspaceId,
            destinationOauthConsentRequest.destinationDefinitionId,
            destinationOauthConsentRequest.redirectUrl,
            oAuthInputConfigValues,
            spec.advancedAuth.oauthConfigSpecification,
            oAuthInputConfigValues,
          ),
        )
    } else {
      result =
        OAuthConsentRead().consentUrl(
          oAuthFlowImplementation!!.getDestinationConsentUrl(
            destinationOauthConsentRequest.workspaceId,
            destinationOauthConsentRequest.destinationDefinitionId,
            destinationOauthConsentRequest.redirectUrl,
            Jsons.emptyObject(),
            null,
            destinationOAuthParamConfig,
          ),
        )
    }
    try {
      trackingClient.track(destinationOauthConsentRequest.workspaceId, ScopeType.WORKSPACE, "Get Oauth Consent URL - Backend", metadata)
    } catch (e: Exception) {
      log.error(ERROR_MESSAGE, e)
    }
    return result
  }

  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun completeSourceOAuthHandleReturnSecret(completeSourceOauthRequest: CompleteSourceOauthRequest): CompleteOAuthResponse? {
    val completeOAuthResponse = completeSourceOAuth(completeSourceOauthRequest)
    return if (completeOAuthResponse != null && completeSourceOauthRequest.returnSecretCoordinate) {
      writeOAuthResponseSecret(completeSourceOauthRequest.workspaceId, completeOAuthResponse)
    } else {
      completeOAuthResponse
    }
  }

  @VisibleForTesting
  @Throws(
    JsonValidationException::class,
    ConfigNotFoundException::class,
    IOException::class,
  )
  fun completeSourceOAuth(completeSourceOauthRequest: CompleteSourceOauthRequest): CompleteOAuthResponse {
    val traceTags =
      java.util.Map.of<String?, Any?>(
        WORKSPACE_ID_KEY,
        completeSourceOauthRequest.workspaceId,
        SOURCE_DEFINITION_ID_KEY,
        completeSourceOauthRequest.sourceDefinitionId,
      )
    addTagsToTrace(traceTags)
    addTagsToRootSpan(traceTags)

    val sourceDefinition =
      sourceService.getStandardSourceDefinition(completeSourceOauthRequest.sourceDefinitionId)
    val sourceVersion =
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        completeSourceOauthRequest.workspaceId,
        completeSourceOauthRequest.sourceId,
      )

    val workspaceId = completeSourceOauthRequest.workspaceId
    val organizationId = workspaceService.getOrganizationIdFromWorkspaceId(workspaceId).get()

    val spec = sourceVersion.spec
    val oAuthFlowImplementation = oAuthImplementationFactory.create(sourceVersion.dockerRepository, spec)
    val metadata: Map<String, Any?> = generateSourceDefinitionMetadata(sourceDefinition, sourceVersion)
    val result: Map<String, Any>

    val paramOptional =
      oAuthService
        .getSourceOAuthParameterWithSecretsOptional(completeSourceOauthRequest.workspaceId, completeSourceOauthRequest.sourceDefinitionId)
    val sourceOAuthParamConfig =
      if (paramOptional.isPresent) {
        flattenOAuthConfig(paramOptional.get().configuration)
      } else {
        Jsons.emptyObject()
      }

    if (hasOAuthConfigSpecification(spec)) {
      val oauthConfigSpecification = spec.advancedAuth.oauthConfigSpecification
      updateOauthConfigToAcceptAdditionalUserInputProperties(oauthConfigSpecification)

      val oAuthInputConfigurationForConsent: JsonNode

      if (completeSourceOauthRequest.sourceId == null) {
        oAuthInputConfigurationForConsent = completeSourceOauthRequest.getoAuthInputConfiguration()
      } else {
        val sourceConnection: SourceConnection
        try {
          sourceConnection = sourceService.getSourceConnection(completeSourceOauthRequest.sourceId)
        } catch (e: ConfigNotFoundException) {
          throw ConfigNotFoundException(e.type, e.configId)
        }

        val configWithRefs =
          secretReferenceService.getConfigWithSecretReferences(
            ActorId(sourceConnection.sourceId),
            sourceConnection.configuration,
            WorkspaceId(workspaceId),
          )
        val hydratedSourceConfig = getHydratedConfiguration(configWithRefs, organizationId, workspaceId)
        oAuthInputConfigurationForConsent =
          getOAuthInputConfigurationForConsent(
            spec,
            hydratedSourceConfig,
            completeSourceOauthRequest.getoAuthInputConfiguration(),
          )
      }

      val oAuthInputConfigValues = Jsons.mergeNodes(sourceOAuthParamConfig, oAuthInputConfigurationForConsent)

      result =
        oAuthFlowImplementation!!.completeSourceOAuth(
          completeSourceOauthRequest.workspaceId,
          completeSourceOauthRequest.sourceDefinitionId,
          completeSourceOauthRequest.queryParams,
          completeSourceOauthRequest.redirectUrl,
          oAuthInputConfigValues,
          oauthConfigSpecification,
          oAuthInputConfigValues,
        )
    } else {
      // deprecated but this path is kept for connectors that don't define OAuth Spec yet
      result =
        oAuthFlowImplementation!!.completeSourceOAuth(
          completeSourceOauthRequest.workspaceId,
          completeSourceOauthRequest.sourceDefinitionId,
          completeSourceOauthRequest.queryParams,
          completeSourceOauthRequest.redirectUrl,
          sourceOAuthParamConfig,
        )
    }
    try {
      trackingClient.track(completeSourceOauthRequest.workspaceId, ScopeType.WORKSPACE, "Complete OAuth Flow - Backend", metadata)
    } catch (e: Exception) {
      log.error(ERROR_MESSAGE, e)
    }
    return mapToCompleteOAuthResponse(result)
  }

  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun completeDestinationOAuth(completeDestinationOAuthRequest: CompleteDestinationOAuthRequest): CompleteOAuthResponse {
    val traceTags =
      java.util.Map.of<String?, Any?>(
        WORKSPACE_ID_KEY,
        completeDestinationOAuthRequest.workspaceId,
        DESTINATION_DEFINITION_ID_KEY,
        completeDestinationOAuthRequest.destinationDefinitionId,
      )
    addTagsToTrace(traceTags)
    addTagsToRootSpan(traceTags)

    val destinationDefinition =
      destinationService.getStandardDestinationDefinition(completeDestinationOAuthRequest.destinationDefinitionId)
    val destinationVersion =
      actorDefinitionVersionHelper.getDestinationVersion(
        destinationDefinition,
        completeDestinationOAuthRequest.workspaceId,
        completeDestinationOAuthRequest.destinationId,
      )
    val spec = destinationVersion.spec
    val oAuthFlowImplementation = oAuthImplementationFactory.create(destinationVersion.dockerRepository, spec)
    val metadata: Map<String, Any?> = generateDestinationDefinitionMetadata(destinationDefinition, destinationVersion)
    val result: Map<String, Any>

    val workspaceId = completeDestinationOAuthRequest.workspaceId
    val organizationId = workspaceService.getOrganizationIdFromWorkspaceId(workspaceId).get()

    val paramOptional =
      oAuthService.getDestinationOAuthParameterWithSecretsOptional(
        completeDestinationOAuthRequest.workspaceId,
        completeDestinationOAuthRequest.destinationDefinitionId,
      )
    val destinationOAuthParamConfig =
      if (paramOptional.isPresent) {
        flattenOAuthConfig(paramOptional.get().configuration)
      } else {
        Jsons.emptyObject()
      }

    if (hasOAuthConfigSpecification(spec)) {
      val oauthConfigSpecification = spec.advancedAuth.oauthConfigSpecification
      updateOauthConfigToAcceptAdditionalUserInputProperties(oauthConfigSpecification)

      val oAuthInputConfigurationForConsent: JsonNode

      if (completeDestinationOAuthRequest.destinationId == null) {
        oAuthInputConfigurationForConsent = completeDestinationOAuthRequest.getoAuthInputConfiguration()
      } else {
        val destinationConnection: DestinationConnection
        try {
          destinationConnection = destinationService.getDestinationConnection(completeDestinationOAuthRequest.destinationId)
        } catch (e: ConfigNotFoundException) {
          throw ConfigNotFoundException(e.type, e.configId)
        }

        val configWithRefs =
          secretReferenceService.getConfigWithSecretReferences(
            ActorId(destinationConnection.destinationId),
            destinationConnection.configuration,
            WorkspaceId(workspaceId),
          )
        val hydratedDestinationConfig = getHydratedConfiguration(configWithRefs, organizationId, workspaceId)
        oAuthInputConfigurationForConsent =
          getOAuthInputConfigurationForConsent(
            spec,
            hydratedDestinationConfig,
            completeDestinationOAuthRequest.getoAuthInputConfiguration(),
          )
      }

      val oAuthInputConfigValues = Jsons.mergeNodes(destinationOAuthParamConfig, oAuthInputConfigurationForConsent)

      result =
        oAuthFlowImplementation!!.completeDestinationOAuth(
          completeDestinationOAuthRequest.workspaceId,
          completeDestinationOAuthRequest.destinationDefinitionId,
          completeDestinationOAuthRequest.queryParams,
          completeDestinationOAuthRequest.redirectUrl,
          oAuthInputConfigValues,
          oauthConfigSpecification,
          oAuthInputConfigValues,
        )
    } else {
      // deprecated but this path is kept for connectors that don't define OAuth Spec yet
      result =
        oAuthFlowImplementation!!.completeDestinationOAuth(
          completeDestinationOAuthRequest.workspaceId,
          completeDestinationOAuthRequest.destinationDefinitionId,
          completeDestinationOAuthRequest.queryParams,
          completeDestinationOAuthRequest.redirectUrl,
          destinationOAuthParamConfig,
        )
    }
    try {
      trackingClient.track(completeDestinationOAuthRequest.workspaceId, ScopeType.WORKSPACE, "Complete OAuth Flow - Backend", metadata)
    } catch (e: Exception) {
      log.error(ERROR_MESSAGE, e)
    }
    return mapToCompleteOAuthResponse(result)
  }

  @Throws(IOException::class, ConfigNotFoundException::class, JsonValidationException::class)
  fun revokeSourceOauthTokens(revokeSourceOauthTokensRequest: RevokeSourceOauthTokensRequest) {
    val sourceDefinition =
      sourceService.getStandardSourceDefinition(revokeSourceOauthTokensRequest.sourceDefinitionId)
    val workspaceId = revokeSourceOauthTokensRequest.workspaceId
    val organizationId = workspaceService.getOrganizationIdFromWorkspaceId(workspaceId).get()
    val sourceVersion =
      actorDefinitionVersionHelper.getSourceVersion(
        sourceDefinition,
        workspaceId,
        revokeSourceOauthTokensRequest.sourceId,
      )
    val spec = sourceVersion.spec
    val oAuthFlowImplementation = oAuthImplementationFactory.create(sourceVersion.dockerRepository, spec)
    val sourceConnection: SourceConnection
    try {
      sourceConnection =
        sourceService.getSourceConnection(
          revokeSourceOauthTokensRequest.sourceId,
        )
    } catch (e: ConfigNotFoundException) {
      throw ConfigNotFoundException(e.type, e.configId)
    }
    val sourceOAuthParamConfig =
      getSourceOAuthParamConfig(revokeSourceOauthTokensRequest.workspaceId, revokeSourceOauthTokensRequest.sourceDefinitionId)
    val configWithRefs =
      secretReferenceService.getConfigWithSecretReferences(
        ActorId(sourceConnection.sourceId),
        sourceConnection.configuration,
        WorkspaceId(workspaceId),
      )
    val hydratedSourceConfig = getHydratedConfiguration(configWithRefs, organizationId, workspaceId)
    oAuthFlowImplementation!!.revokeSourceOauth(
      revokeSourceOauthTokensRequest.workspaceId,
      revokeSourceOauthTokensRequest.sourceDefinitionId,
      hydratedSourceConfig!!,
      sourceOAuthParamConfig,
    )
  }

  @Throws(IOException::class)
  fun setSourceInstancewideOauthParams(requestBody: SetInstancewideSourceOauthParamsRequestBody) {
    val param =
      oAuthService
        .getSourceOAuthParamByDefinitionIdOptional(Optional.empty(), Optional.empty(), requestBody.sourceDefinitionId)
        .orElseGet { SourceOAuthParameter().withOauthParameterId(UUID.randomUUID()) }
        .withConfiguration(Jsons.jsonNode(requestBody.params))
        .withSourceDefinitionId(requestBody.sourceDefinitionId)
    // TODO validate requestBody.getParams() against
    // spec.getAdvancedAuth().getOauthConfigSpecification().getCompleteOauthServerInputSpecification()
    oAuthService.writeSourceOAuthParam(param)
  }

  @Throws(IOException::class)
  fun setDestinationInstancewideOauthParams(requestBody: SetInstancewideDestinationOauthParamsRequestBody) {
    val param =
      oAuthService
        .getDestinationOAuthParamByDefinitionIdOptional(Optional.empty(), Optional.empty(), requestBody.destinationDefinitionId)
        .orElseGet { DestinationOAuthParameter().withOauthParameterId(UUID.randomUUID()) }
        .withConfiguration(Jsons.jsonNode(requestBody.params))
        .withDestinationDefinitionId(requestBody.destinationDefinitionId)
    // TODO validate requestBody.getParams() against
    // spec.getAdvancedAuth().getOauthConfigSpecification().getCompleteOauthServerInputSpecification()
    oAuthService.writeDestinationOAuthParam(param)
  }

  private fun getOAuthInputConfigurationForConsent(
    spec: ConnectorSpecification,
    hydratedSourceConnectionConfiguration: JsonNode,
    oAuthInputConfiguration: JsonNode,
  ): JsonNode {
    val configOauthFields =
      buildJsonPathFromOAuthFlowInitParameters(
        extractOauthConfigurationPaths(
          spec.advancedAuth.oauthConfigSpecification.oauthUserInputFromConnectorConfigSpecification,
        ),
      ).toMutableMap()
    val serverOrConfigOauthFields: Map<String, String> =
      buildJsonPathFromOAuthFlowInitParameters(
        extractOauthConfigurationPaths(
          spec.advancedAuth.oauthConfigSpecification.completeOauthServerOutputSpecification,
        ),
      )
    configOauthFields.putAll(serverOrConfigOauthFields)

    val oAuthInputConfigurationFromDB = getOAuthInputConfiguration(hydratedSourceConnectionConfiguration, configOauthFields)
    log.warn { "oAuthInputConfigurationFromDB: $oAuthInputConfigurationFromDB" }

    return getOauthFromDBIfNeeded(oAuthInputConfigurationFromDB, oAuthInputConfiguration)
  }

  @VisibleForTesting
  fun buildJsonPathFromOAuthFlowInitParameters(oAuthFlowInitParameters: Map<String, List<String>>): Map<String, String> =
    oAuthFlowInitParameters.mapValues { (_, value) -> "$." + value.joinToString(".") }

  @VisibleForTesting
  fun getOauthFromDBIfNeeded(
    oAuthInputConfigurationFromDB: JsonNode,
    oAuthInputConfigurationFromInput: JsonNode,
  ): JsonNode {
    val result = Jsons.emptyObject() as ObjectNode

    oAuthInputConfigurationFromInput.fields().forEachRemaining { entry: Map.Entry<String, JsonNode> ->
      val k = entry.key
      val v = entry.value

      // Note: This does not currently handle replacing masked secrets within nested objects.
      if (AirbyteSecretConstants.SECRETS_MASK == v.textValue()) {
        if (oAuthInputConfigurationFromDB.has(k)) {
          result.set<JsonNode>(k, oAuthInputConfigurationFromDB[k])
        } else {
          log.warn { "Missing the key $k in the config store in DB" }
        }
      } else {
        result.set<JsonNode>(k, v)
      }
    }

    return result
  }

  @VisibleForTesting
  fun getOAuthInputConfiguration(
    hydratedSourceConnectionConfiguration: JsonNode,
    pathsToGet: Map<String, String>,
  ): JsonNode {
    val result: MutableMap<String, JsonNode> = HashMap()
    pathsToGet.forEach { (k: String, v: String) ->
      val configValue = JsonPaths.getSingleValue(hydratedSourceConnectionConfiguration, v)
      if (configValue.isPresent) {
        result[k] = configValue.get()
      } else {
        log.warn { "Missing the key $k from the config stored in DB" }
      }
    }

    return Jsons.jsonNode<Map<String, JsonNode>>(result)
  }

  /**
   * Given an OAuth response, writes a secret and returns the secret Coordinate in the appropriate
   * format.
   *
   *
   * Unlike our regular source creation flow, the OAuth credentials created and stored this way will
   * be stored in a singular secret as a string. When these secrets are used, the user will be
   * expected to use the specification to rehydrate the connection configuration with the secret
   * values prior to saving a source/destination.
   *
   *
   * The singular secret was chosen to optimize UX for public API consumers (passing them one secret
   * to keep track of > passing them a set of secrets).
   *
   *
   * See https://github.com/airbytehq/airbyte/pull/22151#discussion_r1104856648 for full discussion.
   */
  fun writeOAuthResponseSecret(
    workspaceId: UUID,
    payload: CompleteOAuthResponse?,
  ): CompleteOAuthResponse {
    try {
      val payloadString = Jackson.getObjectMapper().writeValueAsString(payload)
      val secretCoordinate: AirbyteManagedSecretCoordinate
      val secretPersistence = secretPersistenceService.getPersistenceFromWorkspaceId(WorkspaceId(workspaceId))
      secretCoordinate =
        secretsRepositoryWriter.store(
          generateOAuthSecretCoordinate(workspaceId),
          payloadString,
          secretPersistence,
        )
      return mapToCompleteOAuthResponse(java.util.Map.of("secretId", secretCoordinate.fullCoordinate))
    } catch (e: JsonProcessingException) {
      throw RuntimeException("Json object could not be written to string.", e)
    }
  }

  /**
   * Generate OAuthSecretCoordinates. Always use the default version and do not support secret updates
   */
  private fun generateOAuthSecretCoordinate(workspaceId: UUID): AirbyteManagedSecretCoordinate =
    AirbyteManagedSecretCoordinate(
      "oauth_workspace_",
      workspaceId,
      AirbyteManagedSecretCoordinate.DEFAULT_VERSION,
    ) { UUID.randomUUID() }

  /**
   * Sets workspace level overrides for OAuth parameters.
   *
   * @param requestBody request body
   */
  @Throws(
    JsonValidationException::class,
    IOException::class,
    ConfigNotFoundException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun setWorkspaceOverrideOAuthParams(requestBody: WorkspaceOverrideOauthParamsRequestBody) {
    when (requestBody.actorType) {
      ActorType.SOURCE -> setSourceWorkspaceOverrideOauthParams(requestBody)
      ActorType.DESTINATION -> setDestinationWorkspaceOverrideOauthParams(requestBody)
      else -> throw BadObjectSchemaKnownException("actorType must be one of ['source', 'destination']")
    }
  }

  @Throws(
    JsonValidationException::class,
    IOException::class,
    ConfigNotFoundException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun setOrganizationOverrideOAuthParams(
    organizationId: OrganizationId,
    actorDefinitionId: ActorDefinitionId,
    actorType: ActorTypeEnum,
    params: JsonNode,
  ) {
    when (actorType) {
      ActorTypeEnum.SOURCE -> setSourceOrganizationOverrideOauthParams(organizationId, actorDefinitionId, params)
      ActorTypeEnum.DESTINATION -> setDestinationOrganizationOverrideOauthParams(organizationId, actorDefinitionId, params)
      else -> throw BadObjectSchemaKnownException("actorType must be one of ['source', 'destination']")
    }
  }

  @Throws(
    JsonValidationException::class,
    IOException::class,
    ConfigNotFoundException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun setSourceOrganizationOverrideOauthParams(
    organizationId: OrganizationId,
    actorDefinitionId: ActorDefinitionId,
    params: JsonNode,
  ) {
    val standardSourceDefinition =
      sourceService.getStandardSourceDefinition(actorDefinitionId.value)

        /*
         * It is possible that the version has been overriden for the organization in a way that the spec
         * would be different. We don't currently have a method for getting a version for an organization so
         * this is a gap right now.
         */
    val actorDefinitionVersion = actorDefinitionVersionHelper.getDefaultSourceVersion(standardSourceDefinition)

    val connectorSpecification = actorDefinitionVersion.spec

    val sanitizedOauthConfiguration =
      sanitizeOauthConfiguration(organizationId.value, connectorSpecification, params, Optional.empty())

    val param =
      oAuthService
        .getSourceOAuthParamByDefinitionIdOptional(Optional.empty(), Optional.of(organizationId.value), actorDefinitionId.value)
        .orElseGet { SourceOAuthParameter().withOauthParameterId(UUID.randomUUID()) }
        .withConfiguration(sanitizedOauthConfiguration)
        .withSourceDefinitionId(actorDefinitionId.value)
        .withOrganizationId(organizationId.value)

    oAuthService.writeSourceOAuthParam(param)
  }

  @Throws(
    JsonValidationException::class,
    IOException::class,
    ConfigNotFoundException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun setSourceWorkspaceOverrideOauthParams(requestBody: WorkspaceOverrideOauthParamsRequestBody) {
    val definitionId = requestBody.definitionId
    val standardSourceDefinition =
      sourceService.getStandardSourceDefinition(definitionId)

    val workspaceId = requestBody.workspaceId
    val actorDefinitionVersion =
      actorDefinitionVersionHelper.getSourceVersion(standardSourceDefinition, workspaceId)

    val connectorSpecification = actorDefinitionVersion.spec

    val oauthParamConfiguration = Jsons.jsonNode(requestBody.params)

    val organizationId = workspaceService.getOrganizationIdFromWorkspaceId(workspaceId).get()
    val sanitizedOauthConfiguration =
      sanitizeOauthConfiguration(organizationId, connectorSpecification, oauthParamConfiguration, Optional.of(workspaceId))

    val param =
      oAuthService
        .getSourceOAuthParamByDefinitionIdOptional(Optional.of(workspaceId), Optional.empty(), definitionId)
        .orElseGet { SourceOAuthParameter().withOauthParameterId(UUID.randomUUID()) }
        .withConfiguration(sanitizedOauthConfiguration)
        .withSourceDefinitionId(definitionId)
        .withWorkspaceId(workspaceId)

    oAuthService.writeSourceOAuthParam(param)
  }

  @Throws(
    JsonValidationException::class,
    IOException::class,
    ConfigNotFoundException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun setDestinationOrganizationOverrideOauthParams(
    organizationId: OrganizationId,
    actorDefinitionId: ActorDefinitionId,
    params: JsonNode,
  ) {
    val standardDestinationDefinition =
      destinationService.getStandardDestinationDefinition(actorDefinitionId.value)

        /*
         * It is possible that the version has been overriden for the organization in a way that the spec
         * would be different. We don't currently have a method for getting a version for an organization so
         * this is a gap right now.
         */
    val actorDefinitionVersion = actorDefinitionVersionHelper.getDefaultDestinationVersion(standardDestinationDefinition)

    val connectorSpecification = actorDefinitionVersion.spec

    val sanitizedOauthConfiguration =
      sanitizeOauthConfiguration(organizationId.value, connectorSpecification, params, Optional.empty())

    val param =
      oAuthService
        .getDestinationOAuthParamByDefinitionIdOptional(Optional.empty(), Optional.of(organizationId.value), actorDefinitionId.value)
        .orElseGet { DestinationOAuthParameter().withOauthParameterId(UUID.randomUUID()) }
        .withConfiguration(sanitizedOauthConfiguration)
        .withDestinationDefinitionId(actorDefinitionId.value)
        .withOrganizationId(organizationId.value)

    oAuthService.writeDestinationOAuthParam(param)
  }

  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  fun setDestinationWorkspaceOverrideOauthParams(requestBody: WorkspaceOverrideOauthParamsRequestBody) {
    val workspaceId = requestBody.workspaceId
    val definitionId = requestBody.definitionId
    val destinationDefinition =
      destinationService.getStandardDestinationDefinition(definitionId)
    val actorDefinitionVersion =
      actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, workspaceId)

    val connectorSpecification = actorDefinitionVersion.spec

    val oauthParamConfiguration = Jsons.jsonNode(requestBody.params)

    val organizationId = workspaceService.getOrganizationIdFromWorkspaceId(workspaceId).get()

    val sanitizedOauthConfiguration =
      sanitizeOauthConfiguration(organizationId, connectorSpecification, oauthParamConfiguration, Optional.of(workspaceId))

    val param =
      oAuthService
        .getDestinationOAuthParamByDefinitionIdOptional(Optional.of(workspaceId), Optional.empty(), definitionId)
        .orElseGet { DestinationOAuthParameter().withOauthParameterId(UUID.randomUUID()) }
        .withConfiguration(sanitizedOauthConfiguration)
        .withDestinationDefinitionId(definitionId)
        .withWorkspaceId(workspaceId)

    oAuthService.writeDestinationOAuthParam(param)
  }

  private fun getHydratedConfiguration(
    config: ConfigWithSecretReferences,
    organizationId: UUID,
    workspaceId: UUID,
  ): JsonNode {
    val hydrationContext: SecretHydrationContext = fromJava(organizationId, workspaceId)
    val secretPersistenceMap: Map<UUID?, SecretPersistence> = secretPersistenceService.getPersistenceMapFromConfig(config, hydrationContext)
    return secretsRepositoryReader.hydrateConfig(config, secretPersistenceMap)
  }

  /**
   * Method to handle sanitizing OAuth param configuration. Secrets are split out and stored in the
   * secrets manager and a new ready-for-storage version of the oauth param config JSON will be
   * returned.
   *
   * @param organizationId the current organization ID
   * @param connectorSpecification the connector specification of the source/destination in question
   * @param oauthParamConfiguration the oauth param configuration passed in by the user.
   * @param workspaceId the workspace ID if applicable
   * @return new oauth param configuration to be stored to the db.
   * @throws JsonValidationException if oauth param configuration doesn't pass spec validation
   */
  @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
  private fun sanitizeOauthConfiguration(
    organizationId: UUID,
    connectorSpecification: ConnectorSpecification,
    oauthParamConfiguration: JsonNode,
    workspaceId: Optional<UUID>,
  ): JsonNode {
    val id = workspaceId.orElse(organizationId)
    val secretPrefix = if (workspaceId.isPresent) AirbyteManagedSecretCoordinate.DEFAULT_SECRET_BASE_PREFIX else ORGANIZATION_SECRET_PREFIX

    if (hasOAuthConfigSpecification(connectorSpecification)) {
      // Advanced auth handling
      val advancedAuthSpecification =
        validateOauthParamConfigAndReturnAdvancedAuthSecretSpec(connectorSpecification, oauthParamConfiguration)
      log.debug { "AdvancedAuthSpecification: $advancedAuthSpecification" }

      return statefulSplitSecrets(organizationId, oauthParamConfiguration, advancedAuthSpecification, id, secretPrefix)
    } else {
      // This works because:
      // 1. In non advanced_auth specs, the connector configuration matches the oauth param configuration,
      // the two are just merged together
      // 2. For these non advanced_auth specs, the actual variables are present and tagged as secrets so
      // statefulSplitSecrets can find and
      // store them in our secrets manager and replace the values appropriately.
      return statefulSplitSecrets(organizationId, oauthParamConfiguration, connectorSpecification, id, secretPrefix)
    }
  }

  @VisibleForTesting
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun getSourceOAuthParamConfig(
    workspaceId: UUID,
    sourceDefinitionId: UUID,
  ): JsonNode {
    val paramOptional = oAuthService.getSourceOAuthParameterWithSecretsOptional(workspaceId, sourceDefinitionId)
    if (paramOptional.isPresent) {
      // TODO: if we write a flyway migration to flatten persisted configs in db, we don't need to flatten
      // here see https://github.com/airbytehq/airbyte/issues/7624
      // Should already be hydrated.
      return flattenOAuthConfig(paramOptional.get().configuration)
    } else {
      throw ConfigNotFoundException(
        ConfigNotFoundType.SOURCE_OAUTH_PARAM,
        String.format("workspaceId: %s, sourceDefinitionId: %s", workspaceId, sourceDefinitionId),
      )
    }
  }

  @VisibleForTesting
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun getDestinationOAuthParamConfig(
    workspaceId: UUID,
    destinationDefinitionId: UUID,
  ): JsonNode {
    val paramOptional =
      oAuthService.getDestinationOAuthParameterWithSecretsOptional(workspaceId, destinationDefinitionId)
    if (paramOptional.isPresent) {
      // TODO: if we write a flyway migration to flatten persisted configs in db, we don't need to flatten
      // here see https://github.com/airbytehq/airbyte/issues/7624
      // Should already be hydrated.
      return flattenOAuthConfig(paramOptional.get().configuration)
    } else {
      throw ConfigNotFoundException(
        ConfigNotFoundType.DESTINATION_OAUTH_PARAM,
        String.format("workspaceId: %s, destinationDefinitionId: %s", workspaceId, destinationDefinitionId),
      )
    }
  }

  /**
   * Wrapper around {SecretsRepositoryWriter#statefulSplitSecrets} that fetches organization and uses
   * runtime secret persistence appropriately.
   *
   * @param workspaceId workspace ID
   * @param oauthParamConfiguration oauth param config
   * @param connectorSpecification either the advancedAuthSpecification or the connectorSpecification.
   * @return OAuth param config with secrets split out.
   */
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun statefulSplitSecrets(
    workspaceId: UUID,
    oauthParamConfiguration: JsonNode,
    connectorSpecification: ConnectorSpecification,
  ): JsonNode {
    val organizationId = workspaceService.getOrganizationIdFromWorkspaceId(workspaceId)

    if (organizationId.isPresent) {
      return statefulSplitSecrets(
        organizationId.get(),
        oauthParamConfiguration,
        connectorSpecification,
        workspaceId,
        AirbyteManagedSecretCoordinate.DEFAULT_SECRET_BASE_PREFIX,
      )
    } else {
      throw RuntimeException("Could not find organization ID for workspace ID: $workspaceId. This should never happen.")
    }
  }

  @Throws(IOException::class, ConfigNotFoundException::class)
  fun statefulSplitSecrets(
    organizationId: UUID,
    oauthParamConfiguration: JsonNode,
    connectorSpecification: ConnectorSpecification,
    secretBaseId: UUID,
    secretBasePrefix: String,
  ): JsonNode {
    var secretPersistence: RuntimeSecretPersistence? = null

    if (featureFlagClient.boolVariation(UseRuntimeSecretPersistence, Organization(organizationId))) {
      try {
        val secretPersistenceConfig = secretPersistenceConfigService.get(ScopeType.ORGANIZATION, organizationId)
        secretPersistence = RuntimeSecretPersistence(secretPersistenceConfig, metricClient)
      } catch (e: ConfigNotFoundException) {
        throw ConfigNotFoundException(e.type, e.configId)
      }
    }

    return secretsRepositoryWriter.createFromConfigLegacy(
      secretBaseId,
      oauthParamConfiguration,
      connectorSpecification.connectionSpecification,
      secretPersistence,
      secretBasePrefix,
    )
  }

  companion object {
    private val log = KotlinLogging.logger {}
    private const val ERROR_MESSAGE = "failed while reporting usage."

    private const val ORGANIZATION_SECRET_PREFIX = "organization_"
  }
}
