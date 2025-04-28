/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.commons.server.handlers.helpers.OAuthSecretHelper.validateOauthParamConfigAndReturnAdvancedAuthSecretSpec;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.DESTINATION_DEFINITION_ID_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.SOURCE_DEFINITION_ID_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.WORKSPACE_ID_KEY;

import com.amazonaws.util.json.Jackson;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.analytics.TrackingClient;
import io.airbyte.api.client.model.generated.WorkspaceOverrideOauthParamsRequestBody;
import io.airbyte.api.model.generated.ActorTypeEnum;
import io.airbyte.api.model.generated.CompleteDestinationOAuthRequest;
import io.airbyte.api.model.generated.CompleteOAuthResponse;
import io.airbyte.api.model.generated.CompleteSourceOauthRequest;
import io.airbyte.api.model.generated.DestinationOauthConsentRequest;
import io.airbyte.api.model.generated.OAuthConsentRead;
import io.airbyte.api.model.generated.RevokeSourceOauthTokensRequest;
import io.airbyte.api.model.generated.SetInstancewideDestinationOauthParamsRequestBody;
import io.airbyte.api.model.generated.SetInstancewideSourceOauthParamsRequestBody;
import io.airbyte.api.model.generated.SourceOauthConsentRequest;
import io.airbyte.commons.constants.AirbyteSecretConstants;
import io.airbyte.commons.json.JsonPaths;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.errors.BadObjectSchemaKnownException;
import io.airbyte.commons.server.handlers.helpers.OAuthHelper;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.DestinationOAuthParameter;
import io.airbyte.config.ScopeType;
import io.airbyte.config.SecretPersistenceConfig;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.SourceOAuthParameter;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.secrets.ConfigWithSecretReferences;
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence;
import io.airbyte.config.secrets.persistence.SecretPersistence;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.OAuthService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.domain.models.ActorDefinitionId;
import io.airbyte.domain.models.OrganizationId;
import io.airbyte.domain.services.secrets.SecretHydrationContext;
import io.airbyte.domain.services.secrets.SecretPersistenceService;
import io.airbyte.domain.services.secrets.SecretReferenceService;
import io.airbyte.featureflag.DestinationDefinition;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.FieldSelectionWorkspaces.ConnectorOAuthConsentDisabled;
import io.airbyte.featureflag.Multi;
import io.airbyte.featureflag.Organization;
import io.airbyte.featureflag.SourceDefinition;
import io.airbyte.featureflag.UseRuntimeSecretPersistence;
import io.airbyte.featureflag.Workspace;
import io.airbyte.metrics.MetricClient;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.oauth.MoreOAuthParameters;
import io.airbyte.oauth.OAuthFlowImplementation;
import io.airbyte.oauth.OAuthImplementationFactory;
import io.airbyte.persistence.job.factory.OAuthConfigSupplier;
import io.airbyte.persistence.job.tracker.TrackingMetadata;
import io.airbyte.protocol.models.v0.ConnectorSpecification;
import io.airbyte.protocol.models.v0.OAuthConfigSpecification;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OAuthHandler. Javadocs suppressed because api docs should be used as source of truth.
 */
@SuppressWarnings({"ParameterName", "PMD.AvoidDuplicateLiterals", "PMD.PreserveStackTrace"})
@Singleton
public class OAuthHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(OAuthHandler.class);
  private static final String ERROR_MESSAGE = "failed while reporting usage.";

  private static final String ORGANIZATION_SECRET_PREFIX = "organization_";

  private final OAuthImplementationFactory oAuthImplementationFactory;
  private final TrackingClient trackingClient;
  private final SecretsRepositoryWriter secretsRepositoryWriter;
  private final SecretsRepositoryReader secretsRepositoryReader;
  private final ActorDefinitionVersionHelper actorDefinitionVersionHelper;
  private final FeatureFlagClient featureFlagClient;
  private final SourceService sourceService;
  private final DestinationService destinationService;
  private final OAuthService oAuthService;
  private final SecretPersistenceConfigService secretPersistenceConfigService;
  private final SecretPersistenceService secretPersistenceService;
  private final SecretReferenceService secretReferenceService;
  private final WorkspaceService workspaceService;
  private final MetricClient metricClient;

  public OAuthHandler(@Named("oauthImplementationFactory") final OAuthImplementationFactory oauthImplementationFactory,
                      final TrackingClient trackingClient,
                      final SecretsRepositoryWriter secretsRepositoryWriter,
                      final SecretsRepositoryReader secretsRepositoryReader,
                      final ActorDefinitionVersionHelper actorDefinitionVersionHelper,
                      final FeatureFlagClient featureFlagClient,
                      final SourceService sourceService,
                      final DestinationService destinationService,
                      final OAuthService oauthService,
                      final SecretPersistenceConfigService secretPersistenceConfigService,
                      final SecretPersistenceService secretPersistenceService,
                      final SecretReferenceService secretReferenceService,
                      final WorkspaceService workspaceService,
                      final MetricClient metricClient) {
    this.oAuthImplementationFactory = oauthImplementationFactory;
    this.trackingClient = trackingClient;
    this.secretsRepositoryWriter = secretsRepositoryWriter;
    this.secretsRepositoryReader = secretsRepositoryReader;
    this.actorDefinitionVersionHelper = actorDefinitionVersionHelper;
    this.featureFlagClient = featureFlagClient;
    this.sourceService = sourceService;
    this.destinationService = destinationService;
    this.oAuthService = oauthService;
    this.secretPersistenceConfigService = secretPersistenceConfigService;
    this.secretPersistenceService = secretPersistenceService;
    this.secretReferenceService = secretReferenceService;
    this.workspaceService = workspaceService;
    this.metricClient = metricClient;
  }

  @SuppressWarnings("PMD.PreserveStackTrace")
  public OAuthConsentRead getSourceOAuthConsent(final SourceOauthConsentRequest sourceOauthConsentRequest)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final Map<String, Object> traceTags = Map.of(WORKSPACE_ID_KEY, sourceOauthConsentRequest.getWorkspaceId(), SOURCE_DEFINITION_ID_KEY,
        sourceOauthConsentRequest.getSourceDefinitionId());
    ApmTraceUtils.addTagsToTrace(traceTags);
    ApmTraceUtils.addTagsToRootSpan(traceTags);

    if (featureFlagClient.boolVariation(ConnectorOAuthConsentDisabled.INSTANCE, new Multi(List.of(
        new SourceDefinition(sourceOauthConsentRequest.getSourceDefinitionId()),
        new Workspace(sourceOauthConsentRequest.getWorkspaceId()))))) {
      // OAuth temporary disabled for this connector via feature flag
      // Returns a 404 (ConfigNotFoundException) so that the frontend displays the correct error message
      throw new ConfigNotFoundException(ConfigSchema.SOURCE_OAUTH_PARAM, "OAuth temporarily disabled");
    }

    final StandardSourceDefinition sourceDefinition =
        sourceService.getStandardSourceDefinition(sourceOauthConsentRequest.getSourceDefinitionId());
    final ActorDefinitionVersion sourceVersion = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition,
        sourceOauthConsentRequest.getWorkspaceId(), sourceOauthConsentRequest.getSourceId());

    final UUID workspaceId = sourceOauthConsentRequest.getWorkspaceId();
    final UUID organizationId = workspaceService.getOrganizationIdFromWorkspaceId(workspaceId).get();

    final ConnectorSpecification spec = sourceVersion.getSpec();
    final OAuthFlowImplementation oAuthFlowImplementation = oAuthImplementationFactory.create(sourceVersion.getDockerRepository(), spec);
    final Map<String, Object> metadata = TrackingMetadata.generateSourceDefinitionMetadata(sourceDefinition, sourceVersion);
    final OAuthConsentRead result;

    final Optional<SourceOAuthParameter> paramOptional = oAuthService
        .getSourceOAuthParameterWithSecretsOptional(sourceOauthConsentRequest.getWorkspaceId(), sourceOauthConsentRequest.getSourceDefinitionId());
    final JsonNode sourceOAuthParamConfig = paramOptional.isPresent()
        ? MoreOAuthParameters.flattenOAuthConfig(paramOptional.get().getConfiguration())
        : Jsons.emptyObject();

    if (OAuthConfigSupplier.hasOAuthConfigSpecification(spec)) {
      final OAuthConfigSpecification oauthConfigSpecification = spec.getAdvancedAuth().getOauthConfigSpecification();
      OAuthHelper.updateOauthConfigToAcceptAdditionalUserInputProperties(oauthConfigSpecification);

      final JsonNode oAuthInputConfigurationForConsent;

      if (sourceOauthConsentRequest.getSourceId() == null) {
        oAuthInputConfigurationForConsent = sourceOauthConsentRequest.getoAuthInputConfiguration();
      } else {
        final SourceConnection sourceConnection;
        try {
          sourceConnection = sourceService.getSourceConnection(sourceOauthConsentRequest.getSourceId());
        } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
          throw new ConfigNotFoundException(e.getType(), e.getConfigId());
        }

        final ConfigWithSecretReferences configWithRefs =
            secretReferenceService.getConfigWithSecretReferences(sourceConnection.getSourceId(), sourceConnection.getConfiguration(), workspaceId);
        final JsonNode hydratedSourceConfig = getHydratedConfiguration(configWithRefs, organizationId, workspaceId);
        oAuthInputConfigurationForConsent = getOAuthInputConfigurationForConsent(spec,
            hydratedSourceConfig,
            sourceOauthConsentRequest.getoAuthInputConfiguration());
      }

      final JsonNode oAuthInputConfigValues = Jsons.mergeNodes(sourceOAuthParamConfig, oAuthInputConfigurationForConsent);

      result = new OAuthConsentRead().consentUrl(oAuthFlowImplementation.getSourceConsentUrl(
          sourceOauthConsentRequest.getWorkspaceId(),
          sourceOauthConsentRequest.getSourceDefinitionId(),
          sourceOauthConsentRequest.getRedirectUrl(),
          oAuthInputConfigValues,
          oauthConfigSpecification, oAuthInputConfigValues));
    } else {
      result = new OAuthConsentRead().consentUrl(oAuthFlowImplementation.getSourceConsentUrl(
          sourceOauthConsentRequest.getWorkspaceId(),
          sourceOauthConsentRequest.getSourceDefinitionId(),
          sourceOauthConsentRequest.getRedirectUrl(), Jsons.emptyObject(), null, sourceOAuthParamConfig));
    }
    try {
      trackingClient.track(sourceOauthConsentRequest.getWorkspaceId(), ScopeType.WORKSPACE, "Get Oauth Consent URL - Backend", metadata);
    } catch (final Exception e) {
      LOGGER.error(ERROR_MESSAGE, e);
    }
    return result;
  }

  @SuppressWarnings("PMD.PreserveStackTrace")
  public OAuthConsentRead getDestinationOAuthConsent(final DestinationOauthConsentRequest destinationOauthConsentRequest)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final Map<String, Object> traceTags = Map.of(WORKSPACE_ID_KEY, destinationOauthConsentRequest.getWorkspaceId(), DESTINATION_DEFINITION_ID_KEY,
        destinationOauthConsentRequest.getDestinationDefinitionId());
    ApmTraceUtils.addTagsToTrace(traceTags);
    ApmTraceUtils.addTagsToRootSpan(traceTags);

    if (featureFlagClient.boolVariation(ConnectorOAuthConsentDisabled.INSTANCE, new Multi(List.of(
        new DestinationDefinition(destinationOauthConsentRequest.getDestinationDefinitionId()),
        new Workspace(destinationOauthConsentRequest.getWorkspaceId()))))) {
      // OAuth temporary disabled for this connector via feature flag
      // Returns a 404 (ConfigNotFoundException) so that the frontend displays the correct error message
      throw new ConfigNotFoundException(ConfigSchema.DESTINATION_OAUTH_PARAM, "OAuth temporarily disabled");
    }

    final StandardDestinationDefinition destinationDefinition =
        destinationService.getStandardDestinationDefinition(destinationOauthConsentRequest.getDestinationDefinitionId());
    final ActorDefinitionVersion destinationVersion = actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition,
        destinationOauthConsentRequest.getWorkspaceId(), destinationOauthConsentRequest.getDestinationId());
    final ConnectorSpecification spec = destinationVersion.getSpec();
    final OAuthFlowImplementation oAuthFlowImplementation = oAuthImplementationFactory.create(destinationVersion.getDockerRepository(), spec);
    final Map<String, Object> metadata = TrackingMetadata.generateDestinationDefinitionMetadata(destinationDefinition, destinationVersion);
    final OAuthConsentRead result;

    final Optional<DestinationOAuthParameter> paramOptional = oAuthService.getDestinationOAuthParameterWithSecretsOptional(
        destinationOauthConsentRequest.getWorkspaceId(), destinationOauthConsentRequest.getDestinationDefinitionId());
    final JsonNode destinationOAuthParamConfig = paramOptional.isPresent()
        ? MoreOAuthParameters.flattenOAuthConfig(paramOptional.get().getConfiguration())
        : Jsons.emptyObject();

    if (OAuthConfigSupplier.hasOAuthConfigSpecification(spec)) {
      final UUID workspaceId = destinationOauthConsentRequest.getWorkspaceId();
      final UUID organizationId = workspaceService.getOrganizationIdFromWorkspaceId(workspaceId).get();

      final OAuthConfigSpecification oauthConfigSpecification = spec.getAdvancedAuth().getOauthConfigSpecification();
      OAuthHelper.updateOauthConfigToAcceptAdditionalUserInputProperties(oauthConfigSpecification);

      final JsonNode oAuthInputConfigurationForConsent;

      if (destinationOauthConsentRequest.getDestinationId() == null) {
        oAuthInputConfigurationForConsent = destinationOauthConsentRequest.getoAuthInputConfiguration();
      } else {
        final DestinationConnection destinationConnection;
        try {
          destinationConnection = destinationService.getDestinationConnection(destinationOauthConsentRequest.getDestinationId());
        } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
          throw new ConfigNotFoundException(e.getType(), e.getConfigId());
        }

        final ConfigWithSecretReferences configWithRefs =
            secretReferenceService.getConfigWithSecretReferences(destinationConnection.getDestinationId(), destinationConnection.getConfiguration(),
                workspaceId);
        final JsonNode hydratedDestinationConfig = getHydratedConfiguration(configWithRefs, organizationId, workspaceId);
        oAuthInputConfigurationForConsent = getOAuthInputConfigurationForConsent(spec,
            hydratedDestinationConfig,
            destinationOauthConsentRequest.getoAuthInputConfiguration());

      }

      final JsonNode oAuthInputConfigValues = Jsons.mergeNodes(destinationOAuthParamConfig, oAuthInputConfigurationForConsent);

      result = new OAuthConsentRead().consentUrl(oAuthFlowImplementation.getDestinationConsentUrl(
          destinationOauthConsentRequest.getWorkspaceId(),
          destinationOauthConsentRequest.getDestinationDefinitionId(),
          destinationOauthConsentRequest.getRedirectUrl(),
          oAuthInputConfigValues,
          spec.getAdvancedAuth().getOauthConfigSpecification(), oAuthInputConfigValues));
    } else {
      result = new OAuthConsentRead().consentUrl(oAuthFlowImplementation.getDestinationConsentUrl(
          destinationOauthConsentRequest.getWorkspaceId(),
          destinationOauthConsentRequest.getDestinationDefinitionId(),
          destinationOauthConsentRequest.getRedirectUrl(), Jsons.emptyObject(), null, destinationOAuthParamConfig));
    }
    try {
      trackingClient.track(destinationOauthConsentRequest.getWorkspaceId(), ScopeType.WORKSPACE, "Get Oauth Consent URL - Backend", metadata);
    } catch (final Exception e) {
      LOGGER.error(ERROR_MESSAGE, e);
    }
    return result;
  }

  public CompleteOAuthResponse completeSourceOAuthHandleReturnSecret(final CompleteSourceOauthRequest completeSourceOauthRequest)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final CompleteOAuthResponse completeOAuthResponse = completeSourceOAuth(completeSourceOauthRequest);
    if (completeOAuthResponse != null && completeSourceOauthRequest.getReturnSecretCoordinate()) {
      return writeOAuthResponseSecret(completeSourceOauthRequest.getWorkspaceId(), completeOAuthResponse);
    } else {
      return completeOAuthResponse;
    }
  }

  @VisibleForTesting
  @SuppressWarnings("PMD.PreserveStackTrace")
  public CompleteOAuthResponse completeSourceOAuth(final CompleteSourceOauthRequest completeSourceOauthRequest)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final Map<String, Object> traceTags = Map.of(WORKSPACE_ID_KEY, completeSourceOauthRequest.getWorkspaceId(), SOURCE_DEFINITION_ID_KEY,
        completeSourceOauthRequest.getSourceDefinitionId());
    ApmTraceUtils.addTagsToTrace(traceTags);
    ApmTraceUtils.addTagsToRootSpan(traceTags);

    final StandardSourceDefinition sourceDefinition =
        sourceService.getStandardSourceDefinition(completeSourceOauthRequest.getSourceDefinitionId());
    final ActorDefinitionVersion sourceVersion = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition,
        completeSourceOauthRequest.getWorkspaceId(), completeSourceOauthRequest.getSourceId());

    final UUID workspaceId = completeSourceOauthRequest.getWorkspaceId();
    final UUID organizationId = workspaceService.getOrganizationIdFromWorkspaceId(workspaceId).get();

    final ConnectorSpecification spec = sourceVersion.getSpec();
    final OAuthFlowImplementation oAuthFlowImplementation = oAuthImplementationFactory.create(sourceVersion.getDockerRepository(), spec);
    final Map<String, Object> metadata = TrackingMetadata.generateSourceDefinitionMetadata(sourceDefinition, sourceVersion);
    final Map<String, Object> result;

    final Optional<SourceOAuthParameter> paramOptional = oAuthService
        .getSourceOAuthParameterWithSecretsOptional(completeSourceOauthRequest.getWorkspaceId(), completeSourceOauthRequest.getSourceDefinitionId());
    final JsonNode sourceOAuthParamConfig = paramOptional.isPresent()
        ? MoreOAuthParameters.flattenOAuthConfig(paramOptional.get().getConfiguration())
        : Jsons.emptyObject();

    if (OAuthConfigSupplier.hasOAuthConfigSpecification(spec)) {
      final OAuthConfigSpecification oauthConfigSpecification = spec.getAdvancedAuth().getOauthConfigSpecification();
      OAuthHelper.updateOauthConfigToAcceptAdditionalUserInputProperties(oauthConfigSpecification);

      final JsonNode oAuthInputConfigurationForConsent;

      if (completeSourceOauthRequest.getSourceId() == null) {
        oAuthInputConfigurationForConsent = completeSourceOauthRequest.getoAuthInputConfiguration();
      } else {
        final SourceConnection sourceConnection;
        try {
          sourceConnection = sourceService.getSourceConnection(completeSourceOauthRequest.getSourceId());
        } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
          throw new ConfigNotFoundException(e.getType(), e.getConfigId());
        }

        final ConfigWithSecretReferences configWithRefs =
            secretReferenceService.getConfigWithSecretReferences(sourceConnection.getSourceId(), sourceConnection.getConfiguration(), workspaceId);
        final JsonNode hydratedSourceConfig = getHydratedConfiguration(configWithRefs, organizationId, workspaceId);
        oAuthInputConfigurationForConsent = getOAuthInputConfigurationForConsent(spec,
            hydratedSourceConfig,
            completeSourceOauthRequest.getoAuthInputConfiguration());
      }

      final JsonNode oAuthInputConfigValues = Jsons.mergeNodes(sourceOAuthParamConfig, oAuthInputConfigurationForConsent);

      result = oAuthFlowImplementation.completeSourceOAuth(
          completeSourceOauthRequest.getWorkspaceId(),
          completeSourceOauthRequest.getSourceDefinitionId(),
          completeSourceOauthRequest.getQueryParams(),
          completeSourceOauthRequest.getRedirectUrl(),
          oAuthInputConfigValues,
          oauthConfigSpecification,
          oAuthInputConfigValues);
    } else {
      // deprecated but this path is kept for connectors that don't define OAuth Spec yet
      result = oAuthFlowImplementation.completeSourceOAuth(
          completeSourceOauthRequest.getWorkspaceId(),
          completeSourceOauthRequest.getSourceDefinitionId(),
          completeSourceOauthRequest.getQueryParams(),
          completeSourceOauthRequest.getRedirectUrl(), sourceOAuthParamConfig);
    }
    try {
      trackingClient.track(completeSourceOauthRequest.getWorkspaceId(), ScopeType.WORKSPACE, "Complete OAuth Flow - Backend", metadata);
    } catch (final Exception e) {
      LOGGER.error(ERROR_MESSAGE, e);
    }
    return OAuthHelper.mapToCompleteOAuthResponse(result);
  }

  @SuppressWarnings("PMD.PreserveStackTrace")
  public CompleteOAuthResponse completeDestinationOAuth(final CompleteDestinationOAuthRequest completeDestinationOAuthRequest)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final Map<String, Object> traceTags = Map.of(WORKSPACE_ID_KEY, completeDestinationOAuthRequest.getWorkspaceId(), DESTINATION_DEFINITION_ID_KEY,
        completeDestinationOAuthRequest.getDestinationDefinitionId());
    ApmTraceUtils.addTagsToTrace(traceTags);
    ApmTraceUtils.addTagsToRootSpan(traceTags);

    final StandardDestinationDefinition destinationDefinition =
        destinationService.getStandardDestinationDefinition(completeDestinationOAuthRequest.getDestinationDefinitionId());
    final ActorDefinitionVersion destinationVersion = actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition,
        completeDestinationOAuthRequest.getWorkspaceId(), completeDestinationOAuthRequest.getDestinationId());
    final ConnectorSpecification spec = destinationVersion.getSpec();
    final OAuthFlowImplementation oAuthFlowImplementation = oAuthImplementationFactory.create(destinationVersion.getDockerRepository(), spec);
    final Map<String, Object> metadata = TrackingMetadata.generateDestinationDefinitionMetadata(destinationDefinition, destinationVersion);
    final Map<String, Object> result;

    final UUID workspaceId = completeDestinationOAuthRequest.getWorkspaceId();
    final UUID organizationId = workspaceService.getOrganizationIdFromWorkspaceId(workspaceId).get();

    final Optional<DestinationOAuthParameter> paramOptional = oAuthService.getDestinationOAuthParameterWithSecretsOptional(
        completeDestinationOAuthRequest.getWorkspaceId(), completeDestinationOAuthRequest.getDestinationDefinitionId());
    final JsonNode destinationOAuthParamConfig = paramOptional.isPresent()
        ? MoreOAuthParameters.flattenOAuthConfig(paramOptional.get().getConfiguration())
        : Jsons.emptyObject();

    if (OAuthConfigSupplier.hasOAuthConfigSpecification(spec)) {
      final OAuthConfigSpecification oauthConfigSpecification = spec.getAdvancedAuth().getOauthConfigSpecification();
      OAuthHelper.updateOauthConfigToAcceptAdditionalUserInputProperties(oauthConfigSpecification);

      final JsonNode oAuthInputConfigurationForConsent;

      if (completeDestinationOAuthRequest.getDestinationId() == null) {
        oAuthInputConfigurationForConsent = completeDestinationOAuthRequest.getoAuthInputConfiguration();
      } else {
        final DestinationConnection destinationConnection;
        try {
          destinationConnection = destinationService.getDestinationConnection(completeDestinationOAuthRequest.getDestinationId());
        } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
          throw new ConfigNotFoundException(e.getType(), e.getConfigId());
        }

        final ConfigWithSecretReferences configWithRefs =
            secretReferenceService.getConfigWithSecretReferences(destinationConnection.getDestinationId(), destinationConnection.getConfiguration(),
                workspaceId);
        final JsonNode hydratedDestinationConfig = getHydratedConfiguration(configWithRefs, organizationId, workspaceId);
        oAuthInputConfigurationForConsent = getOAuthInputConfigurationForConsent(spec,
            hydratedDestinationConfig,
            completeDestinationOAuthRequest.getoAuthInputConfiguration());

      }

      final JsonNode oAuthInputConfigValues = Jsons.mergeNodes(destinationOAuthParamConfig, oAuthInputConfigurationForConsent);

      result = oAuthFlowImplementation.completeDestinationOAuth(
          completeDestinationOAuthRequest.getWorkspaceId(),
          completeDestinationOAuthRequest.getDestinationDefinitionId(),
          completeDestinationOAuthRequest.getQueryParams(),
          completeDestinationOAuthRequest.getRedirectUrl(),
          oAuthInputConfigValues,
          oauthConfigSpecification, oAuthInputConfigValues);
    } else {
      // deprecated but this path is kept for connectors that don't define OAuth Spec yet
      result = oAuthFlowImplementation.completeDestinationOAuth(
          completeDestinationOAuthRequest.getWorkspaceId(),
          completeDestinationOAuthRequest.getDestinationDefinitionId(),
          completeDestinationOAuthRequest.getQueryParams(),
          completeDestinationOAuthRequest.getRedirectUrl(), destinationOAuthParamConfig);
    }
    try {
      trackingClient.track(completeDestinationOAuthRequest.getWorkspaceId(), ScopeType.WORKSPACE, "Complete OAuth Flow - Backend", metadata);
    } catch (final Exception e) {
      LOGGER.error(ERROR_MESSAGE, e);
    }
    return OAuthHelper.mapToCompleteOAuthResponse(result);
  }

  @SuppressWarnings("PMD.PreserveStackTrace")
  public void revokeSourceOauthTokens(final RevokeSourceOauthTokensRequest revokeSourceOauthTokensRequest)
      throws IOException, ConfigNotFoundException, JsonValidationException {
    final StandardSourceDefinition sourceDefinition =
        sourceService.getStandardSourceDefinition(revokeSourceOauthTokensRequest.getSourceDefinitionId());
    final UUID workspaceId = revokeSourceOauthTokensRequest.getWorkspaceId();
    final UUID organizationId = workspaceService.getOrganizationIdFromWorkspaceId(workspaceId).get();
    final ActorDefinitionVersion sourceVersion = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition,
        workspaceId, revokeSourceOauthTokensRequest.getSourceId());
    final ConnectorSpecification spec = sourceVersion.getSpec();
    final OAuthFlowImplementation oAuthFlowImplementation = oAuthImplementationFactory.create(sourceVersion.getDockerRepository(), spec);
    final SourceConnection sourceConnection;
    try {
      sourceConnection = sourceService.getSourceConnection(
          revokeSourceOauthTokensRequest.getSourceId());
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getConfigId());
    }
    final JsonNode sourceOAuthParamConfig =
        getSourceOAuthParamConfig(revokeSourceOauthTokensRequest.getWorkspaceId(), revokeSourceOauthTokensRequest.getSourceDefinitionId());
    final ConfigWithSecretReferences configWithRefs =
        secretReferenceService.getConfigWithSecretReferences(sourceConnection.getSourceId(), sourceConnection.getConfiguration(), workspaceId);
    final JsonNode hydratedSourceConfig = getHydratedConfiguration(configWithRefs, organizationId, workspaceId);
    oAuthFlowImplementation.revokeSourceOauth(
        revokeSourceOauthTokensRequest.getWorkspaceId(),
        revokeSourceOauthTokensRequest.getSourceDefinitionId(),
        hydratedSourceConfig,
        sourceOAuthParamConfig);
  }

  public void setSourceInstancewideOauthParams(final SetInstancewideSourceOauthParamsRequestBody requestBody)
      throws IOException {
    final SourceOAuthParameter param = oAuthService
        .getSourceOAuthParamByDefinitionIdOptional(Optional.empty(), Optional.empty(), requestBody.getSourceDefinitionId())
        .orElseGet(() -> new SourceOAuthParameter().withOauthParameterId(UUID.randomUUID()))
        .withConfiguration(Jsons.jsonNode(requestBody.getParams()))
        .withSourceDefinitionId(requestBody.getSourceDefinitionId());
    // TODO validate requestBody.getParams() against
    // spec.getAdvancedAuth().getOauthConfigSpecification().getCompleteOauthServerInputSpecification()
    oAuthService.writeSourceOAuthParam(param);
  }

  public void setDestinationInstancewideOauthParams(final SetInstancewideDestinationOauthParamsRequestBody requestBody)
      throws IOException {
    final DestinationOAuthParameter param = oAuthService
        .getDestinationOAuthParamByDefinitionIdOptional(Optional.empty(), Optional.empty(), requestBody.getDestinationDefinitionId())
        .orElseGet(() -> new DestinationOAuthParameter().withOauthParameterId(UUID.randomUUID()))
        .withConfiguration(Jsons.jsonNode(requestBody.getParams()))
        .withDestinationDefinitionId(requestBody.getDestinationDefinitionId());
    // TODO validate requestBody.getParams() against
    // spec.getAdvancedAuth().getOauthConfigSpecification().getCompleteOauthServerInputSpecification()
    oAuthService.writeDestinationOAuthParam(param);
  }

  private JsonNode getOAuthInputConfigurationForConsent(final ConnectorSpecification spec,
                                                        final JsonNode hydratedSourceConnectionConfiguration,
                                                        final JsonNode oAuthInputConfiguration) {
    final Map<String, String> configOauthFields =
        buildJsonPathFromOAuthFlowInitParameters(OAuthHelper.extractOauthConfigurationPaths(
            spec.getAdvancedAuth().getOauthConfigSpecification().getOauthUserInputFromConnectorConfigSpecification()));
    final Map<String, String> serverOrConfigOauthFields = buildJsonPathFromOAuthFlowInitParameters(OAuthHelper.extractOauthConfigurationPaths(
        spec.getAdvancedAuth().getOauthConfigSpecification().getCompleteOauthServerOutputSpecification()));
    configOauthFields.putAll(serverOrConfigOauthFields);

    final JsonNode oAuthInputConfigurationFromDB = getOAuthInputConfiguration(hydratedSourceConnectionConfiguration, configOauthFields);
    LOGGER.warn("oAuthInputConfigurationFromDB: {}", oAuthInputConfigurationFromDB);

    return getOauthFromDBIfNeeded(oAuthInputConfigurationFromDB, oAuthInputConfiguration);
  }

  @VisibleForTesting
  Map<String, String> buildJsonPathFromOAuthFlowInitParameters(final Map<String, List<String>> oAuthFlowInitParameters) {
    return oAuthFlowInitParameters.entrySet().stream()
        .map(entry -> Map.entry(entry.getKey(), "$." + String.join(".", entry.getValue())))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @VisibleForTesting
  JsonNode getOauthFromDBIfNeeded(final JsonNode oAuthInputConfigurationFromDB, final JsonNode oAuthInputConfigurationFromInput) {
    final ObjectNode result = (ObjectNode) Jsons.emptyObject();

    oAuthInputConfigurationFromInput.fields().forEachRemaining(entry -> {
      final String k = entry.getKey();
      final JsonNode v = entry.getValue();

      // Note: This does not currently handle replacing masked secrets within nested objects.
      if (AirbyteSecretConstants.SECRETS_MASK.equals(v.textValue())) {
        if (oAuthInputConfigurationFromDB.has(k)) {
          result.set(k, oAuthInputConfigurationFromDB.get(k));
        } else {
          LOGGER.warn("Missing the key {} in the config store in DB", k);
        }
      } else {
        result.set(k, v);
      }
    });

    return result;
  }

  @VisibleForTesting
  JsonNode getOAuthInputConfiguration(final JsonNode hydratedSourceConnectionConfiguration, final Map<String, String> pathsToGet) {
    final Map<String, JsonNode> result = new HashMap<>();
    pathsToGet.forEach((k, v) -> {
      final Optional<JsonNode> configValue = JsonPaths.getSingleValue(hydratedSourceConnectionConfiguration, v);
      if (configValue.isPresent()) {
        result.put(k, configValue.get());
      } else {
        LOGGER.warn("Missing the key {} from the config stored in DB", k);
      }
    });

    return Jsons.jsonNode(result);
  }

  /**
   * Given an OAuth response, writes a secret and returns the secret Coordinate in the appropriate
   * format.
   * <p>
   * Unlike our regular source creation flow, the OAuth credentials created and stored this way will
   * be stored in a singular secret as a string. When these secrets are used, the user will be
   * expected to use the specification to rehydrate the connection configuration with the secret
   * values prior to saving a source/destination.
   * <p>
   * The singular secret was chosen to optimize UX for public API consumers (passing them one secret
   * to keep track of > passing them a set of secrets).
   * <p>
   * See https://github.com/airbytehq/airbyte/pull/22151#discussion_r1104856648 for full discussion.
   */
  @SuppressWarnings("PMD.PreserveStackTrace")
  public CompleteOAuthResponse writeOAuthResponseSecret(final UUID workspaceId, final CompleteOAuthResponse payload) {
    try {
      final String payloadString = Jackson.getObjectMapper().writeValueAsString(payload);
      final AirbyteManagedSecretCoordinate secretCoordinate;
      final SecretPersistence secretPersistence = secretPersistenceService.getPersistenceFromWorkspaceId(workspaceId);
      secretCoordinate = secretsRepositoryWriter.store(
          generateOAuthSecretCoordinate(workspaceId),
          payloadString,
          secretPersistence);
      return OAuthHelper.mapToCompleteOAuthResponse(Map.of("secretId", secretCoordinate.getFullCoordinate()));
    } catch (final JsonProcessingException e) {
      throw new RuntimeException("Json object could not be written to string.", e);
    }
  }

  /**
   * Generate OAuthSecretCoordinates. Always use the default version and do not support secret updates
   */
  private AirbyteManagedSecretCoordinate generateOAuthSecretCoordinate(final UUID workspaceId) {
    return new AirbyteManagedSecretCoordinate(
        "oauth_workspace_",
        workspaceId,
        AirbyteManagedSecretCoordinate.DEFAULT_VERSION,
        UUID::randomUUID);
  }

  /**
   * Sets workspace level overrides for OAuth parameters.
   *
   * @param requestBody request body
   */
  public void setWorkspaceOverrideOAuthParams(final WorkspaceOverrideOauthParamsRequestBody requestBody)
      throws JsonValidationException, IOException, ConfigNotFoundException, io.airbyte.config.persistence.ConfigNotFoundException {
    switch (requestBody.getActorType()) {
      case SOURCE -> setSourceWorkspaceOverrideOauthParams(requestBody);
      case DESTINATION -> setDestinationWorkspaceOverrideOauthParams(requestBody);
      default -> throw new BadObjectSchemaKnownException("actorType must be one of ['source', 'destination']");
    }
  }

  public void setOrganizationOverrideOAuthParams(final OrganizationId organizationId,
                                                 final ActorDefinitionId actorDefinitionId,
                                                 final ActorTypeEnum actorType,
                                                 final JsonNode params)
      throws JsonValidationException, IOException, ConfigNotFoundException, io.airbyte.config.persistence.ConfigNotFoundException {
    switch (actorType) {
      case SOURCE -> setSourceOrganizationOverrideOauthParams(organizationId, actorDefinitionId, params);
      case DESTINATION -> setDestinationOrganizationOverrideOauthParams(organizationId, actorDefinitionId, params);
      default -> throw new BadObjectSchemaKnownException("actorType must be one of ['source', 'destination']");
    }
  }

  public void setSourceOrganizationOverrideOauthParams(final OrganizationId organizationId,
                                                       final ActorDefinitionId actorDefinitionId,
                                                       final JsonNode params)
      throws JsonValidationException, IOException, ConfigNotFoundException, io.airbyte.config.persistence.ConfigNotFoundException {
    final StandardSourceDefinition standardSourceDefinition =
        sourceService.getStandardSourceDefinition(actorDefinitionId.getValue());

    /*
     * It is possible that the version has been overriden for the organization in a way that the spec
     * would be different. We don't currently have a method for getting a version for an organization so
     * this is a gap right now.
     */
    final ActorDefinitionVersion actorDefinitionVersion = actorDefinitionVersionHelper.getDefaultSourceVersion(standardSourceDefinition);

    final ConnectorSpecification connectorSpecification = actorDefinitionVersion.getSpec();

    final JsonNode sanitizedOauthConfiguration =
        sanitizeOauthConfiguration(organizationId.getValue(), connectorSpecification, params, Optional.empty());

    final SourceOAuthParameter param = oAuthService
        .getSourceOAuthParamByDefinitionIdOptional(Optional.empty(), Optional.of(organizationId.getValue()), actorDefinitionId.getValue())
        .orElseGet(() -> new SourceOAuthParameter().withOauthParameterId(UUID.randomUUID()))
        .withConfiguration(sanitizedOauthConfiguration)
        .withSourceDefinitionId(actorDefinitionId.getValue())
        .withOrganizationId(organizationId.getValue());

    oAuthService.writeSourceOAuthParam(param);
  }

  public void setSourceWorkspaceOverrideOauthParams(final WorkspaceOverrideOauthParamsRequestBody requestBody)
      throws JsonValidationException, IOException, ConfigNotFoundException, io.airbyte.config.persistence.ConfigNotFoundException {
    final UUID definitionId = requestBody.getDefinitionId();
    final StandardSourceDefinition standardSourceDefinition =
        sourceService.getStandardSourceDefinition(definitionId);

    final UUID workspaceId = requestBody.getWorkspaceId();
    final ActorDefinitionVersion actorDefinitionVersion =
        actorDefinitionVersionHelper.getSourceVersion(standardSourceDefinition, workspaceId);

    final ConnectorSpecification connectorSpecification = actorDefinitionVersion.getSpec();

    final JsonNode oauthParamConfiguration = Jsons.jsonNode(requestBody.getParams());

    final UUID organizationId = workspaceService.getOrganizationIdFromWorkspaceId(workspaceId).get();
    final JsonNode sanitizedOauthConfiguration =
        sanitizeOauthConfiguration(organizationId, connectorSpecification, oauthParamConfiguration, Optional.of(workspaceId));

    final SourceOAuthParameter param = oAuthService
        .getSourceOAuthParamByDefinitionIdOptional(Optional.of(workspaceId), Optional.empty(), definitionId)
        .orElseGet(() -> new SourceOAuthParameter().withOauthParameterId(UUID.randomUUID()))
        .withConfiguration(sanitizedOauthConfiguration)
        .withSourceDefinitionId(definitionId)
        .withWorkspaceId(workspaceId);

    oAuthService.writeSourceOAuthParam(param);
  }

  public void setDestinationOrganizationOverrideOauthParams(final OrganizationId organizationId,
                                                            final ActorDefinitionId actorDefinitionId,
                                                            final JsonNode params)
      throws JsonValidationException, IOException, ConfigNotFoundException, io.airbyte.config.persistence.ConfigNotFoundException {
    final StandardDestinationDefinition standardDestinationDefinition =
        destinationService.getStandardDestinationDefinition(actorDefinitionId.getValue());

    /*
     * It is possible that the version has been overriden for the organization in a way that the spec
     * would be different. We don't currently have a method for getting a version for an organization so
     * this is a gap right now.
     */
    final ActorDefinitionVersion actorDefinitionVersion = actorDefinitionVersionHelper.getDefaultDestinationVersion(standardDestinationDefinition);

    final ConnectorSpecification connectorSpecification = actorDefinitionVersion.getSpec();

    final JsonNode sanitizedOauthConfiguration =
        sanitizeOauthConfiguration(organizationId.getValue(), connectorSpecification, params, Optional.empty());

    final DestinationOAuthParameter param = oAuthService
        .getDestinationOAuthParamByDefinitionIdOptional(Optional.empty(), Optional.of(organizationId.getValue()), actorDefinitionId.getValue())
        .orElseGet(() -> new DestinationOAuthParameter().withOauthParameterId(UUID.randomUUID()))
        .withConfiguration(sanitizedOauthConfiguration)
        .withDestinationDefinitionId(actorDefinitionId.getValue())
        .withOrganizationId(organizationId.getValue());

    oAuthService.writeDestinationOAuthParam(param);
  }

  public void setDestinationWorkspaceOverrideOauthParams(final WorkspaceOverrideOauthParamsRequestBody requestBody)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    final UUID workspaceId = requestBody.getWorkspaceId();
    final UUID definitionId = requestBody.getDefinitionId();
    final StandardDestinationDefinition destinationDefinition =
        destinationService.getStandardDestinationDefinition(definitionId);
    final ActorDefinitionVersion actorDefinitionVersion =
        actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, workspaceId);

    final ConnectorSpecification connectorSpecification = actorDefinitionVersion.getSpec();

    final JsonNode oauthParamConfiguration = Jsons.jsonNode(requestBody.getParams());

    final UUID organizationId = workspaceService.getOrganizationIdFromWorkspaceId(workspaceId).get();

    final JsonNode sanitizedOauthConfiguration =
        sanitizeOauthConfiguration(organizationId, connectorSpecification, oauthParamConfiguration, Optional.of(workspaceId));

    final DestinationOAuthParameter param = oAuthService
        .getDestinationOAuthParamByDefinitionIdOptional(Optional.of(workspaceId), Optional.empty(), definitionId)
        .orElseGet(() -> new DestinationOAuthParameter().withOauthParameterId(UUID.randomUUID()))
        .withConfiguration(sanitizedOauthConfiguration)
        .withDestinationDefinitionId(definitionId)
        .withWorkspaceId(workspaceId);

    oAuthService.writeDestinationOAuthParam(param);
  }

  private JsonNode getHydratedConfiguration(final ConfigWithSecretReferences config, final UUID organizationId, final UUID workspaceId) {
    final SecretHydrationContext hydrationContext = SecretHydrationContext.fromJava(organizationId, workspaceId);
    final Map<UUID, SecretPersistence> secretPersistenceMap = secretPersistenceService.getPersistenceMapFromConfig(config, hydrationContext);
    return secretsRepositoryReader.hydrateConfig(config, secretPersistenceMap);
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
  private JsonNode sanitizeOauthConfiguration(final UUID organizationId,
                                              final ConnectorSpecification connectorSpecification,
                                              final JsonNode oauthParamConfiguration,
                                              final Optional<UUID> workspaceId)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    UUID id = workspaceId.orElse(organizationId);
    String secretPrefix = workspaceId.isPresent() ? AirbyteManagedSecretCoordinate.DEFAULT_SECRET_BASE_PREFIX : ORGANIZATION_SECRET_PREFIX;

    if (OAuthConfigSupplier.hasOAuthConfigSpecification(connectorSpecification)) {
      // Advanced auth handling
      final ConnectorSpecification advancedAuthSpecification =
          validateOauthParamConfigAndReturnAdvancedAuthSecretSpec(connectorSpecification, oauthParamConfiguration);
      LOGGER.debug("AdvancedAuthSpecification: {}", advancedAuthSpecification);

      return statefulSplitSecrets(organizationId, oauthParamConfiguration, advancedAuthSpecification, id, secretPrefix);
    } else {
      // This works because:
      // 1. In non advanced_auth specs, the connector configuration matches the oauth param configuration,
      // the two are just merged together
      // 2. For these non advanced_auth specs, the actual variables are present and tagged as secrets so
      // statefulSplitSecrets can find and
      // store them in our secrets manager and replace the values appropriately.
      return statefulSplitSecrets(organizationId, oauthParamConfiguration, connectorSpecification, id, secretPrefix);
    }
  }

  @VisibleForTesting
  @SuppressWarnings("PMD.PreserveStackTrace")
  JsonNode getSourceOAuthParamConfig(final UUID workspaceId, final UUID sourceDefinitionId) throws IOException, ConfigNotFoundException {
    final Optional<SourceOAuthParameter> paramOptional = oAuthService.getSourceOAuthParameterWithSecretsOptional(workspaceId, sourceDefinitionId);
    if (paramOptional.isPresent()) {
      // TODO: if we write a flyway migration to flatten persisted configs in db, we don't need to flatten
      // here see https://github.com/airbytehq/airbyte/issues/7624
      // Should already be hydrated.
      return MoreOAuthParameters.flattenOAuthConfig(paramOptional.get().getConfiguration());
    } else {
      throw new ConfigNotFoundException(ConfigSchema.SOURCE_OAUTH_PARAM,
          String.format("workspaceId: %s, sourceDefinitionId: %s", workspaceId, sourceDefinitionId));
    }
  }

  @VisibleForTesting
  @SuppressWarnings("PMD.PreserveStackTrace")
  JsonNode getDestinationOAuthParamConfig(final UUID workspaceId, final UUID destinationDefinitionId) throws IOException, ConfigNotFoundException {
    final Optional<DestinationOAuthParameter> paramOptional =
        oAuthService.getDestinationOAuthParameterWithSecretsOptional(workspaceId, destinationDefinitionId);
    if (paramOptional.isPresent()) {
      // TODO: if we write a flyway migration to flatten persisted configs in db, we don't need to flatten
      // here see https://github.com/airbytehq/airbyte/issues/7624
      // Should already be hydrated.
      return MoreOAuthParameters.flattenOAuthConfig(paramOptional.get().getConfiguration());
    } else {
      throw new ConfigNotFoundException(ConfigSchema.DESTINATION_OAUTH_PARAM,
          String.format("workspaceId: %s, destinationDefinitionId: %s", workspaceId, destinationDefinitionId));
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
  @SuppressWarnings("PMD.PreserveStackTrace")
  JsonNode statefulSplitSecrets(final UUID workspaceId, final JsonNode oauthParamConfiguration, final ConnectorSpecification connectorSpecification)
      throws IOException, ConfigNotFoundException {
    final Optional<UUID> organizationId = workspaceService.getOrganizationIdFromWorkspaceId(workspaceId);

    if (organizationId.isPresent()) {
      return statefulSplitSecrets(organizationId.get(), oauthParamConfiguration, connectorSpecification, workspaceId,
          AirbyteManagedSecretCoordinate.DEFAULT_SECRET_BASE_PREFIX);
    } else {
      throw new RuntimeException("Could not find organization ID for workspace ID: " + workspaceId + ". This should never happen.");
    }
  }

  JsonNode statefulSplitSecrets(final UUID organizationId,
                                final JsonNode oauthParamConfiguration,
                                final ConnectorSpecification connectorSpecification,
                                final UUID secretBaseId,
                                final String secretBasePrefix)
      throws IOException, ConfigNotFoundException {
    RuntimeSecretPersistence secretPersistence = null;

    if (featureFlagClient.boolVariation(UseRuntimeSecretPersistence.INSTANCE, new Organization(organizationId))) {
      try {
        final SecretPersistenceConfig secretPersistenceConfig = secretPersistenceConfigService.get(ScopeType.ORGANIZATION, organizationId);
        secretPersistence = new RuntimeSecretPersistence(secretPersistenceConfig, metricClient);
      } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
        throw new ConfigNotFoundException(e.getType(), e.getConfigId());
      }
    }

    return secretsRepositoryWriter.createFromConfigLegacy(secretBaseId, oauthParamConfiguration, connectorSpecification.getConnectionSpecification(),
        secretPersistence, secretBasePrefix);
  }

}
