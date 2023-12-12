/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
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
import io.airbyte.commons.server.handlers.helpers.OAuthPathExtractor;
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
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.secrets.SecretCoordinate;
import io.airbyte.config.secrets.SecretsHelpers;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.OAuthService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.featureflag.DestinationDefinition;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.FieldSelectionWorkspaces.ConnectorOAuthConsentDisabled;
import io.airbyte.featureflag.Multi;
import io.airbyte.featureflag.Organization;
import io.airbyte.featureflag.SourceDefinition;
import io.airbyte.featureflag.UseRuntimeSecretPersistence;
import io.airbyte.featureflag.Workspace;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.oauth.MoreOAuthParameters;
import io.airbyte.oauth.OAuthFlowImplementation;
import io.airbyte.oauth.OAuthImplementationFactory;
import io.airbyte.persistence.job.factory.OAuthConfigSupplier;
import io.airbyte.persistence.job.tracker.TrackingMetadata;
import io.airbyte.protocol.models.ConnectorSpecification;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.net.http.HttpClient;
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
@SuppressWarnings({"ParameterName", "PMD.AvoidDuplicateLiterals"})
@Singleton
public class OAuthHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(OAuthHandler.class);
  private static final String ERROR_MESSAGE = "failed while reporting usage.";

  private final ConfigRepository configRepository;
  private final OAuthImplementationFactory oAuthImplementationFactory;
  private final TrackingClient trackingClient;
  private final SecretsRepositoryWriter secretsRepositoryWriter;
  private final ActorDefinitionVersionHelper actorDefinitionVersionHelper;
  private final FeatureFlagClient featureFlagClient;
  private final SourceService sourceService;
  private final DestinationService destinationService;
  private final OAuthService oAuthService;
  private final SecretPersistenceConfigService secretPersistenceConfigService;
  private final WorkspaceService workspaceService;

  public OAuthHandler(final ConfigRepository configRepository,
                      @Named("oauthHttpClient") final HttpClient httpClient,
                      final TrackingClient trackingClient,
                      final SecretsRepositoryWriter secretsRepositoryWriter,
                      final ActorDefinitionVersionHelper actorDefinitionVersionHelper,
                      final FeatureFlagClient featureFlagClient,
                      final SourceService sourceService,
                      final DestinationService destinationService,
                      final OAuthService oauthService,
                      final SecretPersistenceConfigService secretPersistenceConfigService,
                      final WorkspaceService workspaceService) {
    this.configRepository = configRepository;
    this.oAuthImplementationFactory = new OAuthImplementationFactory(httpClient);
    this.trackingClient = trackingClient;
    this.secretsRepositoryWriter = secretsRepositoryWriter;
    this.actorDefinitionVersionHelper = actorDefinitionVersionHelper;
    this.featureFlagClient = featureFlagClient;
    this.sourceService = sourceService;
    this.destinationService = destinationService;
    this.oAuthService = oauthService;
    this.secretPersistenceConfigService = secretPersistenceConfigService;
    this.workspaceService = workspaceService;
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
        configRepository.getStandardSourceDefinition(sourceOauthConsentRequest.getSourceDefinitionId());
    final ActorDefinitionVersion sourceVersion = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition,
        sourceOauthConsentRequest.getWorkspaceId(), sourceOauthConsentRequest.getSourceId());
    final OAuthFlowImplementation oAuthFlowImplementation = oAuthImplementationFactory.create(sourceVersion.getDockerRepository());
    final ConnectorSpecification spec = sourceVersion.getSpec();
    final Map<String, Object> metadata = TrackingMetadata.generateSourceDefinitionMetadata(sourceDefinition, sourceVersion);
    final OAuthConsentRead result;

    final JsonNode sourceOAuthParamConfig =
        getSourceOAuthParamConfig(sourceOauthConsentRequest.getWorkspaceId(), sourceDefinition.getSourceDefinitionId());

    if (OAuthConfigSupplier.hasOAuthConfigSpecification(spec)) {
      final JsonNode oAuthInputConfigurationForConsent;

      if (sourceOauthConsentRequest.getSourceId() == null) {
        oAuthInputConfigurationForConsent = sourceOauthConsentRequest.getoAuthInputConfiguration();
      } else {
        final SourceConnection hydratedSourceConnection;
        try {
          hydratedSourceConnection = sourceService.getSourceConnectionWithSecrets(sourceOauthConsentRequest.getSourceId());
        } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
          throw new ConfigNotFoundException(e.getType(), e.getConfigId());
        }

        oAuthInputConfigurationForConsent = getOAuthInputConfigurationForConsent(spec,
            hydratedSourceConnection.getConfiguration(),
            sourceOauthConsentRequest.getoAuthInputConfiguration());
      }

      result = new OAuthConsentRead().consentUrl(oAuthFlowImplementation.getSourceConsentUrl(
          sourceOauthConsentRequest.getWorkspaceId(),
          sourceOauthConsentRequest.getSourceDefinitionId(),
          sourceOauthConsentRequest.getRedirectUrl(),
          oAuthInputConfigurationForConsent,
          spec.getAdvancedAuth().getOauthConfigSpecification(), sourceOAuthParamConfig));
    } else {
      result = new OAuthConsentRead().consentUrl(oAuthFlowImplementation.getSourceConsentUrl(
          sourceOauthConsentRequest.getWorkspaceId(),
          sourceOauthConsentRequest.getSourceDefinitionId(),
          sourceOauthConsentRequest.getRedirectUrl(), Jsons.emptyObject(), null, sourceOAuthParamConfig));
    }
    try {
      trackingClient.track(sourceOauthConsentRequest.getWorkspaceId(), "Get Oauth Consent URL - Backend", metadata);
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
        configRepository.getStandardDestinationDefinition(destinationOauthConsentRequest.getDestinationDefinitionId());
    final ActorDefinitionVersion destinationVersion = actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition,
        destinationOauthConsentRequest.getWorkspaceId(), destinationOauthConsentRequest.getDestinationId());
    final OAuthFlowImplementation oAuthFlowImplementation = oAuthImplementationFactory.create(destinationVersion.getDockerRepository());
    final ConnectorSpecification spec = destinationVersion.getSpec();
    final Map<String, Object> metadata = TrackingMetadata.generateDestinationDefinitionMetadata(destinationDefinition, destinationVersion);
    final OAuthConsentRead result;

    final JsonNode destinationOAuthParamConfig = getDestinationOAuthParamConfig(
        destinationOauthConsentRequest.getWorkspaceId(), destinationOauthConsentRequest.getDestinationDefinitionId());

    if (OAuthConfigSupplier.hasOAuthConfigSpecification(spec)) {
      final JsonNode oAuthInputConfigurationForConsent;

      if (destinationOauthConsentRequest.getDestinationId() == null) {
        oAuthInputConfigurationForConsent = destinationOauthConsentRequest.getoAuthInputConfiguration();
      } else {
        final DestinationConnection hydratedSourceConnection;
        try {
          hydratedSourceConnection = destinationService.getDestinationConnectionWithSecrets(destinationOauthConsentRequest.getDestinationId());
        } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
          throw new ConfigNotFoundException(e.getType(), e.getConfigId());
        }

        oAuthInputConfigurationForConsent = getOAuthInputConfigurationForConsent(spec,
            hydratedSourceConnection.getConfiguration(),
            destinationOauthConsentRequest.getoAuthInputConfiguration());

      }

      result = new OAuthConsentRead().consentUrl(oAuthFlowImplementation.getDestinationConsentUrl(
          destinationOauthConsentRequest.getWorkspaceId(),
          destinationOauthConsentRequest.getDestinationDefinitionId(),
          destinationOauthConsentRequest.getRedirectUrl(),
          oAuthInputConfigurationForConsent,
          spec.getAdvancedAuth().getOauthConfigSpecification(), destinationOAuthParamConfig));
    } else {
      result = new OAuthConsentRead().consentUrl(oAuthFlowImplementation.getDestinationConsentUrl(
          destinationOauthConsentRequest.getWorkspaceId(),
          destinationOauthConsentRequest.getDestinationDefinitionId(),
          destinationOauthConsentRequest.getRedirectUrl(), Jsons.emptyObject(), null, destinationOAuthParamConfig));
    }
    try {
      trackingClient.track(destinationOauthConsentRequest.getWorkspaceId(), "Get Oauth Consent URL - Backend", metadata);
    } catch (final Exception e) {
      LOGGER.error(ERROR_MESSAGE, e);
    }
    return result;
  }

  public CompleteOAuthResponse completeSourceOAuthHandleReturnSecret(final CompleteSourceOauthRequest completeSourceOauthRequest)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final CompleteOAuthResponse oAuthTokens = completeSourceOAuth(completeSourceOauthRequest);
    if (oAuthTokens != null && completeSourceOauthRequest.getReturnSecretCoordinate()) {
      return writeOAuthResponseSecret(completeSourceOauthRequest.getWorkspaceId(), oAuthTokens);
    } else {
      return oAuthTokens;
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
        configRepository.getStandardSourceDefinition(completeSourceOauthRequest.getSourceDefinitionId());
    final ActorDefinitionVersion sourceVersion = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition,
        completeSourceOauthRequest.getWorkspaceId(), completeSourceOauthRequest.getSourceId());
    final OAuthFlowImplementation oAuthFlowImplementation = oAuthImplementationFactory.create(sourceVersion.getDockerRepository());
    final ConnectorSpecification spec = sourceVersion.getSpec();
    final Map<String, Object> metadata = TrackingMetadata.generateSourceDefinitionMetadata(sourceDefinition, sourceVersion);
    final Map<String, Object> result;

    final JsonNode sourceOAuthParamConfig =
        getSourceOAuthParamConfig(completeSourceOauthRequest.getWorkspaceId(), sourceDefinition.getSourceDefinitionId());
    if (OAuthConfigSupplier.hasOAuthConfigSpecification(spec)) {
      final JsonNode oAuthInputConfigurationForConsent;

      if (completeSourceOauthRequest.getSourceId() == null) {
        oAuthInputConfigurationForConsent = completeSourceOauthRequest.getoAuthInputConfiguration();
      } else {
        final SourceConnection hydratedSourceConnection;
        try {
          hydratedSourceConnection = sourceService.getSourceConnectionWithSecrets(completeSourceOauthRequest.getSourceId());
        } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
          throw new ConfigNotFoundException(e.getType(), e.getConfigId());
        }

        oAuthInputConfigurationForConsent = getOAuthInputConfigurationForConsent(spec,
            hydratedSourceConnection.getConfiguration(),
            completeSourceOauthRequest.getoAuthInputConfiguration());
      }

      result = oAuthFlowImplementation.completeSourceOAuth(
          completeSourceOauthRequest.getWorkspaceId(),
          completeSourceOauthRequest.getSourceDefinitionId(),
          completeSourceOauthRequest.getQueryParams(),
          completeSourceOauthRequest.getRedirectUrl(),
          oAuthInputConfigurationForConsent,
          spec.getAdvancedAuth().getOauthConfigSpecification(),
          sourceOAuthParamConfig);
    } else {
      // deprecated but this path is kept for connectors that don't define OAuth Spec yet
      result = oAuthFlowImplementation.completeSourceOAuth(
          completeSourceOauthRequest.getWorkspaceId(),
          completeSourceOauthRequest.getSourceDefinitionId(),
          completeSourceOauthRequest.getQueryParams(),
          completeSourceOauthRequest.getRedirectUrl(), sourceOAuthParamConfig);
    }
    try {
      trackingClient.track(completeSourceOauthRequest.getWorkspaceId(), "Complete OAuth Flow - Backend", metadata);
    } catch (final Exception e) {
      LOGGER.error(ERROR_MESSAGE, e);
    }
    return mapToCompleteOAuthResponse(result);
  }

  @SuppressWarnings("PMD.PreserveStackTrace")
  public CompleteOAuthResponse completeDestinationOAuth(final CompleteDestinationOAuthRequest completeDestinationOAuthRequest)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final Map<String, Object> traceTags = Map.of(WORKSPACE_ID_KEY, completeDestinationOAuthRequest.getWorkspaceId(), DESTINATION_DEFINITION_ID_KEY,
        completeDestinationOAuthRequest.getDestinationDefinitionId());
    ApmTraceUtils.addTagsToTrace(traceTags);
    ApmTraceUtils.addTagsToRootSpan(traceTags);

    final StandardDestinationDefinition destinationDefinition =
        configRepository.getStandardDestinationDefinition(completeDestinationOAuthRequest.getDestinationDefinitionId());
    final ActorDefinitionVersion destinationVersion = actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition,
        completeDestinationOAuthRequest.getWorkspaceId(), completeDestinationOAuthRequest.getDestinationId());
    final OAuthFlowImplementation oAuthFlowImplementation = oAuthImplementationFactory.create(destinationVersion.getDockerRepository());
    final ConnectorSpecification spec = destinationVersion.getSpec();
    final Map<String, Object> metadata = TrackingMetadata.generateDestinationDefinitionMetadata(destinationDefinition, destinationVersion);
    final Map<String, Object> result;

    final JsonNode destinationOAuthParamConfig = getDestinationOAuthParamConfig(
        completeDestinationOAuthRequest.getWorkspaceId(), completeDestinationOAuthRequest.getDestinationDefinitionId());

    if (OAuthConfigSupplier.hasOAuthConfigSpecification(spec)) {
      final JsonNode oAuthInputConfigurationForConsent;

      if (completeDestinationOAuthRequest.getDestinationId() == null) {
        oAuthInputConfigurationForConsent = completeDestinationOAuthRequest.getoAuthInputConfiguration();
      } else {
        final DestinationConnection hydratedSourceConnection;
        try {
          hydratedSourceConnection = destinationService.getDestinationConnectionWithSecrets(completeDestinationOAuthRequest.getDestinationId());
        } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
          throw new ConfigNotFoundException(e.getType(), e.getConfigId());
        }

        oAuthInputConfigurationForConsent = getOAuthInputConfigurationForConsent(spec,
            hydratedSourceConnection.getConfiguration(),
            completeDestinationOAuthRequest.getoAuthInputConfiguration());

      }

      result = oAuthFlowImplementation.completeDestinationOAuth(
          completeDestinationOAuthRequest.getWorkspaceId(),
          completeDestinationOAuthRequest.getDestinationDefinitionId(),
          completeDestinationOAuthRequest.getQueryParams(),
          completeDestinationOAuthRequest.getRedirectUrl(),
          oAuthInputConfigurationForConsent,
          spec.getAdvancedAuth().getOauthConfigSpecification(), destinationOAuthParamConfig);
    } else {
      // deprecated but this path is kept for connectors that don't define OAuth Spec yet
      result = oAuthFlowImplementation.completeDestinationOAuth(
          completeDestinationOAuthRequest.getWorkspaceId(),
          completeDestinationOAuthRequest.getDestinationDefinitionId(),
          completeDestinationOAuthRequest.getQueryParams(),
          completeDestinationOAuthRequest.getRedirectUrl(), destinationOAuthParamConfig);
    }
    try {
      trackingClient.track(completeDestinationOAuthRequest.getWorkspaceId(), "Complete OAuth Flow - Backend", metadata);
    } catch (final Exception e) {
      LOGGER.error(ERROR_MESSAGE, e);
    }
    return mapToCompleteOAuthResponse(result);
  }

  @SuppressWarnings("PMD.PreserveStackTrace")
  public void revokeSourceOauthTokens(final RevokeSourceOauthTokensRequest revokeSourceOauthTokensRequest)
      throws IOException, ConfigNotFoundException, JsonValidationException {
    final StandardSourceDefinition sourceDefinition =
        configRepository.getStandardSourceDefinition(revokeSourceOauthTokensRequest.getSourceDefinitionId());
    final ActorDefinitionVersion sourceVersion = actorDefinitionVersionHelper.getSourceVersion(sourceDefinition,
        revokeSourceOauthTokensRequest.getWorkspaceId(), revokeSourceOauthTokensRequest.getSourceId());
    final OAuthFlowImplementation oAuthFlowImplementation = oAuthImplementationFactory.create(sourceVersion.getDockerRepository());
    final SourceConnection hydratedSourceConnection;
    try {
      hydratedSourceConnection = sourceService.getSourceConnectionWithSecrets(
          revokeSourceOauthTokensRequest.getSourceId());
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getConfigId());
    }
    final JsonNode sourceOAuthParamConfig =
        getSourceOAuthParamConfig(revokeSourceOauthTokensRequest.getWorkspaceId(), revokeSourceOauthTokensRequest.getSourceDefinitionId());
    oAuthFlowImplementation.revokeSourceOauth(
        revokeSourceOauthTokensRequest.getWorkspaceId(),
        revokeSourceOauthTokensRequest.getSourceDefinitionId(),
        hydratedSourceConnection.getConfiguration(),
        sourceOAuthParamConfig);
  }

  public void setSourceInstancewideOauthParams(final SetInstancewideSourceOauthParamsRequestBody requestBody)
      throws JsonValidationException, IOException {
    final SourceOAuthParameter param = configRepository
        .getSourceOAuthParamByDefinitionIdOptional(null, requestBody.getSourceDefinitionId())
        .orElseGet(() -> new SourceOAuthParameter().withOauthParameterId(UUID.randomUUID()))
        .withConfiguration(Jsons.jsonNode(requestBody.getParams()))
        .withSourceDefinitionId(requestBody.getSourceDefinitionId());
    // TODO validate requestBody.getParams() against
    // spec.getAdvancedAuth().getOauthConfigSpecification().getCompleteOauthServerInputSpecification()
    configRepository.writeSourceOAuthParam(param);
  }

  public void setDestinationInstancewideOauthParams(final SetInstancewideDestinationOauthParamsRequestBody requestBody)
      throws JsonValidationException, IOException {
    final DestinationOAuthParameter param = configRepository
        .getDestinationOAuthParamByDefinitionIdOptional(null, requestBody.getDestinationDefinitionId())
        .orElseGet(() -> new DestinationOAuthParameter().withOauthParameterId(UUID.randomUUID()))
        .withConfiguration(Jsons.jsonNode(requestBody.getParams()))
        .withDestinationDefinitionId(requestBody.getDestinationDefinitionId());
    // TODO validate requestBody.getParams() against
    // spec.getAdvancedAuth().getOauthConfigSpecification().getCompleteOauthServerInputSpecification()
    configRepository.writeDestinationOAuthParam(param);
  }

  private JsonNode getOAuthInputConfigurationForConsent(final ConnectorSpecification spec,
                                                        final JsonNode hydratedSourceConnectionConfiguration,
                                                        final JsonNode oAuthInputConfiguration) {
    final Map<String, String> fieldsToGet =
        buildJsonPathFromOAuthFlowInitParameters(OAuthPathExtractor.extractOauthConfigurationPaths(
            spec.getAdvancedAuth().getOauthConfigSpecification().getOauthUserInputFromConnectorConfigSpecification()));

    final JsonNode oAuthInputConfigurationFromDB = getOAuthInputConfiguration(hydratedSourceConnectionConfiguration, fieldsToGet);

    return getOauthFromDBIfNeeded(oAuthInputConfigurationFromDB, oAuthInputConfiguration);
  }

  CompleteOAuthResponse mapToCompleteOAuthResponse(final Map<String, Object> input) {
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
      if (k != "request_succeeded" && k != "request_error") {
        response.getAuthPayload().put(k, v);
      }
    });

    return response;
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
  public CompleteOAuthResponse writeOAuthResponseSecret(final UUID workspaceId, final CompleteOAuthResponse payload)
      throws IOException, ConfigNotFoundException {

    try {
      final String payloadString = Jackson.getObjectMapper().writeValueAsString(payload);
      final Optional<UUID> organizationId = workspaceService.getOrganizationIdFromWorkspaceId(workspaceId);
      final SecretCoordinate secretCoordinate;
      if (organizationId.isPresent()
          && featureFlagClient.boolVariation(UseRuntimeSecretPersistence.INSTANCE, new Organization(organizationId.get()))) {
        try {
          final SecretPersistenceConfig secretPersistenceConfig =
              secretPersistenceConfigService.getSecretPersistenceConfig(ScopeType.ORGANIZATION, organizationId.get());
          secretCoordinate = secretsRepositoryWriter.storeSecretToRuntimeSecretPersistence(
              generateOAuthSecretCoordinate(workspaceId),
              payloadString,
              new RuntimeSecretPersistence(secretPersistenceConfig));
        } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
          throw new ConfigNotFoundException(e.getType(), e.getConfigId());
        }
      } else {
        secretCoordinate = secretsRepositoryWriter.storeSecretToDefaultSecretPersistence(
            generateOAuthSecretCoordinate(workspaceId),
            payloadString);
      }
      return mapToCompleteOAuthResponse(Map.of("secretId", secretCoordinate.getFullCoordinate()));

    } catch (final JsonProcessingException e) {
      throw new RuntimeException("Json object could not be written to string.", e);
    }
  }

  /**
   * Generate OAuthSecretCoordinates. Always assume V1 and do not support secret updates
   */
  private SecretCoordinate generateOAuthSecretCoordinate(final UUID workspaceId) {
    final String coordinateBase = SecretsHelpers.INSTANCE.getCoordinatorBase("airbyte_oauth_workspace_", workspaceId, UUID::randomUUID);
    return new SecretCoordinate(coordinateBase, 1);
  }

  /**
   * Sets workspace level overrides for OAuth parameters.
   *
   * @param requestBody request body
   */
  public void setWorkspaceOverrideOAuthParams(final WorkspaceOverrideOauthParamsRequestBody requestBody)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    switch (requestBody.getActorType()) {
      case SOURCE -> setSourceWorkspaceOverrideOauthParams(requestBody);
      case DESTINATION -> setDestinationWorkspaceOverrideOauthParams(requestBody);
      default -> throw new BadObjectSchemaKnownException("actorType must be one of ['source', 'destination']");
    }
  }

  public void setSourceWorkspaceOverrideOauthParams(final WorkspaceOverrideOauthParamsRequestBody requestBody)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    final UUID definitionId = requestBody.getDefinitionId();
    final StandardSourceDefinition standardSourceDefinition =
        configRepository.getStandardSourceDefinition(definitionId);

    final UUID workspaceId = requestBody.getWorkspaceId();
    final ActorDefinitionVersion actorDefinitionVersion =
        actorDefinitionVersionHelper.getSourceVersion(standardSourceDefinition, workspaceId);

    final ConnectorSpecification connectorSpecification = actorDefinitionVersion.getSpec();

    final JsonNode oauthParamConfiguration = Jsons.jsonNode(requestBody.getParams());

    final JsonNode sanitizedOauthConfiguration = sanitizeOauthConfiguration(workspaceId, connectorSpecification, oauthParamConfiguration);

    final SourceOAuthParameter param = configRepository
        .getSourceOAuthParamByDefinitionIdOptional(workspaceId, definitionId)
        .orElseGet(() -> new SourceOAuthParameter().withOauthParameterId(UUID.randomUUID()))
        .withConfiguration(sanitizedOauthConfiguration)
        .withSourceDefinitionId(definitionId)
        .withWorkspaceId(workspaceId);

    configRepository.writeSourceOAuthParam(param);
  }

  public void setDestinationWorkspaceOverrideOauthParams(final WorkspaceOverrideOauthParamsRequestBody requestBody)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    final UUID workspaceId = requestBody.getWorkspaceId();
    final UUID definitionId = requestBody.getDefinitionId();
    final StandardDestinationDefinition destinationDefinition =
        configRepository.getStandardDestinationDefinition(definitionId);
    final ActorDefinitionVersion actorDefinitionVersion =
        actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, workspaceId);

    final ConnectorSpecification connectorSpecification = actorDefinitionVersion.getSpec();

    final JsonNode oauthParamConfiguration = Jsons.jsonNode(requestBody.getParams());

    final JsonNode sanitizedOauthConfiguration = sanitizeOauthConfiguration(workspaceId, connectorSpecification, oauthParamConfiguration);

    final DestinationOAuthParameter param = configRepository
        .getDestinationOAuthParamByDefinitionIdOptional(workspaceId, definitionId)
        .orElseGet(() -> new DestinationOAuthParameter().withOauthParameterId(UUID.randomUUID()))
        .withConfiguration(sanitizedOauthConfiguration)
        .withDestinationDefinitionId(definitionId)
        .withWorkspaceId(workspaceId);

    configRepository.writeDestinationOAuthParam(param);
  }

  /**
   * Method to handle sanitizing OAuth param configuration. Secrets are split out and stored in the
   * secrets manager and a new ready-for-storage version of the oauth param config JSON will be
   * returned.
   *
   * @param workspaceId the current workspace ID
   * @param connectorSpecification the connector specification of the source/destination in question
   * @param oauthParamConfiguration the oauth param configuration passed in by the user.
   * @return new oauth param configuration to be stored to the db.
   * @throws JsonValidationException if oauth param configuration doesn't pass spec validation
   */
  private JsonNode sanitizeOauthConfiguration(final UUID workspaceId,
                                              final ConnectorSpecification connectorSpecification,
                                              final JsonNode oauthParamConfiguration)
      throws JsonValidationException, IOException, ConfigNotFoundException {

    if (OAuthConfigSupplier.hasOAuthConfigSpecification(connectorSpecification)) {
      // Advanced auth handling
      final ConnectorSpecification advancedAuthSpecification =
          validateOauthParamConfigAndReturnAdvancedAuthSecretSpec(connectorSpecification, oauthParamConfiguration);
      LOGGER.debug("AdvancedAuthSpecification: {}", advancedAuthSpecification);

      return statefulSplitSecrets(workspaceId, oauthParamConfiguration, advancedAuthSpecification);
    } else {
      // This works because:
      // 1. In non advanced_auth specs, the connector configuration matches the oauth param configuration,
      // the two are just merged together
      // 2. For these non advanced_auth specs, the actual variables are present and tagged as secrets so
      // statefulSplitSecrets can find and
      // store them in our secrets manager and replace the values appropriately.
      return statefulSplitSecrets(workspaceId, oauthParamConfiguration, connectorSpecification);
    }
  }

  @VisibleForTesting
  @SuppressWarnings("PMD.PreserveStackTrace")
  JsonNode getSourceOAuthParamConfig(final UUID workspaceId, final UUID sourceDefinitionId) throws IOException, ConfigNotFoundException {
    try {
      final SourceOAuthParameter param = oAuthService.getSourceOAuthParameterWithSecrets(workspaceId, sourceDefinitionId);
      // TODO: if we write a flyway migration to flatten persisted configs in db, we don't need to flatten
      // here see https://github.com/airbytehq/airbyte/issues/7624
      // Should already be hydrated.
      return MoreOAuthParameters.flattenOAuthConfig(param.getConfiguration());
    } catch (final JsonValidationException e) {
      throw new IOException("Failed to load OAuth Parameters", e);
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getConfigId());
    }
  }

  @VisibleForTesting
  @SuppressWarnings("PMD.PreserveStackTrace")
  JsonNode getDestinationOAuthParamConfig(final UUID workspaceId, final UUID destinationDefinitionId)
      throws IOException, ConfigNotFoundException {
    try {
      final DestinationOAuthParameter param =
          oAuthService.getDestinationOAuthParameterWithSecrets(workspaceId, destinationDefinitionId);
      // TODO: if we write a migration to flatten persisted configs in db, we don't need to flatten
      // here see https://github.com/airbytehq/airbyte/issues/7624
      // Should already be hydrated
      return MoreOAuthParameters.flattenOAuthConfig(param.getConfiguration());
    } catch (io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getConfigId());

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
    if (organizationId.isPresent() && featureFlagClient.boolVariation(UseRuntimeSecretPersistence.INSTANCE, new Organization(organizationId.get()))) {
      try {
        final SecretPersistenceConfig secretPersistenceConfig =
            secretPersistenceConfigService.getSecretPersistenceConfig(ScopeType.ORGANIZATION, organizationId.get());

        return secretsRepositoryWriter.statefulSplitSecretsToRuntimeSecretPersistence(
            workspaceId,
            oauthParamConfiguration,
            connectorSpecification,
            new RuntimeSecretPersistence(secretPersistenceConfig));
      } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
        throw new ConfigNotFoundException(e.getType(), e.getConfigId());
      }
    } else {
      return secretsRepositoryWriter.statefulSplitSecretsToDefaultSecretPersistence(workspaceId, oauthParamConfiguration, connectorSpecification);
    }
  }

}
