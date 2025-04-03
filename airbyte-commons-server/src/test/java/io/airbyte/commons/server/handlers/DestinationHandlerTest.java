/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import io.airbyte.api.model.generated.ActorStatus;
import io.airbyte.api.model.generated.ConnectionRead;
import io.airbyte.api.model.generated.ConnectionReadList;
import io.airbyte.api.model.generated.DestinationCreate;
import io.airbyte.api.model.generated.DestinationDefinitionSpecificationRead;
import io.airbyte.api.model.generated.DestinationIdRequestBody;
import io.airbyte.api.model.generated.DestinationRead;
import io.airbyte.api.model.generated.DestinationReadList;
import io.airbyte.api.model.generated.DestinationSearch;
import io.airbyte.api.model.generated.DestinationUpdate;
import io.airbyte.api.model.generated.ResourceRequirements;
import io.airbyte.api.model.generated.ScopedResourceRequirements;
import io.airbyte.api.model.generated.SupportState;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.api.problems.throwable.generated.LicenseEntitlementProblem;
import io.airbyte.commons.entitlements.Entitlement;
import io.airbyte.commons.entitlements.LicenseEntitlementChecker;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.commons.server.converters.ConfigurationUpdate;
import io.airbyte.commons.server.errors.BadRequestException;
import io.airbyte.commons.server.handlers.helpers.ActorDefinitionHandlerHelper;
import io.airbyte.commons.server.handlers.helpers.CatalogConverter;
import io.airbyte.commons.server.helpers.ConnectionHelpers;
import io.airbyte.commons.server.helpers.ConnectorSpecificationHelpers;
import io.airbyte.commons.server.helpers.DestinationHelpers;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.Configs;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.helpers.FieldGenerator;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper.ActorDefinitionVersionWithOverrideStatus;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.secrets.ConfigWithSecretReferences;
import io.airbyte.config.secrets.JsonSecretsProcessor;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.domain.models.SecretReferenceScopeType;
import io.airbyte.domain.services.secrets.SecretReferenceService;
import io.airbyte.featureflag.TestClient;
import io.airbyte.metrics.MetricClient;
import io.airbyte.persistence.job.WorkspaceHelper;
import io.airbyte.persistence.job.factory.OAuthConfigSupplier;
import io.airbyte.protocol.models.v0.ConnectorSpecification;
import io.airbyte.validation.json.JsonSchemaValidator;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DestinationHandlerTest {

  private StandardDestinationDefinition standardDestinationDefinition;
  private ActorDefinitionVersion destinationDefinitionVersion;
  private ActorDefinitionVersionWithOverrideStatus destinationDefinitionVersionWithOverrideStatus;
  private DestinationDefinitionSpecificationRead destinationDefinitionSpecificationRead;
  private DestinationConnection destinationConnection;
  private DestinationHandler destinationHandler;
  private ConnectionsHandler connectionsHandler;
  private ConfigurationUpdate configurationUpdate;
  private JsonSchemaValidator validator;
  private Supplier<UUID> uuidGenerator;
  private JsonSecretsProcessor secretsProcessor;
  private ConnectorSpecification connectorSpecification;
  private OAuthConfigSupplier oAuthConfigSupplier;
  private ActorDefinitionVersionHelper actorDefinitionVersionHelper;
  private ActorDefinitionVersionUpdater actorDefinitionVersionUpdater;
  private ActorDefinitionHandlerHelper actorDefinitionHandlerHelper;
  private LicenseEntitlementChecker licenseEntitlementChecker;
  private final ApiPojoConverters apiPojoConverters = new ApiPojoConverters(new CatalogConverter(new FieldGenerator(), Collections.emptyList()));

  private static final String API_KEY_FIELD = "apiKey";
  private static final String API_KEY_VALUE = "987-xyz";

  // needs to match name of file in src/test/resources/icons
  private static final String ICON_URL = "https://connectors.airbyte.com/files/metadata/airbyte/destination-test/latest/icon.svg";
  private static final Boolean IS_VERSION_OVERRIDE_APPLIED = true;
  private static final Boolean IS_ENTITLED = true;
  private static final SupportState SUPPORT_STATE = SupportState.SUPPORTED;
  private static final String DEFAULT_MEMORY = "2 GB";
  private static final String DEFAULT_CPU = "2";

  private static final ScopedResourceRequirements RESOURCE_ALLOCATION = getResourceRequirementsForDestinationRequest(DEFAULT_CPU, DEFAULT_MEMORY);
  private DestinationService destinationService;
  private WorkspaceHelper workspaceHelper;
  private TestClient featureFlagClient;
  private SecretsRepositoryWriter secretsRepositoryWriter;
  private MetricClient metricClient;
  private SecretPersistenceConfigService secretPersistenceConfigService;
  private SecretReferenceService secretReferenceService;

  @SuppressWarnings("unchecked")
  @BeforeEach
  void setUp() throws IOException, JsonValidationException, ConfigNotFoundException, io.airbyte.data.exceptions.ConfigNotFoundException {
    validator = mock(JsonSchemaValidator.class);
    uuidGenerator = mock(Supplier.class);
    connectionsHandler = mock(ConnectionsHandler.class);
    configurationUpdate = mock(ConfigurationUpdate.class);
    secretsProcessor = mock(JsonSecretsProcessor.class);
    oAuthConfigSupplier = mock(OAuthConfigSupplier.class);
    actorDefinitionVersionHelper = mock(ActorDefinitionVersionHelper.class);
    destinationService = mock(DestinationService.class);
    actorDefinitionHandlerHelper = mock(ActorDefinitionHandlerHelper.class);
    actorDefinitionVersionUpdater = mock(ActorDefinitionVersionUpdater.class);
    workspaceHelper = mock(WorkspaceHelper.class);
    licenseEntitlementChecker = mock(LicenseEntitlementChecker.class);
    featureFlagClient = mock(TestClient.class);
    secretsRepositoryWriter = mock(SecretsRepositoryWriter.class);
    metricClient = mock(MetricClient.class);
    secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);
    secretReferenceService = mock(SecretReferenceService.class);

    connectorSpecification = ConnectorSpecificationHelpers.generateConnectorSpecification();

    standardDestinationDefinition = new StandardDestinationDefinition()
        .withDestinationDefinitionId(UUID.randomUUID())
        .withName("db2")
        .withIconUrl(ICON_URL);

    destinationDefinitionVersion = new ActorDefinitionVersion()
        .withDockerImageTag("thelatesttag")
        .withSpec(connectorSpecification);

    destinationDefinitionVersionWithOverrideStatus = new ActorDefinitionVersionWithOverrideStatus(
        destinationDefinitionVersion, IS_VERSION_OVERRIDE_APPLIED);

    destinationDefinitionSpecificationRead = new DestinationDefinitionSpecificationRead()
        .connectionSpecification(connectorSpecification.getConnectionSpecification())
        .destinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .documentationUrl(connectorSpecification.getDocumentationUrl().toString());

    destinationConnection = DestinationHelpers.generateDestination(standardDestinationDefinition.getDestinationDefinitionId(),
        apiPojoConverters.scopedResourceReqsToInternal(RESOURCE_ALLOCATION));

    destinationHandler =
        new DestinationHandler(
            validator,
            connectionsHandler,
            uuidGenerator,
            secretsProcessor,
            configurationUpdate,
            oAuthConfigSupplier,
            actorDefinitionVersionHelper,
            destinationService,
            actorDefinitionHandlerHelper,
            actorDefinitionVersionUpdater,
            apiPojoConverters,
            workspaceHelper,
            licenseEntitlementChecker,
            Configs.AirbyteEdition.COMMUNITY,
            featureFlagClient,
            secretsRepositoryWriter,
            metricClient,
            secretPersistenceConfigService,
            secretReferenceService);

    when(actorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(standardDestinationDefinition, destinationConnection.getWorkspaceId(),
        destinationConnection.getDestinationId())).thenReturn(destinationDefinitionVersionWithOverrideStatus);

    when(workspaceHelper.getOrganizationForWorkspace(any())).thenReturn(UUID.randomUUID());

    when(licenseEntitlementChecker.checkEntitlement(any(), eq(Entitlement.DESTINATION_CONNECTOR), any())).thenReturn(IS_ENTITLED);
  }

  @Test
  void testCreateDestination()
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.data.exceptions.ConfigNotFoundException {
    when(uuidGenerator.get())
        .thenReturn(destinationConnection.getDestinationId());
    when(destinationService.getDestinationConnection(destinationConnection.getDestinationId()))
        .thenReturn(destinationConnection);
    when(destinationService.getStandardDestinationDefinition(standardDestinationDefinition.getDestinationDefinitionId()))
        .thenReturn(standardDestinationDefinition);
    when(actorDefinitionVersionHelper.getDestinationVersion(standardDestinationDefinition, destinationConnection.getWorkspaceId()))
        .thenReturn(destinationDefinitionVersion);
    when(oAuthConfigSupplier.maskDestinationOAuthParameters(destinationDefinitionSpecificationRead.getDestinationDefinitionId(),
        destinationConnection.getWorkspaceId(),
        destinationConnection.getConfiguration(),
        destinationDefinitionVersion.getSpec())).thenReturn(destinationConnection.getConfiguration());
    when(secretsProcessor.prepareSecretsForOutput(destinationConnection.getConfiguration(),
        destinationDefinitionSpecificationRead.getConnectionSpecification()))
            .thenReturn(destinationConnection.getConfiguration());
    when(secretsRepositoryWriter.createFromConfig(destinationConnection.getWorkspaceId(), destinationConnection.getConfiguration(),
        destinationDefinitionVersion.getSpec().getConnectionSpecification(), null))
            .thenReturn(destinationConnection.getConfiguration());
    when(secretReferenceService.getConfigWithSecretReferences(eq(SecretReferenceScopeType.ACTOR), any(), any()))
        .thenAnswer(i -> new ConfigWithSecretReferences(i.getArgument(2), Map.of()));

    final DestinationCreate destinationCreate = new DestinationCreate()
        .name(destinationConnection.getName())
        .workspaceId(destinationConnection.getWorkspaceId())
        .destinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .connectionConfiguration(DestinationHelpers.getTestDestinationJson())
        .resourceAllocation(RESOURCE_ALLOCATION);

    final DestinationRead actualDestinationRead =
        destinationHandler.createDestination(destinationCreate);

    final DestinationRead expectedDestinationRead = new DestinationRead()
        .name(destinationConnection.getName())
        .destinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .workspaceId(destinationConnection.getWorkspaceId())
        .destinationId(destinationConnection.getDestinationId())
        .connectionConfiguration(DestinationHelpers.getTestDestinationJson())
        .destinationName(standardDestinationDefinition.getName())
        .icon(ICON_URL)
        .isEntitled(IS_ENTITLED)
        .isVersionOverrideApplied(IS_VERSION_OVERRIDE_APPLIED)
        .supportState(SUPPORT_STATE)
        .resourceAllocation(RESOURCE_ALLOCATION);

    assertEquals(expectedDestinationRead, actualDestinationRead);

    verify(validator).ensure(destinationDefinitionSpecificationRead.getConnectionSpecification(), destinationCreate.getConnectionConfiguration());
    verify(destinationService).writeDestinationConnectionNoSecrets(destinationConnection);
    verify(oAuthConfigSupplier).maskDestinationOAuthParameters(destinationDefinitionSpecificationRead.getDestinationDefinitionId(),
        destinationConnection.getWorkspaceId(), destinationConnection.getConfiguration(), destinationDefinitionVersion.getSpec());
    verify(actorDefinitionVersionHelper).getDestinationVersion(standardDestinationDefinition, destinationConnection.getWorkspaceId());
    verify(secretsProcessor)
        .prepareSecretsForOutput(destinationConnection.getConfiguration(), destinationDefinitionSpecificationRead.getConnectionSpecification());
  }

  @Test
  void testCreateDestinationNoEntitlementThrows()
      throws JsonValidationException, IOException, io.airbyte.data.exceptions.ConfigNotFoundException {
    when(uuidGenerator.get())
        .thenReturn(destinationConnection.getDestinationId());
    when(destinationService.getDestinationConnection(destinationConnection.getDestinationId()))
        .thenReturn(destinationConnection);
    when(destinationService.getStandardDestinationDefinition(standardDestinationDefinition.getDestinationDefinitionId()))
        .thenReturn(standardDestinationDefinition);
    when(actorDefinitionVersionHelper.getDestinationVersion(standardDestinationDefinition, destinationConnection.getWorkspaceId()))
        .thenReturn(destinationDefinitionVersion);

    final DestinationCreate destinationCreate = new DestinationCreate()
        .name(destinationConnection.getName())
        .workspaceId(destinationConnection.getWorkspaceId())
        .destinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .connectionConfiguration(DestinationHelpers.getTestDestinationJson())
        .resourceAllocation(RESOURCE_ALLOCATION);

    // Not entitled
    doThrow(new LicenseEntitlementProblem())
        .when(licenseEntitlementChecker)
        .ensureEntitled(any(), eq(Entitlement.DESTINATION_CONNECTOR), eq(standardDestinationDefinition.getDestinationDefinitionId()));

    assertThrows(LicenseEntitlementProblem.class, () -> destinationHandler.createDestination(destinationCreate));

    verify(validator).ensure(destinationDefinitionSpecificationRead.getConnectionSpecification(), destinationConnection.getConfiguration());
    verify(actorDefinitionVersionHelper).getDestinationVersion(standardDestinationDefinition, destinationConnection.getWorkspaceId());
  }

  @Test
  void testNonNullCreateDestinationThrowsOnInvalidResourceAllocation()
      throws IOException {
    DestinationHandler cloudDestinationHandler =
        new DestinationHandler(
            validator,
            connectionsHandler,
            uuidGenerator,
            secretsProcessor,
            configurationUpdate,
            oAuthConfigSupplier,
            actorDefinitionVersionHelper,
            destinationService,
            actorDefinitionHandlerHelper,
            actorDefinitionVersionUpdater,
            apiPojoConverters,
            workspaceHelper,
            licenseEntitlementChecker,
            Configs.AirbyteEdition.CLOUD,
            featureFlagClient,
            secretsRepositoryWriter,
            metricClient,
            secretPersistenceConfigService,
            secretReferenceService);

    final DestinationCreate destinationCreate = new DestinationCreate()
        .name(destinationConnection.getName())
        .workspaceId(destinationConnection.getWorkspaceId())
        .destinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .connectionConfiguration(DestinationHelpers.getTestDestinationJson())
        .resourceAllocation(RESOURCE_ALLOCATION);

    Assertions.assertThrows(
        BadRequestException.class,
        () -> cloudDestinationHandler.createDestination(destinationCreate),
        "Expected createDestination to throw BadRequestException");
  }

  public static ScopedResourceRequirements getResourceRequirementsForDestinationRequest(final String defaultCpuRequest,
                                                                                        final String defaultMemoryRequest) {
    return new ScopedResourceRequirements()._default(new ResourceRequirements().cpuRequest(defaultCpuRequest).memoryRequest(defaultMemoryRequest));
  }

  @Test
  void testUpdateDestination()
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final String updatedDestName = "my updated dest name";
    final JsonNode newConfiguration = destinationConnection.getConfiguration();
    ((ObjectNode) newConfiguration).put(API_KEY_FIELD, API_KEY_VALUE);
    final ScopedResourceRequirements newResourceAllocation = getResourceRequirementsForDestinationRequest("3", "3 GB");
    final DestinationUpdate destinationUpdate = new DestinationUpdate()
        .name(updatedDestName)
        .destinationId(destinationConnection.getDestinationId())
        .connectionConfiguration(newConfiguration)
        .resourceAllocation(newResourceAllocation);

    final DestinationConnection expectedDestinationConnection = Jsons.clone(destinationConnection)
        .withName(updatedDestName)
        .withConfiguration(newConfiguration)
        .withTombstone(false)
        .withResourceRequirements(apiPojoConverters.scopedResourceReqsToInternal(newResourceAllocation));

    when(secretsProcessor
        .copySecrets(destinationConnection.getConfiguration(), newConfiguration, destinationDefinitionSpecificationRead.getConnectionSpecification()))
            .thenReturn(newConfiguration);
    when(secretsProcessor.prepareSecretsForOutput(newConfiguration, destinationDefinitionSpecificationRead.getConnectionSpecification()))
        .thenReturn(newConfiguration);
    when(oAuthConfigSupplier.maskDestinationOAuthParameters(destinationDefinitionSpecificationRead.getDestinationDefinitionId(),
        destinationConnection.getWorkspaceId(),
        newConfiguration, destinationDefinitionVersion.getSpec())).thenReturn(newConfiguration);
    when(destinationService.getStandardDestinationDefinition(standardDestinationDefinition.getDestinationDefinitionId()))
        .thenReturn(standardDestinationDefinition);
    when(actorDefinitionVersionHelper.getDestinationVersion(standardDestinationDefinition, destinationConnection.getWorkspaceId(),
        destinationConnection.getDestinationId()))
            .thenReturn(destinationDefinitionVersion);
    when(destinationService.getDestinationDefinitionFromDestination(destinationConnection.getDestinationId()))
        .thenReturn(standardDestinationDefinition);
    when(destinationService.getDestinationConnection(destinationConnection.getDestinationId()))
        .thenReturn(expectedDestinationConnection);
    when(configurationUpdate.destination(destinationConnection.getDestinationId(), updatedDestName, newConfiguration))
        .thenReturn(expectedDestinationConnection);
    when(destinationService.getDestinationConnectionIfExists(destinationConnection.getDestinationId()))
        .thenReturn(Optional.of(destinationConnection));
    when(secretsRepositoryWriter.updateFromConfig(destinationConnection.getWorkspaceId(), destinationConnection.getConfiguration(),
        destinationConnection.getConfiguration(), destinationDefinitionVersion.getSpec().getConnectionSpecification(), null))
            .thenReturn(destinationConnection.getConfiguration());
    when(secretReferenceService.getConfigWithSecretReferences(eq(SecretReferenceScopeType.ACTOR), any(), any()))
        .thenAnswer(i -> new ConfigWithSecretReferences(i.getArgument(2), Map.of()));

    final DestinationRead actualDestinationRead = destinationHandler.updateDestination(destinationUpdate);

    final DestinationRead expectedDestinationRead = DestinationHelpers
        .getDestinationRead(expectedDestinationConnection, standardDestinationDefinition, IS_VERSION_OVERRIDE_APPLIED, IS_ENTITLED, SUPPORT_STATE,
            newResourceAllocation)
        .connectionConfiguration(newConfiguration);

    assertEquals(expectedDestinationRead, actualDestinationRead);

    verify(secretsProcessor).prepareSecretsForOutput(newConfiguration, destinationDefinitionSpecificationRead.getConnectionSpecification());
    verify(destinationService).writeDestinationConnectionNoSecrets(expectedDestinationConnection);
    verify(oAuthConfigSupplier).maskDestinationOAuthParameters(destinationDefinitionSpecificationRead.getDestinationDefinitionId(),
        destinationConnection.getWorkspaceId(), newConfiguration, destinationDefinitionVersion.getSpec());
    verify(actorDefinitionVersionHelper).getDestinationVersion(standardDestinationDefinition, destinationConnection.getWorkspaceId(),
        destinationConnection.getDestinationId());
    verify(validator).ensure(destinationDefinitionSpecificationRead.getConnectionSpecification(), newConfiguration);
  }

  @Test
  void testUpdateDestinationNoEntitlementThrows()
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final String updatedDestName = "my updated dest name";
    final JsonNode newConfiguration = destinationConnection.getConfiguration();
    ((ObjectNode) newConfiguration).put(API_KEY_FIELD, API_KEY_VALUE);
    final ScopedResourceRequirements newResourceAllocation = getResourceRequirementsForDestinationRequest("3", "3 GB");
    final DestinationUpdate destinationUpdate = new DestinationUpdate()
        .name(updatedDestName)
        .destinationId(destinationConnection.getDestinationId())
        .connectionConfiguration(newConfiguration)
        .resourceAllocation(newResourceAllocation);

    final DestinationConnection expectedDestinationConnection = Jsons.clone(destinationConnection)
        .withName(updatedDestName)
        .withConfiguration(newConfiguration)
        .withTombstone(false)
        .withResourceRequirements(apiPojoConverters.scopedResourceReqsToInternal(newResourceAllocation));

    when(secretsProcessor
        .copySecrets(destinationConnection.getConfiguration(), newConfiguration, destinationDefinitionSpecificationRead.getConnectionSpecification()))
            .thenReturn(newConfiguration);
    when(destinationService.getStandardDestinationDefinition(standardDestinationDefinition.getDestinationDefinitionId()))
        .thenReturn(standardDestinationDefinition);
    when(actorDefinitionVersionHelper.getDestinationVersion(standardDestinationDefinition, destinationConnection.getWorkspaceId(),
        destinationConnection.getDestinationId()))
            .thenReturn(destinationDefinitionVersion);
    when(destinationService.getDestinationDefinitionFromDestination(destinationConnection.getDestinationId()))
        .thenReturn(standardDestinationDefinition);
    when(destinationService.getDestinationConnection(destinationConnection.getDestinationId()))
        .thenReturn(expectedDestinationConnection);
    when(configurationUpdate.destination(destinationConnection.getDestinationId(), updatedDestName, newConfiguration))
        .thenReturn(expectedDestinationConnection);

    // Not entitled
    doThrow(new LicenseEntitlementProblem())
        .when(licenseEntitlementChecker)
        .ensureEntitled(any(), eq(Entitlement.DESTINATION_CONNECTOR), eq(standardDestinationDefinition.getDestinationDefinitionId()));

    assertThrows(LicenseEntitlementProblem.class, () -> destinationHandler.updateDestination(destinationUpdate));

    verify(actorDefinitionVersionHelper).getDestinationVersion(standardDestinationDefinition, destinationConnection.getWorkspaceId(),
        destinationConnection.getDestinationId());
    verify(validator).ensure(destinationDefinitionSpecificationRead.getConnectionSpecification(), newConfiguration);
  }

  @Test
  void testNonNullUpdateDestinationThrowsOnInvalidResourceAllocation() {
    DestinationHandler cloudDestinationHandler =
        new DestinationHandler(
            validator,
            connectionsHandler,
            uuidGenerator,
            secretsProcessor,
            configurationUpdate,
            oAuthConfigSupplier,
            actorDefinitionVersionHelper,
            destinationService,
            actorDefinitionHandlerHelper,
            actorDefinitionVersionUpdater,
            apiPojoConverters,
            workspaceHelper,
            licenseEntitlementChecker,
            Configs.AirbyteEdition.CLOUD,
            featureFlagClient,
            secretsRepositoryWriter,
            metricClient,
            secretPersistenceConfigService,
            secretReferenceService);

    final String updatedDestName = "my updated dest name";
    final JsonNode newConfiguration = destinationConnection.getConfiguration();
    ((ObjectNode) newConfiguration).put(API_KEY_FIELD, API_KEY_VALUE);
    final ScopedResourceRequirements newResourceAllocation = getResourceRequirementsForDestinationRequest("3", "3 GB");
    final DestinationUpdate destinationUpdate = new DestinationUpdate()
        .name(updatedDestName)
        .destinationId(destinationConnection.getDestinationId())
        .connectionConfiguration(newConfiguration)
        .resourceAllocation(newResourceAllocation);

    Assertions.assertThrows(
        BadRequestException.class,
        () -> cloudDestinationHandler.updateDestination(destinationUpdate),
        "Expected updateDestination to throw BadRequestException");
  }

  @Test
  void testUpgradeDestinationVersion()
      throws IOException, JsonValidationException, ConfigNotFoundException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final DestinationIdRequestBody requestBody = new DestinationIdRequestBody().destinationId(destinationConnection.getDestinationId());

    when(destinationService.getDestinationConnection(destinationConnection.getDestinationId()))
        .thenReturn(destinationConnection);
    when(destinationService.getStandardDestinationDefinition(standardDestinationDefinition.getDestinationDefinitionId()))
        .thenReturn(standardDestinationDefinition);

    destinationHandler.upgradeDestinationVersion(requestBody);

    // validate that we call the actorDefinitionVersionUpdater to upgrade the version to global default
    verify(actorDefinitionVersionUpdater).upgradeActorVersion(destinationConnection, standardDestinationDefinition);
  }

  @Test
  void testGetDestination() throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final DestinationRead expectedDestinationRead = new DestinationRead()
        .name(destinationConnection.getName())
        .destinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .workspaceId(destinationConnection.getWorkspaceId())
        .destinationId(destinationConnection.getDestinationId())
        .connectionConfiguration(destinationConnection.getConfiguration())
        .destinationName(standardDestinationDefinition.getName())
        .icon(ICON_URL)
        .isEntitled(IS_ENTITLED)
        .isVersionOverrideApplied(IS_VERSION_OVERRIDE_APPLIED)
        .supportState(SUPPORT_STATE)
        .resourceAllocation(RESOURCE_ALLOCATION);
    final DestinationIdRequestBody destinationIdRequestBody =
        new DestinationIdRequestBody().destinationId(expectedDestinationRead.getDestinationId());

    when(secretsProcessor.prepareSecretsForOutput(destinationConnection.getConfiguration(),
        destinationDefinitionSpecificationRead.getConnectionSpecification()))
            .thenReturn(destinationConnection.getConfiguration());
    when(destinationService.getDestinationConnection(destinationConnection.getDestinationId())).thenReturn(destinationConnection);
    when(destinationService.getStandardDestinationDefinition(standardDestinationDefinition.getDestinationDefinitionId()))
        .thenReturn(standardDestinationDefinition);
    when(actorDefinitionVersionHelper.getDestinationVersion(standardDestinationDefinition, destinationConnection.getWorkspaceId(),
        destinationConnection.getDestinationId()))
            .thenReturn(destinationDefinitionVersion);
    when(secretReferenceService.getConfigWithSecretReferences(eq(SecretReferenceScopeType.ACTOR), any(), any()))
        .thenAnswer(i -> new ConfigWithSecretReferences(i.getArgument(2), Map.of()));

    final DestinationRead actualDestinationRead = destinationHandler.getDestination(destinationIdRequestBody);

    assertEquals(expectedDestinationRead, actualDestinationRead);

    // make sure the icon was loaded into actual svg content
    assertTrue(expectedDestinationRead.getIcon().startsWith("https://"));

    verify(actorDefinitionVersionHelper).getDestinationVersion(standardDestinationDefinition, destinationConnection.getWorkspaceId(),
        destinationConnection.getDestinationId());
    verify(secretsProcessor)
        .prepareSecretsForOutput(destinationConnection.getConfiguration(), destinationDefinitionSpecificationRead.getConnectionSpecification());
  }

  @Test
  void testListDestinationForWorkspace()
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final DestinationRead expectedDestinationRead = new DestinationRead()
        .name(destinationConnection.getName())
        .destinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .workspaceId(destinationConnection.getWorkspaceId())
        .destinationId(destinationConnection.getDestinationId())
        .connectionConfiguration(destinationConnection.getConfiguration())
        .destinationName(standardDestinationDefinition.getName())
        .icon(ICON_URL)
        .isEntitled(IS_ENTITLED)
        .isVersionOverrideApplied(IS_VERSION_OVERRIDE_APPLIED)
        .supportState(SUPPORT_STATE)
        .status(ActorStatus.INACTIVE)
        .resourceAllocation(RESOURCE_ALLOCATION);
    final WorkspaceIdRequestBody workspaceIdRequestBody = new WorkspaceIdRequestBody().workspaceId(destinationConnection.getWorkspaceId());

    when(destinationService.getDestinationConnection(destinationConnection.getDestinationId())).thenReturn(destinationConnection);
    when(destinationService.listWorkspaceDestinationConnection(destinationConnection.getWorkspaceId()))
        .thenReturn(Lists.newArrayList(destinationConnection));
    when(destinationService.getStandardDestinationDefinition(standardDestinationDefinition.getDestinationDefinitionId()))
        .thenReturn(standardDestinationDefinition);
    when(actorDefinitionVersionHelper.getDestinationVersion(standardDestinationDefinition, destinationConnection.getWorkspaceId(),
        destinationConnection.getDestinationId()))
            .thenReturn(destinationDefinitionVersion);
    when(secretsProcessor.prepareSecretsForOutput(destinationConnection.getConfiguration(),
        destinationDefinitionSpecificationRead.getConnectionSpecification()))
            .thenReturn(destinationConnection.getConfiguration());
    when(secretReferenceService.getConfigWithSecretReferences(eq(SecretReferenceScopeType.ACTOR), any(), any()))
        .thenAnswer(i -> new ConfigWithSecretReferences(i.getArgument(2), Map.of()));

    final DestinationReadList actualDestinationRead = destinationHandler.listDestinationsForWorkspace(workspaceIdRequestBody);

    assertEquals(expectedDestinationRead, actualDestinationRead.getDestinations().get(0));
    verify(actorDefinitionVersionHelper).getDestinationVersion(standardDestinationDefinition, destinationConnection.getWorkspaceId(),
        destinationConnection.getDestinationId());
    verify(secretsProcessor)
        .prepareSecretsForOutput(destinationConnection.getConfiguration(), destinationDefinitionSpecificationRead.getConnectionSpecification());
  }

  @Test
  void testDeleteDestinationAndDeleteSecrets()
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final JsonNode newConfiguration = destinationConnection.getConfiguration();
    ((ObjectNode) newConfiguration).put(API_KEY_FIELD, API_KEY_VALUE);

    final DestinationConnection expectedSourceConnection = Jsons.clone(destinationConnection).withTombstone(true);

    final DestinationIdRequestBody destinationIdRequestBody = new DestinationIdRequestBody().destinationId(destinationConnection.getDestinationId());
    final StandardSync standardSync = ConnectionHelpers.generateSyncWithDestinationId(destinationConnection.getDestinationId());
    standardSync.setBreakingChange(false);
    final ConnectionRead connectionRead = ConnectionHelpers.generateExpectedConnectionRead(standardSync);
    final ConnectionReadList connectionReadList = new ConnectionReadList().connections(Collections.singletonList(connectionRead));
    final WorkspaceIdRequestBody workspaceIdRequestBody = new WorkspaceIdRequestBody().workspaceId(destinationConnection.getWorkspaceId());

    when(destinationService.getDestinationConnection(destinationConnection.getDestinationId()))
        .thenReturn(destinationConnection)
        .thenReturn(expectedSourceConnection);
    when(oAuthConfigSupplier.maskSourceOAuthParameters(destinationDefinitionSpecificationRead.getDestinationDefinitionId(),
        destinationConnection.getWorkspaceId(),
        newConfiguration, destinationDefinitionVersion.getSpec())).thenReturn(newConfiguration);
    when(destinationService.getStandardDestinationDefinition(destinationDefinitionSpecificationRead.getDestinationDefinitionId()))
        .thenReturn(standardDestinationDefinition);
    when(actorDefinitionVersionHelper.getDestinationVersion(standardDestinationDefinition, destinationConnection.getWorkspaceId(),
        destinationConnection.getDestinationId()))
            .thenReturn(destinationDefinitionVersion);
    when(destinationService.getDestinationDefinitionFromDestination(destinationConnection.getDestinationId()))
        .thenReturn(standardDestinationDefinition);
    when(connectionsHandler.listConnectionsForWorkspace(workspaceIdRequestBody)).thenReturn(connectionReadList);
    when(
        secretsProcessor.prepareSecretsForOutput(destinationConnection.getConfiguration(),
            destinationDefinitionSpecificationRead.getConnectionSpecification()))
                .thenReturn(destinationConnection.getConfiguration());
    when(actorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(standardDestinationDefinition, destinationConnection.getWorkspaceId(),
        destinationConnection.getDestinationId())).thenReturn(destinationDefinitionVersionWithOverrideStatus);
    when(secretReferenceService.getConfigWithSecretReferences(eq(SecretReferenceScopeType.ACTOR), any(), any()))
        .thenAnswer(i -> new ConfigWithSecretReferences(i.getArgument(2), Map.of()));

    destinationHandler.deleteDestination(destinationIdRequestBody);

    // We should not no longer get secrets or write secrets anymore (since we are deleting the
    // destination).
    verify(destinationService, times(0)).writeDestinationConnectionNoSecrets(expectedSourceConnection);
    verify(destinationService).tombstoneDestination(any(), any(), any(), any());
    verify(connectionsHandler).listConnectionsForWorkspace(workspaceIdRequestBody);
    verify(connectionsHandler).deleteConnection(connectionRead.getConnectionId());
  }

  @Test
  void testSearchDestinations()
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final DestinationRead expectedDestinationRead = new DestinationRead()
        .name(destinationConnection.getName())
        .destinationDefinitionId(standardDestinationDefinition.getDestinationDefinitionId())
        .workspaceId(destinationConnection.getWorkspaceId())
        .destinationId(destinationConnection.getDestinationId())
        .connectionConfiguration(destinationConnection.getConfiguration())
        .destinationName(standardDestinationDefinition.getName())
        .icon(ICON_URL)
        .isEntitled(IS_ENTITLED)
        .isVersionOverrideApplied(IS_VERSION_OVERRIDE_APPLIED)
        .supportState(SUPPORT_STATE)
        .resourceAllocation(RESOURCE_ALLOCATION);

    when(destinationService.getDestinationConnection(destinationConnection.getDestinationId())).thenReturn(destinationConnection);
    when(destinationService.listDestinationConnection()).thenReturn(Lists.newArrayList(destinationConnection));
    when(destinationService.getStandardDestinationDefinition(standardDestinationDefinition.getDestinationDefinitionId()))
        .thenReturn(standardDestinationDefinition);
    when(actorDefinitionVersionHelper.getDestinationVersion(standardDestinationDefinition, destinationConnection.getWorkspaceId(),
        destinationConnection.getDestinationId()))
            .thenReturn(destinationDefinitionVersion);
    when(secretsProcessor.prepareSecretsForOutput(destinationConnection.getConfiguration(),
        destinationDefinitionSpecificationRead.getConnectionSpecification()))
            .thenReturn(destinationConnection.getConfiguration());
    when(secretReferenceService.getConfigWithSecretReferences(eq(SecretReferenceScopeType.ACTOR), any(), any()))
        .thenAnswer(i -> new ConfigWithSecretReferences(i.getArgument(2), Map.of()));

    final DestinationSearch validDestinationSearch = new DestinationSearch().name(destinationConnection.getName());
    DestinationReadList actualDestinationRead = destinationHandler.searchDestinations(validDestinationSearch);
    assertEquals(1, actualDestinationRead.getDestinations().size());
    assertEquals(expectedDestinationRead, actualDestinationRead.getDestinations().get(0));
    verify(secretsProcessor)
        .prepareSecretsForOutput(destinationConnection.getConfiguration(), destinationDefinitionSpecificationRead.getConnectionSpecification());

    final DestinationSearch invalidDestinationSearch = new DestinationSearch().name("invalid");
    actualDestinationRead = destinationHandler.searchDestinations(invalidDestinationSearch);
    assertEquals(0, actualDestinationRead.getDestinations().size());
  }

}
