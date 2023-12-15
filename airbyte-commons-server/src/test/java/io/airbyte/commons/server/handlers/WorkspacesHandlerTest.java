/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import io.airbyte.analytics.TrackingClient;
import io.airbyte.api.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.model.generated.ConnectionRead;
import io.airbyte.api.model.generated.ConnectionReadList;
import io.airbyte.api.model.generated.DestinationRead;
import io.airbyte.api.model.generated.DestinationReadList;
import io.airbyte.api.model.generated.ListWorkspacesInOrganizationRequestBody;
import io.airbyte.api.model.generated.Pagination;
import io.airbyte.api.model.generated.SlugRequestBody;
import io.airbyte.api.model.generated.SourceRead;
import io.airbyte.api.model.generated.SourceReadList;
import io.airbyte.api.model.generated.WebhookConfigRead;
import io.airbyte.api.model.generated.WebhookConfigWrite;
import io.airbyte.api.model.generated.WorkspaceCreate;
import io.airbyte.api.model.generated.WorkspaceCreateWithId;
import io.airbyte.api.model.generated.WorkspaceGiveFeedback;
import io.airbyte.api.model.generated.WorkspaceIdRequestBody;
import io.airbyte.api.model.generated.WorkspaceOrganizationInfoRead;
import io.airbyte.api.model.generated.WorkspaceRead;
import io.airbyte.api.model.generated.WorkspaceReadList;
import io.airbyte.api.model.generated.WorkspaceUpdate;
import io.airbyte.api.model.generated.WorkspaceUpdateName;
import io.airbyte.api.model.generated.WorkspaceUpdateOrganization;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.converters.NotificationConverter;
import io.airbyte.commons.server.converters.NotificationSettingsConverter;
import io.airbyte.config.Geography;
import io.airbyte.config.Notification;
import io.airbyte.config.Notification.NotificationType;
import io.airbyte.config.NotificationItem;
import io.airbyte.config.NotificationSettings;
import io.airbyte.config.Organization;
import io.airbyte.config.SlackNotificationConfiguration;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.persistence.ConfigRepository.ResourcesByOrganizationQueryPaginated;
import io.airbyte.config.persistence.OrganizationPersistence;
import io.airbyte.config.persistence.PermissionPersistence;
import io.airbyte.config.persistence.WorkspacePersistence;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.config.secrets.persistence.SecretPersistence;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;

class WorkspacesHandlerTest {

  public static final String UPDATED = "updated";
  private static final String FAILURE_NOTIFICATION_WEBHOOK = "http://airbyte.notifications/failure";
  private static final String NEW_WORKSPACE = "new workspace";
  private static final String TEST_NAME = "test-name";
  private static final UUID ORGANIZATION_ID = UUID.randomUUID();
  private static final String TEST_AUTH_TOKEN = "test-auth-token";
  private static final UUID WEBHOOK_CONFIG_ID = UUID.randomUUID();
  private static final JsonNode PERSISTED_WEBHOOK_CONFIGS = Jsons.deserialize(
      String.format("{\"webhookConfigs\": [{\"id\": \"%s\", \"name\": \"%s\", \"authToken\": {\"_secret\": \"a-secret_v1\"}}]}",
          WEBHOOK_CONFIG_ID, TEST_NAME));

  private static final JsonNode SECRET_WEBHOOK_CONFIGS = Jsons.deserialize(
      String.format("{\"webhookConfigs\": [{\"id\": \"%s\", \"name\": \"%s\", \"authToken\": \"%s\"}]}",
          WEBHOOK_CONFIG_ID, TEST_NAME, TEST_AUTH_TOKEN));
  private static final String TEST_EMAIL = "test@airbyte.io";
  private static final String TEST_WORKSPACE_NAME = "test workspace";
  private static final String TEST_WORKSPACE_SLUG = "test-workspace";
  private static final String TEST_ORGANIZATION_NAME = "test organization";
  private static final io.airbyte.api.model.generated.Geography GEOGRAPHY_AUTO =
      io.airbyte.api.model.generated.Geography.AUTO;
  private static final io.airbyte.api.model.generated.Geography GEOGRAPHY_US =
      io.airbyte.api.model.generated.Geography.US;
  private ConfigRepository configRepository;
  private SecretsRepositoryWriter secretsRepositoryWriter;
  private ConnectionsHandler connectionsHandler;
  private DestinationHandler destinationHandler;
  private SourceHandler sourceHandler;
  private Supplier<UUID> uuidSupplier;
  private StandardWorkspace workspace;
  private WorkspacesHandler workspacesHandler;
  private SecretPersistence secretPersistence;

  private PermissionPersistence permissionPersistence;
  private WorkspacePersistence workspacePersistence;
  private WorkspaceService workspaceService;
  private OrganizationPersistence organizationPersistence;
  private TrackingClient trackingClient;

  @SuppressWarnings("unchecked")
  @BeforeEach
  void setUp() {
    configRepository = mock(ConfigRepository.class);
    workspacePersistence = mock(WorkspacePersistence.class);
    organizationPersistence = mock(OrganizationPersistence.class);
    secretPersistence = mock(SecretPersistence.class);
    permissionPersistence = mock(PermissionPersistence.class);
    secretsRepositoryWriter = new SecretsRepositoryWriter(secretPersistence);
    connectionsHandler = mock(ConnectionsHandler.class);
    destinationHandler = mock(DestinationHandler.class);
    sourceHandler = mock(SourceHandler.class);
    uuidSupplier = mock(Supplier.class);
    workspaceService = mock(WorkspaceService.class);
    trackingClient = mock(TrackingClient.class);

    workspace = generateWorkspace();
    workspacesHandler =
        new WorkspacesHandler(configRepository, workspacePersistence, organizationPersistence, secretsRepositoryWriter, permissionPersistence,
            connectionsHandler,
            destinationHandler, sourceHandler, uuidSupplier, workspaceService, trackingClient);
  }

  private StandardWorkspace generateWorkspace() {
    return new StandardWorkspace()
        .withWorkspaceId(UUID.randomUUID())
        .withCustomerId(UUID.randomUUID())
        .withEmail(TEST_EMAIL)
        .withName(TEST_WORKSPACE_NAME)
        .withSlug(TEST_WORKSPACE_SLUG)
        .withInitialSetupComplete(false)
        .withDisplaySetupWizard(true)
        .withNews(false)
        .withAnonymousDataCollection(false)
        .withSecurityUpdates(false)
        .withTombstone(false)
        .withNotifications(List.of(generateNotification()))
        .withNotificationSettings(generateNotificationSettings())
        .withDefaultGeography(Geography.AUTO)
        .withOrganizationId(ORGANIZATION_ID);
  }

  private Organization generateOrganization(final String ssoRealm) {
    return new Organization()
        .withOrganizationId(ORGANIZATION_ID)
        .withName(TEST_ORGANIZATION_NAME)
        .withEmail(TEST_EMAIL)
        .withPba(false)
        .withOrgLevelBilling(false)
        .withSsoRealm(ssoRealm);
  }

  private Notification generateNotification() {
    return new Notification()
        .withNotificationType(NotificationType.SLACK)
        .withSlackConfiguration(new SlackNotificationConfiguration()
            .withWebhook(FAILURE_NOTIFICATION_WEBHOOK));
  }

  private NotificationSettings generateNotificationSettings() {
    return new NotificationSettings()
        .withSendOnFailure(
            new NotificationItem()
                .withNotificationType(List.of(NotificationType.SLACK))
                .withSlackConfiguration(new SlackNotificationConfiguration()
                    .withWebhook(FAILURE_NOTIFICATION_WEBHOOK)));
  }

  private io.airbyte.api.model.generated.Notification generateApiNotification() {
    return new io.airbyte.api.model.generated.Notification()
        .notificationType(io.airbyte.api.model.generated.NotificationType.SLACK)
        .slackConfiguration(new io.airbyte.api.model.generated.SlackNotificationConfiguration()
            .webhook(FAILURE_NOTIFICATION_WEBHOOK));
  }

  private io.airbyte.api.model.generated.NotificationSettings generateApiNotificationSettings() {
    return new io.airbyte.api.model.generated.NotificationSettings()
        .sendOnFailure(
            new io.airbyte.api.model.generated.NotificationItem().notificationType(List.of(io.airbyte.api.model.generated.NotificationType.SLACK))
                .slackConfiguration(new io.airbyte.api.model.generated.SlackNotificationConfiguration()
                    .webhook(FAILURE_NOTIFICATION_WEBHOOK)));
  }

  private io.airbyte.api.model.generated.NotificationSettings generateApiNotificationSettingsWithDefaultValue() {
    return new io.airbyte.api.model.generated.NotificationSettings()
        .sendOnFailure(
            new io.airbyte.api.model.generated.NotificationItem().notificationType(List.of(io.airbyte.api.model.generated.NotificationType.SLACK))
                .slackConfiguration(new io.airbyte.api.model.generated.SlackNotificationConfiguration()
                    .webhook(FAILURE_NOTIFICATION_WEBHOOK)))
        .sendOnSuccess(new io.airbyte.api.model.generated.NotificationItem().notificationType(List.of()))
        .sendOnConnectionUpdate(new io.airbyte.api.model.generated.NotificationItem().addNotificationTypeItem(
            io.airbyte.api.model.generated.NotificationType.CUSTOMERIO))
        .sendOnConnectionUpdateActionRequired(new io.airbyte.api.model.generated.NotificationItem().addNotificationTypeItem(
            io.airbyte.api.model.generated.NotificationType.CUSTOMERIO))
        .sendOnSyncDisabled(new io.airbyte.api.model.generated.NotificationItem().addNotificationTypeItem(
            io.airbyte.api.model.generated.NotificationType.CUSTOMERIO))
        .sendOnSyncDisabledWarning(new io.airbyte.api.model.generated.NotificationItem().addNotificationTypeItem(
            io.airbyte.api.model.generated.NotificationType.CUSTOMERIO))
        .sendOnBreakingChangeWarning(new io.airbyte.api.model.generated.NotificationItem().addNotificationTypeItem(
            io.airbyte.api.model.generated.NotificationType.CUSTOMERIO))
        .sendOnBreakingChangeSyncsDisabled(new io.airbyte.api.model.generated.NotificationItem().addNotificationTypeItem(
            io.airbyte.api.model.generated.NotificationType.CUSTOMERIO));
  }

  private io.airbyte.api.model.generated.NotificationSettings generateDefaultApiNotificationSettings() {
    return new io.airbyte.api.model.generated.NotificationSettings()
        .sendOnSuccess(new io.airbyte.api.model.generated.NotificationItem().notificationType(List.of()))
        .sendOnFailure(new io.airbyte.api.model.generated.NotificationItem()
            .notificationType(List.of(io.airbyte.api.model.generated.NotificationType.CUSTOMERIO)))
        .sendOnConnectionUpdate(new io.airbyte.api.model.generated.NotificationItem().addNotificationTypeItem(
            io.airbyte.api.model.generated.NotificationType.CUSTOMERIO))
        .sendOnConnectionUpdateActionRequired(new io.airbyte.api.model.generated.NotificationItem().addNotificationTypeItem(
            io.airbyte.api.model.generated.NotificationType.CUSTOMERIO))
        .sendOnSyncDisabled(new io.airbyte.api.model.generated.NotificationItem().addNotificationTypeItem(
            io.airbyte.api.model.generated.NotificationType.CUSTOMERIO))
        .sendOnSyncDisabledWarning(new io.airbyte.api.model.generated.NotificationItem().addNotificationTypeItem(
            io.airbyte.api.model.generated.NotificationType.CUSTOMERIO))
        .sendOnBreakingChangeWarning(new io.airbyte.api.model.generated.NotificationItem().addNotificationTypeItem(
            io.airbyte.api.model.generated.NotificationType.CUSTOMERIO))
        .sendOnBreakingChangeSyncsDisabled(new io.airbyte.api.model.generated.NotificationItem().addNotificationTypeItem(
            io.airbyte.api.model.generated.NotificationType.CUSTOMERIO));
  }

  @Test
  void testCreateWorkspace() throws JsonValidationException, IOException, ConfigNotFoundException {
    workspace.withWebhookOperationConfigs(PERSISTED_WEBHOOK_CONFIGS);
    when(configRepository.getStandardWorkspaceNoSecrets(any(), eq(false))).thenReturn(workspace);

    final UUID uuid = UUID.randomUUID();
    when(uuidSupplier.get()).thenReturn(uuid);

    configRepository.writeStandardWorkspaceNoSecrets(workspace);

    final WorkspaceCreate workspaceCreate = new WorkspaceCreate()
        .name(NEW_WORKSPACE)
        .email(TEST_EMAIL)
        .news(false)
        .anonymousDataCollection(false)
        .securityUpdates(false)
        .notifications(List.of(generateApiNotification()))
        .notificationSettings(generateApiNotificationSettings())
        .defaultGeography(GEOGRAPHY_US)
        .webhookConfigs(List.of(new WebhookConfigWrite().name(TEST_NAME).authToken(TEST_AUTH_TOKEN)))
        .organizationId(ORGANIZATION_ID);

    when(secretPersistence.read(any())).thenReturn("");

    final WorkspaceRead actualRead = workspacesHandler.createWorkspace(workspaceCreate);
    final WorkspaceRead expectedRead = new WorkspaceRead()
        .workspaceId(uuid)
        .customerId(uuid)
        .email(TEST_EMAIL)
        .name(NEW_WORKSPACE)
        .slug("new-workspace")
        .initialSetupComplete(false)
        .displaySetupWizard(false)
        .news(false)
        .anonymousDataCollection(false)
        .securityUpdates(false)
        .notifications(List.of(generateApiNotification()))
        .notificationSettings(generateApiNotificationSettingsWithDefaultValue())
        .defaultGeography(GEOGRAPHY_US)
        .webhookConfigs(List.of(new WebhookConfigRead().id(uuid).name(TEST_NAME)))
        .organizationId(ORGANIZATION_ID);

    assertEquals(expectedRead, actualRead);
  }

  @Test
  void testCreateWorkspaceIfNotExist() throws JsonValidationException, IOException, ConfigNotFoundException {
    workspace.withWebhookOperationConfigs(PERSISTED_WEBHOOK_CONFIGS);
    when(configRepository.getStandardWorkspaceNoSecrets(any(), eq(false))).thenReturn(workspace);

    final UUID uuid = UUID.randomUUID();
    when(uuidSupplier.get()).thenReturn(uuid);

    configRepository.writeStandardWorkspaceNoSecrets(workspace);

    final UUID notSuppliedUUID = UUID.randomUUID();

    final WorkspaceCreateWithId workspaceCreateWithId = new WorkspaceCreateWithId()
        .id(notSuppliedUUID)
        .name(NEW_WORKSPACE)
        .email(TEST_EMAIL)
        .news(false)
        .anonymousDataCollection(false)
        .securityUpdates(false)
        .notifications(List.of(generateApiNotification()))
        .notificationSettings(generateApiNotificationSettings())
        .defaultGeography(GEOGRAPHY_US)
        .webhookConfigs(List.of(new WebhookConfigWrite().name(TEST_NAME).authToken(TEST_AUTH_TOKEN)))
        .organizationId(ORGANIZATION_ID);

    when(secretPersistence.read(any())).thenReturn("");

    final WorkspaceRead actualRead = workspacesHandler.createWorkspaceIfNotExist(workspaceCreateWithId);
    final WorkspaceRead secondActualRead = workspacesHandler.createWorkspaceIfNotExist(workspaceCreateWithId);
    final WorkspaceRead expectedRead = new WorkspaceRead()
        .workspaceId(notSuppliedUUID)
        .customerId(uuid)
        .email(TEST_EMAIL)
        .name(NEW_WORKSPACE)
        .slug("new-workspace")
        .initialSetupComplete(false)
        .displaySetupWizard(false)
        .news(false)
        .anonymousDataCollection(false)
        .securityUpdates(false)
        .notifications(List.of(generateApiNotification()))
        .notificationSettings(generateApiNotificationSettingsWithDefaultValue())
        .defaultGeography(GEOGRAPHY_US)
        .webhookConfigs(List.of(new WebhookConfigRead().id(uuid).name(TEST_NAME)))
        .organizationId(ORGANIZATION_ID);

    assertEquals(expectedRead, actualRead);
    assertEquals(expectedRead, secondActualRead);
  }

  @Test
  void testCreateWorkspaceWithMinimumInput() throws JsonValidationException, IOException, ConfigNotFoundException {
    when(configRepository.getStandardWorkspaceNoSecrets(any(), eq(false))).thenReturn(workspace);

    final UUID uuid = UUID.randomUUID();
    when(uuidSupplier.get()).thenReturn(uuid);

    configRepository.writeStandardWorkspaceNoSecrets(workspace);

    final WorkspaceCreate workspaceCreate = new WorkspaceCreate()
        .name(NEW_WORKSPACE)
        .email(TEST_EMAIL);

    final WorkspaceRead actualRead = workspacesHandler.createWorkspace(workspaceCreate);
    final WorkspaceRead expectedRead = new WorkspaceRead()
        .workspaceId(actualRead.getWorkspaceId())
        .customerId(actualRead.getCustomerId())
        .email(TEST_EMAIL)
        .name(NEW_WORKSPACE)
        .slug(actualRead.getSlug())
        .initialSetupComplete(false)
        .displaySetupWizard(false)
        .news(false)
        .anonymousDataCollection(false)
        .securityUpdates(false)
        .notifications(List.of())
        .notificationSettings(generateDefaultApiNotificationSettings())
        .defaultGeography(GEOGRAPHY_AUTO)
        .webhookConfigs(Collections.emptyList());

    assertEquals(expectedRead, actualRead);
  }

  @Test
  void testCreateWorkspaceDuplicateSlug() throws JsonValidationException, IOException, ConfigNotFoundException {
    when(configRepository.getWorkspaceBySlugOptional(any(String.class), eq(true)))
        .thenReturn(Optional.of(workspace))
        .thenReturn(Optional.of(workspace))
        .thenReturn(Optional.empty());
    when(configRepository.getStandardWorkspaceNoSecrets(any(), eq(false))).thenReturn(workspace);

    final UUID uuid = UUID.randomUUID();
    when(uuidSupplier.get()).thenReturn(uuid);

    configRepository.writeStandardWorkspaceNoSecrets(workspace);

    final WorkspaceCreate workspaceCreate = new WorkspaceCreate()
        .name(workspace.getName())
        .email(TEST_EMAIL)
        .news(false)
        .anonymousDataCollection(false)
        .securityUpdates(false)
        .notifications(Collections.emptyList());

    final WorkspaceRead actualRead = workspacesHandler.createWorkspace(workspaceCreate);
    final WorkspaceRead expectedRead = new WorkspaceRead()
        .workspaceId(uuid)
        .customerId(uuid)
        .email(TEST_EMAIL)
        .name(workspace.getName())
        .slug(workspace.getSlug())
        .initialSetupComplete(false)
        .displaySetupWizard(false)
        .news(false)
        .anonymousDataCollection(false)
        .securityUpdates(false)
        .notifications(Collections.emptyList())
        .notificationSettings(generateDefaultApiNotificationSettings())
        .defaultGeography(GEOGRAPHY_AUTO)
        .webhookConfigs(Collections.emptyList());

    assertTrue(actualRead.getSlug().startsWith(workspace.getSlug()));
    assertNotEquals(workspace.getSlug(), actualRead.getSlug());
    assertEquals(Jsons.clone(expectedRead).slug(null), Jsons.clone(actualRead).slug(null));
    final ArgumentCaptor<String> slugCaptor = ArgumentCaptor.forClass(String.class);
    verify(configRepository, times(3)).getWorkspaceBySlugOptional(slugCaptor.capture(), eq(true));
    assertEquals(3, slugCaptor.getAllValues().size());
    assertEquals(workspace.getSlug(), slugCaptor.getAllValues().get(0));
    assertTrue(slugCaptor.getAllValues().get(1).startsWith(workspace.getSlug()));
    assertTrue(slugCaptor.getAllValues().get(2).startsWith(workspace.getSlug()));

  }

  @Test
  void testDeleteWorkspace() throws JsonValidationException, ConfigNotFoundException, IOException {
    final WorkspaceIdRequestBody workspaceIdRequestBody = new WorkspaceIdRequestBody().workspaceId(workspace.getWorkspaceId());

    final ConnectionRead connection = new ConnectionRead();
    final DestinationRead destination = new DestinationRead();
    final SourceRead source = new SourceRead();

    when(configRepository.getStandardWorkspaceNoSecrets(workspace.getWorkspaceId(), false)).thenReturn(workspace);

    when(configRepository.listStandardWorkspaces(false)).thenReturn(Collections.singletonList(workspace));

    when(connectionsHandler.listConnectionsForWorkspace(workspaceIdRequestBody))
        .thenReturn(new ConnectionReadList().connections(Collections.singletonList(connection)));

    when(destinationHandler.listDestinationsForWorkspace(workspaceIdRequestBody))
        .thenReturn(new DestinationReadList().destinations(Collections.singletonList(destination)));

    when(sourceHandler.listSourcesForWorkspace(workspaceIdRequestBody))
        .thenReturn(new SourceReadList().sources(Collections.singletonList(source)));

    workspacesHandler.deleteWorkspace(workspaceIdRequestBody);

    verify(connectionsHandler).deleteConnection(connection.getConnectionId());
    verify(destinationHandler).deleteDestination(destination);
    verify(sourceHandler).deleteSource(source);
  }

  @Test
  void testListWorkspaces() throws JsonValidationException, IOException {
    final StandardWorkspace workspace2 = generateWorkspace();

    when(configRepository.listStandardWorkspaces(false)).thenReturn(Lists.newArrayList(workspace, workspace2));

    final WorkspaceRead expectedWorkspaceRead1 = new WorkspaceRead()
        .workspaceId(workspace.getWorkspaceId())
        .customerId(workspace.getCustomerId())
        .email(workspace.getEmail())
        .name(workspace.getName())
        .slug(workspace.getSlug())
        .initialSetupComplete(workspace.getInitialSetupComplete())
        .displaySetupWizard(workspace.getDisplaySetupWizard())
        .news(workspace.getNews())
        .anonymousDataCollection(workspace.getAnonymousDataCollection())
        .securityUpdates(workspace.getSecurityUpdates())
        .notifications(List.of(generateApiNotification()))
        .notificationSettings(generateApiNotificationSettings())
        .defaultGeography(GEOGRAPHY_AUTO)
        .organizationId(ORGANIZATION_ID);

    final WorkspaceRead expectedWorkspaceRead2 = new WorkspaceRead()
        .workspaceId(workspace2.getWorkspaceId())
        .customerId(workspace2.getCustomerId())
        .email(workspace2.getEmail())
        .name(workspace2.getName())
        .slug(workspace2.getSlug())
        .initialSetupComplete(workspace2.getInitialSetupComplete())
        .displaySetupWizard(workspace2.getDisplaySetupWizard())
        .news(workspace2.getNews())
        .anonymousDataCollection(workspace2.getAnonymousDataCollection())
        .securityUpdates(workspace2.getSecurityUpdates())
        .notifications(List.of(generateApiNotification()))
        .notificationSettings(generateApiNotificationSettings())
        .defaultGeography(GEOGRAPHY_AUTO)
        .organizationId(ORGANIZATION_ID);

    final WorkspaceReadList actualWorkspaceReadList = workspacesHandler.listWorkspaces();

    assertEquals(Lists.newArrayList(expectedWorkspaceRead1, expectedWorkspaceRead2),
        actualWorkspaceReadList.getWorkspaces());
  }

  @Test
  void testGetWorkspace() throws JsonValidationException, ConfigNotFoundException, IOException {
    workspace.withWebhookOperationConfigs(PERSISTED_WEBHOOK_CONFIGS);
    when(configRepository.getStandardWorkspaceNoSecrets(workspace.getWorkspaceId(), false)).thenReturn(workspace);

    final WorkspaceIdRequestBody workspaceIdRequestBody = new WorkspaceIdRequestBody().workspaceId(workspace.getWorkspaceId());

    final WorkspaceRead workspaceRead = new WorkspaceRead()
        .workspaceId(workspace.getWorkspaceId())
        .customerId(workspace.getCustomerId())
        .email(TEST_EMAIL)
        .name(TEST_WORKSPACE_NAME)
        .slug(TEST_WORKSPACE_SLUG)
        .initialSetupComplete(false)
        .displaySetupWizard(true)
        .news(false)
        .anonymousDataCollection(false)
        .securityUpdates(false)
        .notifications(List.of(generateApiNotification()))
        .notificationSettings(generateApiNotificationSettings())
        .defaultGeography(GEOGRAPHY_AUTO)
        .webhookConfigs(List.of(new WebhookConfigRead().id(WEBHOOK_CONFIG_ID).name(TEST_NAME)))
        .organizationId(ORGANIZATION_ID);

    assertEquals(workspaceRead, workspacesHandler.getWorkspace(workspaceIdRequestBody));
  }

  @Test
  void testGetWorkspaceBySlug() throws JsonValidationException, ConfigNotFoundException, IOException {
    when(configRepository.getWorkspaceBySlug("default", false)).thenReturn(workspace);

    final SlugRequestBody slugRequestBody = new SlugRequestBody().slug("default");
    final WorkspaceRead workspaceRead = new WorkspaceRead()
        .workspaceId(workspace.getWorkspaceId())
        .customerId(workspace.getCustomerId())
        .email(TEST_EMAIL)
        .name(workspace.getName())
        .slug(workspace.getSlug())
        .initialSetupComplete(workspace.getInitialSetupComplete())
        .displaySetupWizard(workspace.getDisplaySetupWizard())
        .news(workspace.getNews())
        .anonymousDataCollection(workspace.getAnonymousDataCollection())
        .securityUpdates(workspace.getSecurityUpdates())
        .notifications(NotificationConverter.toApiList(workspace.getNotifications()))
        .notificationSettings(NotificationSettingsConverter.toApi(workspace.getNotificationSettings()))
        .defaultGeography(GEOGRAPHY_AUTO)
        .organizationId(ORGANIZATION_ID);

    assertEquals(workspaceRead, workspacesHandler.getWorkspaceBySlug(slugRequestBody));
  }

  @Test
  void testGetWorkspaceByConnectionId() throws ConfigNotFoundException {
    final UUID connectionId = UUID.randomUUID();
    when(configRepository.getStandardWorkspaceFromConnection(connectionId, false)).thenReturn(workspace);
    final ConnectionIdRequestBody connectionIdRequestBody = new ConnectionIdRequestBody().connectionId(connectionId);
    final WorkspaceRead workspaceRead = new WorkspaceRead()
        .workspaceId(workspace.getWorkspaceId())
        .customerId(workspace.getCustomerId())
        .email(TEST_EMAIL)
        .name(workspace.getName())
        .slug(workspace.getSlug())
        .initialSetupComplete(workspace.getInitialSetupComplete())
        .displaySetupWizard(workspace.getDisplaySetupWizard())
        .news(workspace.getNews())
        .anonymousDataCollection(workspace.getAnonymousDataCollection())
        .securityUpdates(workspace.getSecurityUpdates())
        .notifications(NotificationConverter.toApiList(workspace.getNotifications()))
        .notificationSettings(NotificationSettingsConverter.toApi(workspace.getNotificationSettings()))
        .defaultGeography(GEOGRAPHY_AUTO)
        .organizationId(ORGANIZATION_ID);

    assertEquals(workspaceRead, workspacesHandler.getWorkspaceByConnectionId(connectionIdRequestBody));
  }

  @Test
  void testGetWorkspaceByConnectionIdOnConfigNotFound() throws ConfigNotFoundException {
    final UUID connectionId = UUID.randomUUID();
    when(configRepository.getStandardWorkspaceFromConnection(connectionId, false))
        .thenThrow(new ConfigNotFoundException("something", connectionId.toString()));
    final ConnectionIdRequestBody connectionIdRequestBody = new ConnectionIdRequestBody().connectionId(connectionId);
    assertThrows(ConfigNotFoundException.class, () -> workspacesHandler.getWorkspaceByConnectionId(connectionIdRequestBody));
  }

  @ParameterizedTest
  @CsvSource({"true", "false"})
  void testGetWorkspaceOrganizationInfo(final Boolean isSso) throws IOException, ConfigNotFoundException {
    final Organization organization = generateOrganization(isSso ? "test-realm" : null);
    final WorkspaceIdRequestBody workspaceIdRequestBody = new WorkspaceIdRequestBody().workspaceId(workspace.getWorkspaceId());

    when(organizationPersistence.getOrganizationByWorkspaceId(workspace.getWorkspaceId())).thenReturn(Optional.of(organization));

    final WorkspaceOrganizationInfoRead orgInfo = workspacesHandler.getWorkspaceOrganizationInfo(workspaceIdRequestBody);

    assertEquals(organization.getOrganizationId(), orgInfo.getOrganizationId());
    assertEquals(organization.getName(), orgInfo.getOrganizationName());
    assertEquals(organization.getPba(), orgInfo.getPba());
    assertEquals(isSso, orgInfo.getSso()); // sso is true if ssoRealm is set
  }

  @Test
  void testGerWorkspaceOrganizationInfoConfigNotFound() throws IOException {
    final WorkspaceIdRequestBody workspaceIdRequestBody = new WorkspaceIdRequestBody().workspaceId(workspace.getWorkspaceId());

    when(organizationPersistence.getOrganizationByWorkspaceId(workspace.getWorkspaceId())).thenReturn(Optional.empty());

    assertThrows(ConfigNotFoundException.class, () -> workspacesHandler.getWorkspaceOrganizationInfo(workspaceIdRequestBody));
  }

  @Test
  void testUpdateWorkspace()
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final io.airbyte.api.model.generated.Notification apiNotification = generateApiNotification();
    apiNotification.getSlackConfiguration().webhook(UPDATED);
    final WorkspaceUpdate workspaceUpdate = new WorkspaceUpdate()
        .workspaceId(workspace.getWorkspaceId())
        .anonymousDataCollection(true)
        .securityUpdates(false)
        .news(false)
        .initialSetupComplete(true)
        .displaySetupWizard(false)
        .notifications(List.of(apiNotification))
        .notificationSettings(generateApiNotificationSettings())
        .defaultGeography(GEOGRAPHY_US)
        .webhookConfigs(List.of(new WebhookConfigWrite().name(TEST_NAME).authToken("test-auth-token")));

    final Notification expectedNotification = generateNotification();
    expectedNotification.getSlackConfiguration().withWebhook(UPDATED);
    final StandardWorkspace expectedWorkspace = new StandardWorkspace()
        .withWorkspaceId(workspace.getWorkspaceId())
        .withCustomerId(workspace.getCustomerId())
        .withEmail(TEST_EMAIL)
        .withName(TEST_WORKSPACE_NAME)
        .withSlug(TEST_WORKSPACE_SLUG)
        .withAnonymousDataCollection(true)
        .withSecurityUpdates(false)
        .withNews(false)
        .withInitialSetupComplete(true)
        .withDisplaySetupWizard(false)
        .withTombstone(false)
        .withNotifications(List.of(expectedNotification))
        .withNotificationSettings(generateNotificationSettings())
        .withDefaultGeography(Geography.US)
        .withWebhookOperationConfigs(PERSISTED_WEBHOOK_CONFIGS)
        .withOrganizationId(ORGANIZATION_ID);

    when(uuidSupplier.get()).thenReturn(WEBHOOK_CONFIG_ID);

    when(configRepository.getStandardWorkspaceNoSecrets(workspace.getWorkspaceId(), false))
        .thenReturn(workspace)
        .thenReturn(expectedWorkspace);

    when(secretPersistence.read(any())).thenReturn("");

    final WorkspaceRead actualWorkspaceRead = workspacesHandler.updateWorkspace(workspaceUpdate);

    final io.airbyte.api.model.generated.Notification expectedNotificationRead = generateApiNotification();
    expectedNotificationRead.getSlackConfiguration().webhook(UPDATED);
    final WorkspaceRead expectedWorkspaceRead = new WorkspaceRead()
        .workspaceId(workspace.getWorkspaceId())
        .customerId(workspace.getCustomerId())
        .email(TEST_EMAIL)
        .name(TEST_WORKSPACE_NAME)
        .slug(TEST_WORKSPACE_SLUG)
        .initialSetupComplete(true)
        .displaySetupWizard(false)
        .news(false)
        .anonymousDataCollection(true)
        .securityUpdates(false)
        .notifications(List.of(expectedNotificationRead))
        .notificationSettings(generateApiNotificationSettings())
        .defaultGeography(GEOGRAPHY_US)
        .webhookConfigs(List.of(new WebhookConfigRead().name(TEST_NAME).id(WEBHOOK_CONFIG_ID)))
        .organizationId(ORGANIZATION_ID);

    final StandardWorkspace expectedWorkspaceWithSecrets = new StandardWorkspace()
        .withWorkspaceId(workspace.getWorkspaceId())
        .withCustomerId(workspace.getCustomerId())
        .withEmail(TEST_EMAIL)
        .withName(TEST_WORKSPACE_NAME)
        .withSlug(TEST_WORKSPACE_SLUG)
        .withAnonymousDataCollection(true)
        .withSecurityUpdates(false)
        .withNews(false)
        .withInitialSetupComplete(true)
        .withDisplaySetupWizard(false)
        .withTombstone(false)
        .withNotifications(List.of(expectedNotification))
        .withNotificationSettings(generateNotificationSettings())
        .withDefaultGeography(Geography.US)
        .withWebhookOperationConfigs(SECRET_WEBHOOK_CONFIGS)
        .withOrganizationId(ORGANIZATION_ID);

    verify(workspaceService).writeWorkspaceWithSecrets(expectedWorkspaceWithSecrets);

    assertEquals(expectedWorkspaceRead, actualWorkspaceRead);
  }

  @Test
  void testUpdateWorkspaceWithoutWebhookConfigs() throws JsonValidationException, ConfigNotFoundException, IOException {
    final io.airbyte.api.model.generated.Notification apiNotification = generateApiNotification();
    apiNotification.getSlackConfiguration().webhook(UPDATED);
    final WorkspaceUpdate workspaceUpdate = new WorkspaceUpdate()
        .workspaceId(workspace.getWorkspaceId())
        .anonymousDataCollection(false);

    final Notification expectedNotification = generateNotification();
    expectedNotification.getSlackConfiguration().withWebhook(UPDATED);
    final StandardWorkspace expectedWorkspace = new StandardWorkspace()
        .withWorkspaceId(workspace.getWorkspaceId())
        .withCustomerId(workspace.getCustomerId())
        .withEmail(TEST_EMAIL)
        .withName(TEST_WORKSPACE_NAME)
        .withSlug(TEST_WORKSPACE_SLUG)
        .withAnonymousDataCollection(true)
        .withSecurityUpdates(false)
        .withNews(false)
        .withInitialSetupComplete(true)
        .withDisplaySetupWizard(false)
        .withTombstone(false)
        .withNotifications(List.of(expectedNotification))
        .withDefaultGeography(Geography.US)
        .withWebhookOperationConfigs(PERSISTED_WEBHOOK_CONFIGS);

    when(uuidSupplier.get()).thenReturn(WEBHOOK_CONFIG_ID);

    when(configRepository.getStandardWorkspaceNoSecrets(workspace.getWorkspaceId(), false))
        .thenReturn(expectedWorkspace)
        .thenReturn(expectedWorkspace.withAnonymousDataCollection(false));

    workspacesHandler.updateWorkspace(workspaceUpdate);

    verify(configRepository).writeStandardWorkspaceNoSecrets(expectedWorkspace);
  }

  @Test
  @DisplayName("Updating workspace name should update name and slug")
  void testUpdateWorkspaceNoNameUpdate() throws JsonValidationException, ConfigNotFoundException, IOException {
    final WorkspaceUpdateName workspaceUpdate = new WorkspaceUpdateName()
        .workspaceId(workspace.getWorkspaceId())
        .name("New Workspace Name");

    final StandardWorkspace expectedWorkspace = new StandardWorkspace()
        .withWorkspaceId(workspace.getWorkspaceId())
        .withCustomerId(workspace.getCustomerId())
        .withEmail(TEST_EMAIL)
        .withName("New Workspace Name")
        .withSlug("new-workspace-name")
        .withAnonymousDataCollection(workspace.getAnonymousDataCollection())
        .withSecurityUpdates(workspace.getSecurityUpdates())
        .withNews(workspace.getNews())
        .withInitialSetupComplete(workspace.getInitialSetupComplete())
        .withDisplaySetupWizard(workspace.getDisplaySetupWizard())
        .withTombstone(false)
        .withNotifications(workspace.getNotifications())
        .withNotificationSettings(workspace.getNotificationSettings())
        .withDefaultGeography(Geography.AUTO)
        .withOrganizationId(ORGANIZATION_ID);

    when(configRepository.getStandardWorkspaceNoSecrets(workspace.getWorkspaceId(), false))
        .thenReturn(workspace)
        .thenReturn(expectedWorkspace);

    final WorkspaceRead actualWorkspaceRead = workspacesHandler.updateWorkspaceName(workspaceUpdate);

    final WorkspaceRead expectedWorkspaceRead = new WorkspaceRead()
        .workspaceId(workspace.getWorkspaceId())
        .customerId(workspace.getCustomerId())
        .email(TEST_EMAIL)
        .name("New Workspace Name")
        .slug("new-workspace-name")
        .initialSetupComplete(workspace.getInitialSetupComplete())
        .displaySetupWizard(workspace.getDisplaySetupWizard())
        .news(workspace.getNews())
        .anonymousDataCollection(workspace.getAnonymousDataCollection())
        .securityUpdates(workspace.getSecurityUpdates())
        .notifications(List.of(generateApiNotification()))
        .notificationSettings(generateApiNotificationSettings())
        .defaultGeography(GEOGRAPHY_AUTO)
        .organizationId(ORGANIZATION_ID);

    verify(configRepository).writeStandardWorkspaceNoSecrets(expectedWorkspace);

    assertEquals(expectedWorkspaceRead, actualWorkspaceRead);
  }

  @Test
  @DisplayName("Update organization in workspace")
  void testWorkspaceUpdateOrganization()
      throws JsonValidationException, ConfigNotFoundException, IOException, io.airbyte.data.exceptions.ConfigNotFoundException {
    final UUID newOrgId = UUID.randomUUID();
    final WorkspaceUpdateOrganization workspaceUpdateOrganization = new WorkspaceUpdateOrganization()
        .workspaceId(workspace.getWorkspaceId())
        .organizationId(newOrgId);
    final StandardWorkspace expectedWorkspace = Jsons.clone(workspace).withOrganizationId(newOrgId);
    when(workspaceService.getStandardWorkspaceNoSecrets(workspace.getWorkspaceId(), false))
        .thenReturn(expectedWorkspace);
    when(configRepository.getStandardWorkspaceNoSecrets(workspace.getWorkspaceId(), false))
        .thenReturn(expectedWorkspace);
    // The same as the original workspace, with only the organization ID updated.
    final WorkspaceRead expectedWorkspaceRead = new WorkspaceRead()
        .workspaceId(workspace.getWorkspaceId())
        .customerId(workspace.getCustomerId())
        .email(workspace.getEmail())
        .name(workspace.getName())
        .slug(workspace.getSlug())
        .initialSetupComplete(workspace.getInitialSetupComplete())
        .displaySetupWizard(workspace.getDisplaySetupWizard())
        .news(workspace.getNews())
        .anonymousDataCollection(false)
        .securityUpdates(workspace.getSecurityUpdates())
        .notifications(NotificationConverter.toApiList(workspace.getNotifications()))
        .notificationSettings(NotificationSettingsConverter.toApi(workspace.getNotificationSettings()))
        .defaultGeography(GEOGRAPHY_AUTO)
        .organizationId(newOrgId);

    final WorkspaceRead actualWorkspaceRead = workspacesHandler.updateWorkspaceOrganization(workspaceUpdateOrganization);
    verify(workspaceService).writeStandardWorkspaceNoSecrets(expectedWorkspace);
    assertEquals(expectedWorkspaceRead, actualWorkspaceRead);
  }

  @Test
  @DisplayName("Partial patch update should preserve unchanged fields")
  void testWorkspacePatchUpdate() throws JsonValidationException, ConfigNotFoundException, IOException {
    final String expectedNewEmail = "expected-new-email@example.com";
    final WorkspaceUpdate workspaceUpdate = new WorkspaceUpdate()
        .workspaceId(workspace.getWorkspaceId())
        .anonymousDataCollection(true)
        .email(expectedNewEmail);

    final StandardWorkspace expectedWorkspace = Jsons.clone(workspace).withEmail(expectedNewEmail).withAnonymousDataCollection(true);
    when(configRepository.getStandardWorkspaceNoSecrets(workspace.getWorkspaceId(), false))
        .thenReturn(workspace)
        .thenReturn(expectedWorkspace);
    // The same as the original workspace, with only the email and data collection flags changed.
    final WorkspaceRead expectedWorkspaceRead = new WorkspaceRead()
        .workspaceId(workspace.getWorkspaceId())
        .customerId(workspace.getCustomerId())
        .email(expectedNewEmail)
        .name(workspace.getName())
        .slug(workspace.getSlug())
        .initialSetupComplete(workspace.getInitialSetupComplete())
        .displaySetupWizard(workspace.getDisplaySetupWizard())
        .news(workspace.getNews())
        .anonymousDataCollection(true)
        .securityUpdates(workspace.getSecurityUpdates())
        .notifications(NotificationConverter.toApiList(workspace.getNotifications()))
        .notificationSettings(NotificationSettingsConverter.toApi(workspace.getNotificationSettings()))
        .defaultGeography(GEOGRAPHY_AUTO)
        .organizationId(ORGANIZATION_ID);

    final WorkspaceRead actualWorkspaceRead = workspacesHandler.updateWorkspace(workspaceUpdate);
    verify(configRepository).writeStandardWorkspaceNoSecrets(expectedWorkspace);
    assertEquals(expectedWorkspaceRead, actualWorkspaceRead);
  }

  @Test
  void testSetFeedbackDone() throws JsonValidationException, ConfigNotFoundException, IOException {
    final WorkspaceGiveFeedback workspaceGiveFeedback = new WorkspaceGiveFeedback()
        .workspaceId(UUID.randomUUID());

    workspacesHandler.setFeedbackDone(workspaceGiveFeedback);

    verify(configRepository).setFeedback(workspaceGiveFeedback.getWorkspaceId());
  }

  @Test
  void testWorkspaceIsWrittenThroughSecretsWriter()
      throws JsonValidationException, IOException, ConfigNotFoundException, io.airbyte.data.exceptions.ConfigNotFoundException {
    secretsRepositoryWriter = mock(SecretsRepositoryWriter.class);
    workspacesHandler =
        new WorkspacesHandler(configRepository, workspacePersistence, organizationPersistence,
            secretsRepositoryWriter, permissionPersistence, connectionsHandler,
            destinationHandler, sourceHandler, uuidSupplier, workspaceService, trackingClient);

    final UUID uuid = UUID.randomUUID();
    when(uuidSupplier.get()).thenReturn(uuid);

    final WorkspaceCreate workspaceCreate = new WorkspaceCreate()
        .name(NEW_WORKSPACE)
        .email(TEST_EMAIL)
        .news(false)
        .anonymousDataCollection(false)
        .securityUpdates(false)
        .notifications(List.of(generateApiNotification()))
        .notificationSettings(generateApiNotificationSettings())
        .defaultGeography(GEOGRAPHY_US);

    final WorkspaceRead actualRead = workspacesHandler.createWorkspace(workspaceCreate);
    final WorkspaceRead expectedRead = new WorkspaceRead()
        .workspaceId(uuid)
        .customerId(uuid)
        .email(TEST_EMAIL)
        .name(NEW_WORKSPACE)
        .slug("new-workspace")
        .initialSetupComplete(false)
        .displaySetupWizard(false)
        .news(false)
        .anonymousDataCollection(false)
        .securityUpdates(false)
        .notifications(List.of(generateApiNotification()))
        .notificationSettings(generateApiNotificationSettingsWithDefaultValue())
        .defaultGeography(GEOGRAPHY_US)
        .webhookConfigs(Collections.emptyList());

    assertEquals(expectedRead, actualRead);
    verify(workspaceService, times(1)).writeWorkspaceWithSecrets(any());
  }

  @Test
  void testListWorkspacesInOrgNoKeyword() throws Exception {
    final ListWorkspacesInOrganizationRequestBody request =
        new ListWorkspacesInOrganizationRequestBody().organizationId(ORGANIZATION_ID).pagination(new Pagination().pageSize(100).rowOffset(0));
    final List<StandardWorkspace> expectedWorkspaces = List.of(generateWorkspace(), generateWorkspace());

    when(workspacePersistence.listWorkspacesByOrganizationIdPaginated(new ResourcesByOrganizationQueryPaginated(ORGANIZATION_ID, false, 100, 0),
        Optional.empty()))
            .thenReturn(expectedWorkspaces);
    final WorkspaceReadList result = workspacesHandler.listWorkspacesInOrganization(request);
    assertEquals(2, result.getWorkspaces().size());
  }

  @Test
  void testListWorkspacesInOrgWithKeyword() throws Exception {
    final ListWorkspacesInOrganizationRequestBody request = new ListWorkspacesInOrganizationRequestBody().organizationId(ORGANIZATION_ID)
        .nameContains("nameContains").pagination(new Pagination().pageSize(100).rowOffset(0));
    final List<StandardWorkspace> expectedWorkspaces = List.of(generateWorkspace(), generateWorkspace());

    when(workspacePersistence.listWorkspacesByOrganizationIdPaginated(new ResourcesByOrganizationQueryPaginated(ORGANIZATION_ID, false, 100, 0),
        Optional.of("nameContains")))
            .thenReturn(expectedWorkspaces);
    final WorkspaceReadList result = workspacesHandler.listWorkspacesInOrganization(request);
    assertEquals(2, result.getWorkspaces().size());
  }

}
