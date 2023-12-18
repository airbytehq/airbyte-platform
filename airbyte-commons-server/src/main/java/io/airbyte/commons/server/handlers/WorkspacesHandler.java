/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import com.github.slugify.Slugify;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.airbyte.analytics.TrackingClient;
import io.airbyte.api.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.model.generated.ConnectionRead;
import io.airbyte.api.model.generated.DestinationRead;
import io.airbyte.api.model.generated.Geography;
import io.airbyte.api.model.generated.ListResourcesForWorkspacesRequestBody;
import io.airbyte.api.model.generated.ListWorkspacesByUserRequestBody;
import io.airbyte.api.model.generated.ListWorkspacesInOrganizationRequestBody;
import io.airbyte.api.model.generated.NotificationItem;
import io.airbyte.api.model.generated.NotificationSettings;
import io.airbyte.api.model.generated.NotificationType;
import io.airbyte.api.model.generated.SlugRequestBody;
import io.airbyte.api.model.generated.SourceRead;
import io.airbyte.api.model.generated.UserRead;
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
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.commons.server.converters.NotificationConverter;
import io.airbyte.commons.server.converters.NotificationSettingsConverter;
import io.airbyte.commons.server.converters.WorkspaceWebhookConfigsConverter;
import io.airbyte.commons.server.errors.InternalServerKnownException;
import io.airbyte.commons.server.errors.ValueConflictKnownException;
import io.airbyte.config.Organization;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.persistence.ConfigRepository.ResourcesByOrganizationQueryPaginated;
import io.airbyte.config.persistence.ConfigRepository.ResourcesByUserQueryPaginated;
import io.airbyte.config.persistence.ConfigRepository.ResourcesQueryPaginated;
import io.airbyte.config.persistence.OrganizationPersistence;
import io.airbyte.config.persistence.PermissionPersistence;
import io.airbyte.config.persistence.WorkspacePersistence;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.jooq.tools.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * WorkspacesHandler. Javadocs suppressed because api docs should be used as source of truth.
 */
@Singleton
public class WorkspacesHandler {

  public static final int MAX_SLUG_GENERATION_ATTEMPTS = 10;
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkspacesHandler.class);
  private final ConfigRepository configRepository;
  private final WorkspacePersistence workspacePersistence;
  private final OrganizationPersistence organizationPersistence;
  private final SecretsRepositoryWriter secretsRepositoryWriter;
  private final PermissionPersistence permissionPersistence;
  private final ConnectionsHandler connectionsHandler;
  private final DestinationHandler destinationHandler;
  private final SourceHandler sourceHandler;
  private final Supplier<UUID> uuidSupplier;
  private final WorkspaceService workspaceService;
  private final Slugify slugify;
  private final TrackingClient trackingClient;

  @VisibleForTesting
  public WorkspacesHandler(final ConfigRepository configRepository,
                           final WorkspacePersistence workspacePersistence,
                           final OrganizationPersistence organizationPersistence,
                           final SecretsRepositoryWriter secretsRepositoryWriter,
                           final PermissionPersistence permissionPersistence,
                           final ConnectionsHandler connectionsHandler,
                           final DestinationHandler destinationHandler,
                           final SourceHandler sourceHandler,
                           @Named("uuidGenerator") final Supplier<UUID> uuidSupplier,
                           final WorkspaceService workspaceService,
                           final TrackingClient trackingClient) {
    this.configRepository = configRepository;
    this.workspacePersistence = workspacePersistence;
    this.organizationPersistence = organizationPersistence;
    this.secretsRepositoryWriter = secretsRepositoryWriter;
    this.permissionPersistence = permissionPersistence;
    this.connectionsHandler = connectionsHandler;
    this.destinationHandler = destinationHandler;
    this.sourceHandler = sourceHandler;
    this.uuidSupplier = uuidSupplier;
    this.workspaceService = workspaceService;
    this.slugify = new Slugify();
    this.trackingClient = trackingClient;
  }

  private static WorkspaceRead buildWorkspaceRead(final StandardWorkspace workspace) {
    final WorkspaceRead result = new WorkspaceRead()
        .workspaceId(workspace.getWorkspaceId())
        .customerId(workspace.getCustomerId())
        .email(workspace.getEmail())
        .name(workspace.getName())
        .slug(workspace.getSlug())
        .initialSetupComplete(workspace.getInitialSetupComplete())
        .displaySetupWizard(workspace.getDisplaySetupWizard())
        .anonymousDataCollection(workspace.getAnonymousDataCollection())
        .news(workspace.getNews())
        .securityUpdates(workspace.getSecurityUpdates())
        .notifications(NotificationConverter.toApiList(workspace.getNotifications()))
        .notificationSettings(NotificationSettingsConverter.toApi(workspace.getNotificationSettings()))
        .defaultGeography(Enums.convertTo(workspace.getDefaultGeography(), Geography.class))
        .organizationId(workspace.getOrganizationId());
    // Add read-only webhook configs.
    if (workspace.getWebhookOperationConfigs() != null) {
      result.setWebhookConfigs(WorkspaceWebhookConfigsConverter.toApiReads(workspace.getWebhookOperationConfigs()));
    }
    return result;
  }

  public WorkspaceRead createWorkspace(final WorkspaceCreate workspaceCreate)
      throws JsonValidationException, IOException, ValueConflictKnownException, ConfigNotFoundException {

    final WorkspaceCreateWithId workspaceCreateWithId = new WorkspaceCreateWithId()
        .id(uuidSupplier.get())
        .organizationId(workspaceCreate.getOrganizationId())
        .defaultGeography(workspaceCreate.getDefaultGeography())
        .displaySetupWizard(workspaceCreate.getDisplaySetupWizard())
        .name(workspaceCreate.getName())
        .notifications(workspaceCreate.getNotifications())
        .webhookConfigs(workspaceCreate.getWebhookConfigs())
        .anonymousDataCollection(workspaceCreate.getAnonymousDataCollection())
        .email(workspaceCreate.getEmail())
        .news(workspaceCreate.getNews())
        .notificationSettings(workspaceCreate.getNotificationSettings())
        .securityUpdates(workspaceCreate.getSecurityUpdates());

    return createWorkspaceIfNotExist(workspaceCreateWithId);
  }

  public WorkspaceRead createWorkspaceIfNotExist(final WorkspaceCreateWithId workspaceCreateWithId)
      throws JsonValidationException, IOException, ValueConflictKnownException, ConfigNotFoundException {

    final String email = workspaceCreateWithId.getEmail();
    final Boolean anonymousDataCollection = workspaceCreateWithId.getAnonymousDataCollection();
    final Boolean news = workspaceCreateWithId.getNews();
    final Boolean securityUpdates = workspaceCreateWithId.getSecurityUpdates();
    final Boolean displaySetupWizard = workspaceCreateWithId.getDisplaySetupWizard();

    // if not set on the workspaceCreate, set the defaultGeography to AUTO
    final io.airbyte.config.Geography defaultGeography = workspaceCreateWithId.getDefaultGeography() != null
        ? Enums.convertTo(workspaceCreateWithId.getDefaultGeography(), io.airbyte.config.Geography.class)
        : io.airbyte.config.Geography.AUTO;

    // NotificationSettings from input will be patched with default values.
    final NotificationSettings notificationSettings = patchNotificationSettingsWithDefaultValue(workspaceCreateWithId);

    final StandardWorkspace workspace = new StandardWorkspace()
        .withWorkspaceId(workspaceCreateWithId.getId())
        .withCustomerId(uuidSupplier.get()) // "customer_id" should be deprecated
        .withName(workspaceCreateWithId.getName())
        .withSlug(generateUniqueSlug(workspaceCreateWithId.getName()))
        .withInitialSetupComplete(false)
        .withAnonymousDataCollection(anonymousDataCollection != null ? anonymousDataCollection : false)
        .withNews(news != null ? news : false)
        .withSecurityUpdates(securityUpdates != null ? securityUpdates : false)
        .withDisplaySetupWizard(displaySetupWizard != null ? displaySetupWizard : false)
        .withTombstone(false)
        .withNotifications(NotificationConverter.toConfigList(workspaceCreateWithId.getNotifications()))
        .withNotificationSettings(NotificationSettingsConverter.toConfig(notificationSettings))
        .withDefaultGeography(defaultGeography)
        .withWebhookOperationConfigs(WorkspaceWebhookConfigsConverter.toPersistenceWrite(workspaceCreateWithId.getWebhookConfigs(), uuidSupplier))
        .withOrganizationId(workspaceCreateWithId.getOrganizationId());

    if (!Strings.isNullOrEmpty(email)) {
      workspace.withEmail(email);
    }

    return persistStandardWorkspace(workspace);
  }

  public WorkspaceRead createDefaultWorkspaceForUser(final UserRead user, final Optional<Organization> organization)
      throws IOException, JsonValidationException, ConfigNotFoundException {
    // if user already had a default workspace, throw an exception
    if (user.getDefaultWorkspaceId() != null) {
      throw new IllegalArgumentException(
          String.format("User %s already has a default workspace %s", user.getUserId(), user.getDefaultWorkspaceId()));
    }
    final String companyName = user.getCompanyName();
    final String email = user.getEmail();
    final Boolean news = user.getNews();
    // otherwise, create a default workspace for this user
    final WorkspaceCreate workspaceCreate = new WorkspaceCreate()
        .name(getDefaultWorkspaceName(organization, companyName, email))
        .organizationId(organization.map(Organization::getOrganizationId).orElse(null))
        .email(email)
        .news(news)
        .anonymousDataCollection(false)
        .securityUpdates(false)
        .displaySetupWizard(true);
    return createWorkspace(workspaceCreate);
  }

  private String getDefaultWorkspaceName(final Optional<Organization> organization, final String companyName, final String email) {
    String defaultWorkspaceName = "";
    if (organization.isPresent()) {
      // use organization name as default workspace name
      defaultWorkspaceName = organization.get().getName().trim();
    }
    // if organization name is not available or empty, use user's company name (note: this is an
    // optional field)
    if (defaultWorkspaceName.isEmpty() && companyName != null) {
      defaultWorkspaceName = companyName.trim();
    }
    // if company name is still empty, use user's email (note: this is a required field)
    if (defaultWorkspaceName.isEmpty()) {
      defaultWorkspaceName = email;
    }
    return defaultWorkspaceName;
  }

  public void deleteWorkspace(final WorkspaceIdRequestBody workspaceIdRequestBody)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    // get existing implementation
    final StandardWorkspace persistedWorkspace = configRepository.getStandardWorkspaceNoSecrets(workspaceIdRequestBody.getWorkspaceId(), false);

    // disable all connections associated with this workspace
    for (final ConnectionRead connectionRead : connectionsHandler.listConnectionsForWorkspace(workspaceIdRequestBody).getConnections()) {
      connectionsHandler.deleteConnection(connectionRead.getConnectionId());
    }

    // disable all destinations associated with this workspace
    for (final DestinationRead destinationRead : destinationHandler.listDestinationsForWorkspace(workspaceIdRequestBody).getDestinations()) {
      destinationHandler.deleteDestination(destinationRead);
    }

    // disable all sources associated with this workspace
    for (final SourceRead sourceRead : sourceHandler.listSourcesForWorkspace(workspaceIdRequestBody).getSources()) {
      sourceHandler.deleteSource(sourceRead);
    }
    persistedWorkspace.withTombstone(true);
    persistStandardWorkspace(persistedWorkspace);
  }

  private NotificationSettings patchNotificationSettingsWithDefaultValue(final WorkspaceCreateWithId workspaceCreateWithId) {
    final NotificationSettings notificationSettings = new NotificationSettings()
        .sendOnSuccess(new NotificationItem().notificationType(List.of()))
        .sendOnFailure(new NotificationItem().addNotificationTypeItem(NotificationType.CUSTOMERIO))
        .sendOnConnectionUpdate(new NotificationItem().addNotificationTypeItem(NotificationType.CUSTOMERIO))
        .sendOnConnectionUpdateActionRequired(new NotificationItem().addNotificationTypeItem(NotificationType.CUSTOMERIO))
        .sendOnSyncDisabled(new NotificationItem().addNotificationTypeItem(NotificationType.CUSTOMERIO))
        .sendOnSyncDisabledWarning(new NotificationItem().addNotificationTypeItem(NotificationType.CUSTOMERIO))
        .sendOnBreakingChangeWarning(new NotificationItem().addNotificationTypeItem(NotificationType.CUSTOMERIO))
        .sendOnBreakingChangeSyncsDisabled(new NotificationItem().addNotificationTypeItem(NotificationType.CUSTOMERIO));
    if (workspaceCreateWithId.getNotificationSettings() != null) {
      final NotificationSettings inputNotificationSettings = workspaceCreateWithId.getNotificationSettings();
      if (inputNotificationSettings.getSendOnSuccess() != null) {
        notificationSettings.setSendOnSuccess(inputNotificationSettings.getSendOnSuccess());
      }
      if (inputNotificationSettings.getSendOnFailure() != null) {
        notificationSettings.setSendOnFailure(inputNotificationSettings.getSendOnFailure());
      }
      if (inputNotificationSettings.getSendOnConnectionUpdate() != null) {
        notificationSettings.setSendOnConnectionUpdate(inputNotificationSettings.getSendOnConnectionUpdate());
      }
      if (inputNotificationSettings.getSendOnConnectionUpdateActionRequired() != null) {
        notificationSettings.setSendOnConnectionUpdateActionRequired(inputNotificationSettings.getSendOnConnectionUpdateActionRequired());
      }
      if (inputNotificationSettings.getSendOnSyncDisabled() != null) {
        notificationSettings.setSendOnSyncDisabled(inputNotificationSettings.getSendOnSyncDisabled());
      }
      if (inputNotificationSettings.getSendOnSyncDisabledWarning() != null) {
        notificationSettings.setSendOnSyncDisabledWarning(inputNotificationSettings.getSendOnSyncDisabledWarning());
      }
      if (inputNotificationSettings.getSendOnBreakingChangeWarning() != null) {
        notificationSettings.setSendOnBreakingChangeWarning(inputNotificationSettings.getSendOnBreakingChangeWarning());
      }
      if (inputNotificationSettings.getSendOnBreakingChangeSyncsDisabled() != null) {
        notificationSettings.setSendOnBreakingChangeSyncsDisabled(inputNotificationSettings.getSendOnBreakingChangeSyncsDisabled());
      }
    }
    return notificationSettings;
  }

  public WorkspaceReadList listWorkspaces() throws JsonValidationException, IOException {
    final List<WorkspaceRead> reads = configRepository.listStandardWorkspaces(false).stream()
        .map(WorkspacesHandler::buildWorkspaceRead)
        .collect(Collectors.toList());
    return new WorkspaceReadList().workspaces(reads);
  }

  public WorkspaceReadList listAllWorkspacesPaginated(final ListResourcesForWorkspacesRequestBody listResourcesForWorkspacesRequestBody)
      throws IOException {
    final List<WorkspaceRead> reads = configRepository.listAllWorkspacesPaginated(
        new ResourcesQueryPaginated(
            listResourcesForWorkspacesRequestBody.getWorkspaceIds(),
            listResourcesForWorkspacesRequestBody.getIncludeDeleted(),
            listResourcesForWorkspacesRequestBody.getPagination().getPageSize(),
            listResourcesForWorkspacesRequestBody.getPagination().getRowOffset(),
            listResourcesForWorkspacesRequestBody.getNameContains()))
        .stream()
        .map(WorkspacesHandler::buildWorkspaceRead)
        .collect(Collectors.toList());
    return new WorkspaceReadList().workspaces(reads);
  }

  public WorkspaceReadList listWorkspacesPaginated(final ListResourcesForWorkspacesRequestBody listResourcesForWorkspacesRequestBody)
      throws IOException {
    final List<StandardWorkspace> standardWorkspaces = configRepository.listStandardWorkspacesPaginated(new ResourcesQueryPaginated(
        listResourcesForWorkspacesRequestBody.getWorkspaceIds(),
        listResourcesForWorkspacesRequestBody.getIncludeDeleted(),
        listResourcesForWorkspacesRequestBody.getPagination().getPageSize(),
        listResourcesForWorkspacesRequestBody.getPagination().getRowOffset(), null));

    final List<WorkspaceRead> reads = standardWorkspaces
        .stream()
        .map(WorkspacesHandler::buildWorkspaceRead)
        .collect(Collectors.toList());
    return new WorkspaceReadList().workspaces(reads);
  }

  public WorkspaceRead getWorkspace(final WorkspaceIdRequestBody workspaceIdRequestBody)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    final UUID workspaceId = workspaceIdRequestBody.getWorkspaceId();
    final boolean includeTombstone = workspaceIdRequestBody.getIncludeTombstone() != null ? workspaceIdRequestBody.getIncludeTombstone() : false;
    final StandardWorkspace workspace = configRepository.getStandardWorkspaceNoSecrets(workspaceId, includeTombstone);
    return buildWorkspaceRead(workspace);
  }

  public WorkspaceOrganizationInfoRead getWorkspaceOrganizationInfo(final WorkspaceIdRequestBody workspaceIdRequestBody)
      throws IOException, ConfigNotFoundException {
    final UUID workspaceId = workspaceIdRequestBody.getWorkspaceId();
    final Optional<Organization> organization = organizationPersistence.getOrganizationByWorkspaceId(workspaceId);
    if (organization.isEmpty()) {
      throw new ConfigNotFoundException("ORGANIZATION_FOR_WORKSPACE", workspaceId.toString());
    }
    return buildWorkspaceOrganizationInfoRead(organization.get());
  }

  @SuppressWarnings("unused")
  public WorkspaceRead getWorkspaceBySlug(final SlugRequestBody slugRequestBody) throws IOException, ConfigNotFoundException {
    // for now we assume there is one workspace and it has a default uuid.
    final StandardWorkspace workspace = configRepository.getWorkspaceBySlug(slugRequestBody.getSlug(), false);
    return buildWorkspaceRead(workspace);
  }

  public WorkspaceRead getWorkspaceByConnectionId(final ConnectionIdRequestBody connectionIdRequestBody) throws ConfigNotFoundException {
    final StandardWorkspace workspace = configRepository.getStandardWorkspaceFromConnection(connectionIdRequestBody.getConnectionId(), false);
    return buildWorkspaceRead(workspace);
  }

  public WorkspaceReadList listWorkspacesInOrganization(final ListWorkspacesInOrganizationRequestBody request) throws IOException {
    final Optional<String> nameContains = StringUtils.isBlank(request.getNameContains()) ? Optional.empty() : Optional.of(request.getNameContains());
    final List<WorkspaceRead> standardWorkspaces;
    if (request.getPagination() != null) {
      standardWorkspaces = workspacePersistence
          .listWorkspacesByOrganizationIdPaginated(
              new ResourcesByOrganizationQueryPaginated(request.getOrganizationId(),
                  false, request.getPagination().getPageSize(), request.getPagination().getRowOffset()),
              nameContains)
          .stream()
          .map(WorkspacesHandler::buildWorkspaceRead)
          .collect(Collectors.toList());
    } else {
      standardWorkspaces = workspacePersistence
          .listWorkspacesByOrganizationId(request.getOrganizationId(), false, nameContains)
          .stream()
          .map(WorkspacesHandler::buildWorkspaceRead)
          .collect(Collectors.toList());
    }
    return new WorkspaceReadList().workspaces(standardWorkspaces);
  }

  private WorkspaceReadList listWorkspacesByInstanceAdminUser(final ListWorkspacesByUserRequestBody request) throws IOException {
    final Optional<String> nameContains = StringUtils.isBlank(request.getNameContains()) ? Optional.empty() : Optional.of(request.getNameContains());
    final List<WorkspaceRead> standardWorkspaces;
    if (request.getPagination() != null) {
      standardWorkspaces = workspacePersistence
          .listWorkspacesByInstanceAdminUserPaginated(
              false, request.getPagination().getPageSize(), request.getPagination().getRowOffset(),
              nameContains)
          .stream()
          .map(WorkspacesHandler::buildWorkspaceRead)
          .collect(Collectors.toList());
    } else {
      standardWorkspaces = workspacePersistence
          .listWorkspacesByInstanceAdminUser(false, nameContains)
          .stream()
          .map(WorkspacesHandler::buildWorkspaceRead)
          .collect(Collectors.toList());
    }
    return new WorkspaceReadList().workspaces(standardWorkspaces);
  }

  public WorkspaceReadList listWorkspacesByUser(final ListWorkspacesByUserRequestBody request)
      throws IOException {
    // If user has instance_admin permission, list all workspaces.
    if (permissionPersistence.isUserInstanceAdmin(request.getUserId())) {
      return listWorkspacesByInstanceAdminUser(request);
    }
    // User has no instance_admin permission.
    final Optional<String> nameContains = StringUtils.isBlank(request.getNameContains()) ? Optional.empty() : Optional.of(request.getNameContains());
    final List<WorkspaceRead> standardWorkspaces;
    if (request.getPagination() != null) {
      standardWorkspaces = workspacePersistence
          .listWorkspacesByUserIdPaginated(
              new ResourcesByUserQueryPaginated(request.getUserId(),
                  false, request.getPagination().getPageSize(), request.getPagination().getRowOffset()),
              nameContains)
          .stream()
          .map(WorkspacesHandler::buildWorkspaceRead)
          .collect(Collectors.toList());
    } else {
      standardWorkspaces = workspacePersistence
          .listActiveWorkspacesByUserId(request.getUserId(), nameContains)
          .stream()
          .map(WorkspacesHandler::buildWorkspaceRead)
          .collect(Collectors.toList());
    }
    return new WorkspaceReadList().workspaces(standardWorkspaces);
  }

  public WorkspaceRead updateWorkspace(final WorkspaceUpdate workspacePatch) throws ConfigNotFoundException, IOException, JsonValidationException {
    final UUID workspaceId = workspacePatch.getWorkspaceId();

    LOGGER.debug("Starting updateWorkspace for workspaceId {}...", workspaceId);
    LOGGER.debug("Incoming workspacePatch: {}", workspacePatch);

    final StandardWorkspace workspace = configRepository.getStandardWorkspaceNoSecrets(workspaceId, false);
    LOGGER.debug("Initial workspace: {}", workspace);

    validateWorkspacePatch(workspace, workspacePatch);

    LOGGER.debug("Initial WorkspaceRead: {}", buildWorkspaceRead(workspace));

    applyPatchToStandardWorkspace(workspace, workspacePatch);

    LOGGER.debug("Patched Workspace before persisting: {}", workspace);

    if (workspacePatch.getWebhookConfigs() == null) {
      // We aren't persisting any secrets. It's safe (and necessary) to use the NoSecrets variant because
      // we never hydrated them in the first place.
      configRepository.writeStandardWorkspaceNoSecrets(workspace);
    } else {
      // We're saving new webhook configs, so we need to persist the secrets.
      persistStandardWorkspace(workspace);
    }

    // after updating email or tracking info, we need to re-identify the instance.
    trackingClient.identify(workspaceId);

    return buildWorkspaceReadFromId(workspaceId);
  }

  public WorkspaceRead updateWorkspaceName(final WorkspaceUpdateName workspaceUpdateName)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final UUID workspaceId = workspaceUpdateName.getWorkspaceId();

    final StandardWorkspace persistedWorkspace = configRepository.getStandardWorkspaceNoSecrets(workspaceId, false);

    persistedWorkspace
        .withName(workspaceUpdateName.getName())
        .withSlug(generateUniqueSlug(workspaceUpdateName.getName()));

    // NOTE: it's safe (and necessary) to use the NoSecrets variant because we never hydrated them in
    // the first place.
    configRepository.writeStandardWorkspaceNoSecrets(persistedWorkspace);

    return buildWorkspaceReadFromId(workspaceId);
  }

  @SuppressWarnings("PMD.PreserveStackTrace")
  public WorkspaceRead updateWorkspaceOrganization(final WorkspaceUpdateOrganization workspaceUpdateOrganization)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final UUID workspaceId = workspaceUpdateOrganization.getWorkspaceId();

    try {
      final StandardWorkspace persistedWorkspace = workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false);
      persistedWorkspace
          .withOrganizationId(workspaceUpdateOrganization.getOrganizationId());
      workspaceService.writeStandardWorkspaceNoSecrets(persistedWorkspace);
      return buildWorkspaceReadFromId(workspaceId);
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getConfigId());
    }
  }

  public void setFeedbackDone(final WorkspaceGiveFeedback workspaceGiveFeedback)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    configRepository.setFeedback(workspaceGiveFeedback.getWorkspaceId());
  }

  private WorkspaceRead buildWorkspaceReadFromId(final UUID workspaceId) throws ConfigNotFoundException, IOException, JsonValidationException {
    final StandardWorkspace workspace = configRepository.getStandardWorkspaceNoSecrets(workspaceId, false);
    return buildWorkspaceRead(workspace);
  }

  private WorkspaceOrganizationInfoRead buildWorkspaceOrganizationInfoRead(final Organization organization) {
    return new WorkspaceOrganizationInfoRead()
        .organizationId(organization.getOrganizationId())
        .organizationName(organization.getName())
        .pba(organization.getPba())
        .sso(organization.getSsoRealm() != null && !organization.getSsoRealm().isEmpty());
  }

  private String generateUniqueSlug(final String workspaceName) throws IOException {
    final String proposedSlug = slugify.slugify(workspaceName);

    // todo (cgardens) - this is going to be too expensive once there are too many workspaces. needs to
    // be replaced with an actual sql query. e.g. SELECT COUNT(*) WHERE slug=%s;
    boolean isSlugUsed = configRepository.getWorkspaceBySlugOptional(proposedSlug, true).isPresent();
    String resolvedSlug = proposedSlug;
    int count = 0;
    while (isSlugUsed) {
      // todo (cgardens) - this is still susceptible to a race condition where we randomly generate the
      // same slug in two different threads. this should be very unlikely. we can fix this by exposing
      // database transaction, but that is not something we can do quickly.
      resolvedSlug = proposedSlug + "-" + RandomStringUtils.randomAlphabetic(8);
      isSlugUsed = configRepository.getWorkspaceBySlugOptional(resolvedSlug, true).isPresent();
      count++;
      if (count > MAX_SLUG_GENERATION_ATTEMPTS) {
        throw new InternalServerKnownException(String.format("could not generate a valid slug after %s tries.", MAX_SLUG_GENERATION_ATTEMPTS));
      }
    }

    return resolvedSlug;
  }

  private void validateWorkspacePatch(final StandardWorkspace persistedWorkspace, final WorkspaceUpdate workspacePatch) {
    Preconditions.checkArgument(persistedWorkspace.getWorkspaceId().equals(workspacePatch.getWorkspaceId()));
  }

  private void applyPatchToStandardWorkspace(final StandardWorkspace workspace, final WorkspaceUpdate workspacePatch) {
    if (workspacePatch.getAnonymousDataCollection() != null) {
      workspace.setAnonymousDataCollection(workspacePatch.getAnonymousDataCollection());
    }
    if (workspacePatch.getNews() != null) {
      workspace.setNews(workspacePatch.getNews());
    }
    if (workspacePatch.getDisplaySetupWizard() != null) {
      workspace.setDisplaySetupWizard(workspacePatch.getDisplaySetupWizard());
    }
    if (workspacePatch.getSecurityUpdates() != null) {
      workspace.setSecurityUpdates(workspacePatch.getSecurityUpdates());
    }
    if (!Strings.isNullOrEmpty(workspacePatch.getEmail())) {
      workspace.setEmail(workspacePatch.getEmail());
    }
    if (workspacePatch.getInitialSetupComplete() != null) {
      workspace.setInitialSetupComplete(workspacePatch.getInitialSetupComplete());
    }
    if (workspacePatch.getNotifications() != null) {
      workspace.setNotifications(NotificationConverter.toConfigList(workspacePatch.getNotifications()));
    }
    if (workspacePatch.getNotificationSettings() != null) {
      workspace.setNotificationSettings(NotificationSettingsConverter.toConfig(workspacePatch.getNotificationSettings()));
    }
    if (workspacePatch.getDefaultGeography() != null) {
      workspace.setDefaultGeography(ApiPojoConverters.toPersistenceGeography(workspacePatch.getDefaultGeography()));
    }
    if (workspacePatch.getWebhookConfigs() != null) {
      workspace.setWebhookOperationConfigs(WorkspaceWebhookConfigsConverter.toPersistenceWrite(workspacePatch.getWebhookConfigs(), uuidSupplier));
    }
  }

  @SuppressWarnings("PMD.PreserveStackTrace")
  private WorkspaceRead persistStandardWorkspace(final StandardWorkspace workspace)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    try {
      workspaceService.writeWorkspaceWithSecrets(workspace);
    } catch (final io.airbyte.data.exceptions.ConfigNotFoundException e) {
      throw new ConfigNotFoundException(e.getType(), e.getConfigId());
    }
    return buildWorkspaceRead(workspace);
  }

}
