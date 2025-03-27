/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.config.persistence.ConfigNotFoundException.NO_ORGANIZATION_FOR_WORKSPACE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.airbyte.analytics.TrackingClient;
import io.airbyte.api.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.model.generated.ConnectionRead;
import io.airbyte.api.model.generated.DestinationRead;
import io.airbyte.api.model.generated.EmailNotificationConfig;
import io.airbyte.api.model.generated.ListResourcesForWorkspacesRequestBody;
import io.airbyte.api.model.generated.ListWorkspacesByUserRequestBody;
import io.airbyte.api.model.generated.ListWorkspacesInOrganizationRequestBody;
import io.airbyte.api.model.generated.NotificationConfig;
import io.airbyte.api.model.generated.NotificationItem;
import io.airbyte.api.model.generated.NotificationSettings;
import io.airbyte.api.model.generated.NotificationType;
import io.airbyte.api.model.generated.NotificationsConfig;
import io.airbyte.api.model.generated.SlugRequestBody;
import io.airbyte.api.model.generated.SourceRead;
import io.airbyte.api.model.generated.UserRead;
import io.airbyte.api.model.generated.WebhookNotificationConfig;
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
import io.airbyte.api.problems.model.generated.ProblemMessageData;
import io.airbyte.api.problems.throwable.generated.NotificationMissingUrlProblem;
import io.airbyte.api.problems.throwable.generated.NotificationRequiredProblem;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.random.RandomKt;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.commons.server.converters.NotificationConverter;
import io.airbyte.commons.server.converters.NotificationSettingsConverter;
import io.airbyte.commons.server.converters.WorkspaceConverter;
import io.airbyte.commons.server.converters.WorkspaceWebhookConfigsConverter;
import io.airbyte.commons.server.errors.BadObjectSchemaKnownException;
import io.airbyte.commons.server.errors.InternalServerKnownException;
import io.airbyte.commons.server.errors.ValueConflictKnownException;
import io.airbyte.commons.server.handlers.helpers.WorkspaceHelpersKt;
import io.airbyte.commons.server.limits.ConsumptionService;
import io.airbyte.commons.server.limits.ProductLimitsProvider;
import io.airbyte.commons.server.slug.Slug;
import io.airbyte.config.Configs;
import io.airbyte.config.Notification;
import io.airbyte.config.Organization;
import io.airbyte.config.ScopeType;
import io.airbyte.config.SlackNotificationConfiguration;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.persistence.OrganizationPersistence;
import io.airbyte.config.persistence.PermissionPersistence;
import io.airbyte.config.persistence.WorkspacePersistence;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.data.services.shared.ResourcesByOrganizationQueryPaginated;
import io.airbyte.data.services.shared.ResourcesByUserQueryPaginated;
import io.airbyte.data.services.shared.ResourcesQueryPaginated;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.HydrateLimits;
import io.airbyte.featureflag.Workspace;
import io.airbyte.validation.json.JsonValidationException;
import io.micronaut.core.util.CollectionUtils;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
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
  private final WorkspacePersistence workspacePersistence;
  private final OrganizationPersistence organizationPersistence;
  private final SecretsRepositoryWriter secretsRepositoryWriter;
  private final PermissionPersistence permissionPersistence;
  private final ConnectionsHandler connectionsHandler;
  private final DestinationHandler destinationHandler;
  private final SourceHandler sourceHandler;
  private final Supplier<UUID> uuidSupplier;
  private final WorkspaceService workspaceService;
  private final TrackingClient trackingClient;
  private final ApiPojoConverters apiPojoConverters;
  private final ProductLimitsProvider limitsProvider;
  private final ConsumptionService consumptionService;
  private final FeatureFlagClient ffClient;
  private final Configs.AirbyteEdition airbyteEdition;

  @VisibleForTesting
  public WorkspacesHandler(final WorkspacePersistence workspacePersistence,
                           final OrganizationPersistence organizationPersistence,
                           final SecretsRepositoryWriter secretsRepositoryWriter,
                           final PermissionPersistence permissionPersistence,
                           final ConnectionsHandler connectionsHandler,
                           final DestinationHandler destinationHandler,
                           final SourceHandler sourceHandler,
                           @Named("uuidGenerator") final Supplier<UUID> uuidSupplier,
                           final WorkspaceService workspaceService,
                           final TrackingClient trackingClient,
                           final ApiPojoConverters apiPojoConverters,
                           final ProductLimitsProvider limitsProvider,
                           final ConsumptionService consumptionService,
                           final FeatureFlagClient ffClient,
                           final Configs.AirbyteEdition airbyteEdition) {
    this.workspacePersistence = workspacePersistence;
    this.organizationPersistence = organizationPersistence;
    this.secretsRepositoryWriter = secretsRepositoryWriter;
    this.permissionPersistence = permissionPersistence;
    this.connectionsHandler = connectionsHandler;
    this.destinationHandler = destinationHandler;
    this.sourceHandler = sourceHandler;
    this.uuidSupplier = uuidSupplier;
    this.workspaceService = workspaceService;
    this.trackingClient = trackingClient;
    this.apiPojoConverters = apiPojoConverters;
    this.limitsProvider = limitsProvider;
    this.consumptionService = consumptionService;
    this.ffClient = ffClient;
    this.airbyteEdition = airbyteEdition;
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

    // We expect that the caller is specifying the workspace ID.
    // Since this code is currently only called by OSS, it's enforced in the public API and the UI
    // currently.
    if (workspaceCreateWithId.getOrganizationId() == null) {
      throw new BadObjectSchemaKnownException("Workspace missing org ID.");
    }

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

    validateWorkspace(workspace);

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
        .name(WorkspaceHelpersKt.getDefaultWorkspaceName(organization, companyName, email))
        .organizationId(organization.map(Organization::getOrganizationId).orElse(null))
        .email(email)
        .news(news)
        .anonymousDataCollection(false)
        .securityUpdates(false)
        .displaySetupWizard(true);
    return createWorkspace(workspaceCreate);
  }

  public void deleteWorkspace(final WorkspaceIdRequestBody workspaceIdRequestBody)
      throws JsonValidationException, IOException, ConfigNotFoundException, io.airbyte.config.persistence.ConfigNotFoundException {
    // get existing implementation
    final StandardWorkspace persistedWorkspace = workspaceService.getStandardWorkspaceNoSecrets(workspaceIdRequestBody.getWorkspaceId(), false);

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
    final List<WorkspaceRead> reads = workspaceService.listStandardWorkspaces(false).stream()
        .map(WorkspaceConverter::domainToApiModel)
        .collect(Collectors.toList());
    return new WorkspaceReadList().workspaces(reads);
  }

  public WorkspaceReadList listAllWorkspacesPaginated(final ListResourcesForWorkspacesRequestBody listResourcesForWorkspacesRequestBody)
      throws IOException {
    final List<WorkspaceRead> reads = workspaceService.listAllWorkspacesPaginated(
        new ResourcesQueryPaginated(
            listResourcesForWorkspacesRequestBody.getWorkspaceIds(),
            listResourcesForWorkspacesRequestBody.getIncludeDeleted(),
            listResourcesForWorkspacesRequestBody.getPagination().getPageSize(),
            listResourcesForWorkspacesRequestBody.getPagination().getRowOffset(),
            listResourcesForWorkspacesRequestBody.getNameContains()))
        .stream()
        .map(WorkspaceConverter::domainToApiModel)
        .collect(Collectors.toList());
    return new WorkspaceReadList().workspaces(reads);
  }

  public WorkspaceReadList listWorkspacesPaginated(final ListResourcesForWorkspacesRequestBody listResourcesForWorkspacesRequestBody)
      throws IOException {
    final List<StandardWorkspace> standardWorkspaces = workspaceService.listStandardWorkspacesPaginated(new ResourcesQueryPaginated(
        listResourcesForWorkspacesRequestBody.getWorkspaceIds(),
        listResourcesForWorkspacesRequestBody.getIncludeDeleted(),
        listResourcesForWorkspacesRequestBody.getPagination().getPageSize(),
        listResourcesForWorkspacesRequestBody.getPagination().getRowOffset(), null));

    final List<WorkspaceRead> reads = standardWorkspaces
        .stream()
        .map(WorkspaceConverter::domainToApiModel)
        .collect(Collectors.toList());
    return new WorkspaceReadList().workspaces(reads);
  }

  public WorkspaceRead getWorkspace(final WorkspaceIdRequestBody workspaceIdRequestBody)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    final UUID workspaceId = workspaceIdRequestBody.getWorkspaceId();
    final boolean includeTombstone = workspaceIdRequestBody.getIncludeTombstone() != null ? workspaceIdRequestBody.getIncludeTombstone() : false;
    final StandardWorkspace workspace = workspaceService.getStandardWorkspaceNoSecrets(workspaceId, includeTombstone);
    final WorkspaceRead result = WorkspaceConverter.domainToApiModel(workspace);
    if (ffClient.boolVariation(HydrateLimits.INSTANCE, new Workspace(workspaceId))) {
      final ProductLimitsProvider.WorkspaceLimits limits = limitsProvider.getLimitForWorkspace(workspaceId);
      final var consumption = consumptionService.getForWorkspace(workspaceId);
      result.workspaceLimits(WorkspaceConverter.domainToApiModel(limits, consumption));
    }
    return result;
  }

  public WorkspaceOrganizationInfoRead getWorkspaceOrganizationInfo(final WorkspaceIdRequestBody workspaceIdRequestBody)
      throws IOException, ConfigNotFoundException {
    final UUID workspaceId = workspaceIdRequestBody.getWorkspaceId();
    final Optional<Organization> organization = organizationPersistence.getOrganizationByWorkspaceId(workspaceId);
    if (organization.isEmpty()) {
      throw new ConfigNotFoundException(NO_ORGANIZATION_FOR_WORKSPACE, workspaceId.toString());
    }
    return buildWorkspaceOrganizationInfoRead(organization.get());
  }

  @SuppressWarnings("unused")
  public WorkspaceRead getWorkspaceBySlug(final SlugRequestBody slugRequestBody) throws IOException, ConfigNotFoundException {
    // for now we assume there is one workspace and it has a default uuid.
    final StandardWorkspace workspace = workspaceService.getWorkspaceBySlug(slugRequestBody.getSlug(), false);
    return WorkspaceConverter.domainToApiModel(workspace);
  }

  public WorkspaceRead getWorkspaceByConnectionId(final ConnectionIdRequestBody connectionIdRequestBody, boolean includeTombstone)
      throws ConfigNotFoundException {
    final StandardWorkspace workspace =
        workspaceService.getStandardWorkspaceFromConnection(connectionIdRequestBody.getConnectionId(), includeTombstone);
    return WorkspaceConverter.domainToApiModel(workspace);
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
          .map(WorkspaceConverter::domainToApiModel)
          .collect(Collectors.toList());
    } else {
      standardWorkspaces = workspacePersistence
          .listWorkspacesByOrganizationId(request.getOrganizationId(), false, nameContains)
          .stream()
          .map(WorkspaceConverter::domainToApiModel)
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
          .map(WorkspaceConverter::domainToApiModel)
          .collect(Collectors.toList());
    } else {
      standardWorkspaces = workspacePersistence
          .listWorkspacesByInstanceAdminUser(false, nameContains)
          .stream()
          .map(WorkspaceConverter::domainToApiModel)
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
          .map(WorkspaceConverter::domainToApiModel)
          .collect(Collectors.toList());
    } else {
      standardWorkspaces = workspacePersistence
          .listActiveWorkspacesByUserId(request.getUserId(), nameContains)
          .stream()
          .map(WorkspaceConverter::domainToApiModel)
          .collect(Collectors.toList());
    }
    return new WorkspaceReadList().workspaces(standardWorkspaces);
  }

  public WorkspaceRead updateWorkspace(final WorkspaceUpdate workspacePatch) throws ConfigNotFoundException, IOException, JsonValidationException {
    final UUID workspaceId = workspacePatch.getWorkspaceId();

    LOGGER.debug("Starting updateWorkspace for workspaceId {}...", workspaceId);
    LOGGER.debug("Incoming workspacePatch: {}", workspacePatch);

    final StandardWorkspace workspace = workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false);
    LOGGER.debug("Initial workspace: {}", workspace);
    LOGGER.debug("Initial WorkspaceRead: {}", WorkspaceConverter.domainToApiModel(workspace));

    applyPatchToStandardWorkspace(workspace, workspacePatch);
    validateWorkspacePatch(workspace, workspacePatch);
    validateWorkspace(workspace);

    LOGGER.debug("Patched Workspace before persisting: {}", workspace);

    if (CollectionUtils.isEmpty(workspacePatch.getWebhookConfigs())) {
      // We aren't persisting any secrets. It's safe (and necessary) to use the NoSecrets variant because
      // we never hydrated them in the first place.
      workspaceService.writeStandardWorkspaceNoSecrets(workspace);
    } else {
      // We're saving new webhook configs, so we need to persist the secrets.
      persistStandardWorkspace(workspace);
    }

    // after updating email or tracking info, we need to re-identify the instance.
    trackingClient.identify(workspaceId, ScopeType.WORKSPACE);

    return buildWorkspaceReadFromId(workspaceId);
  }

  public WorkspaceRead updateWorkspaceName(final WorkspaceUpdateName workspaceUpdateName)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final UUID workspaceId = workspaceUpdateName.getWorkspaceId();

    final StandardWorkspace persistedWorkspace = workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false);

    persistedWorkspace
        .withName(workspaceUpdateName.getName())
        .withSlug(generateUniqueSlug(workspaceUpdateName.getName()));

    // NOTE: it's safe (and necessary) to use the NoSecrets variant because we never hydrated them in
    // the first place.
    workspaceService.writeStandardWorkspaceNoSecrets(persistedWorkspace);

    return buildWorkspaceReadFromId(workspaceId);
  }

  @SuppressWarnings("PMD.PreserveStackTrace")
  public WorkspaceRead updateWorkspaceOrganization(final WorkspaceUpdateOrganization workspaceUpdateOrganization)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final UUID workspaceId = workspaceUpdateOrganization.getWorkspaceId();

    final StandardWorkspace persistedWorkspace = workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false);
    persistedWorkspace
        .withOrganizationId(workspaceUpdateOrganization.getOrganizationId());
    workspaceService.writeStandardWorkspaceNoSecrets(persistedWorkspace);
    return buildWorkspaceReadFromId(workspaceId);
  }

  public void setFeedbackDone(final WorkspaceGiveFeedback workspaceGiveFeedback)
      throws IOException, ConfigNotFoundException {
    workspaceService.setFeedback(workspaceGiveFeedback.getWorkspaceId());
  }

  private WorkspaceRead buildWorkspaceReadFromId(final UUID workspaceId) throws ConfigNotFoundException, IOException, JsonValidationException {
    final StandardWorkspace workspace = workspaceService.getStandardWorkspaceNoSecrets(workspaceId, false);
    return WorkspaceConverter.domainToApiModel(workspace);
  }

  private WorkspaceOrganizationInfoRead buildWorkspaceOrganizationInfoRead(final Organization organization) {
    return new WorkspaceOrganizationInfoRead()
        .organizationId(organization.getOrganizationId())
        .organizationName(organization.getName())
        .sso(organization.getSsoRealm() != null && !organization.getSsoRealm().isEmpty());
  }

  private String generateUniqueSlug(final String workspaceName) throws IOException {
    final String proposedSlug = Slug.slugify(workspaceName);

    // todo (cgardens) - this is going to be too expensive once there are too many workspaces. needs to
    // be replaced with an actual sql query. e.g. SELECT COUNT(*) WHERE slug=%s;
    boolean isSlugUsed = workspaceService.getWorkspaceBySlugOptional(proposedSlug, true).isPresent();
    String resolvedSlug = proposedSlug;
    int count = 0;
    while (isSlugUsed) {
      // todo (cgardens) - this is still susceptible to a race condition where we randomly generate the
      // same slug in two different threads. this should be very unlikely. we can fix this by exposing
      // database transaction, but that is not something we can do quickly.
      resolvedSlug = proposedSlug + "-" + RandomKt.randomAlpha(8);
      isSlugUsed = workspaceService.getWorkspaceBySlugOptional(resolvedSlug, true).isPresent();
      count++;
      if (count > MAX_SLUG_GENERATION_ATTEMPTS) {
        throw new InternalServerKnownException(String.format("could not generate a valid slug after %s tries.", MAX_SLUG_GENERATION_ATTEMPTS));
      }
    }

    return resolvedSlug;
  }

  private void validateWorkspace(final StandardWorkspace workspace) {
    if (workspace.getNotificationSettings() != null) {
      final io.airbyte.config.NotificationSettings settings = workspace.getNotificationSettings();
      validateNotificationItem(settings.getSendOnSuccess(), "success");
      validateNotificationItem(settings.getSendOnFailure(), "failure");
      validateNotificationItem(settings.getSendOnConnectionUpdate(), "connectionUpdate");
      validateNotificationItem(settings.getSendOnConnectionUpdateActionRequired(), "connectionUpdateActionRequired");
      validateNotificationItem(settings.getSendOnSyncDisabled(), "syncDisabled");
      validateNotificationItem(settings.getSendOnSyncDisabledWarning(), "syncDisabledWarning");
    }
  }

  private void validateNotificationItem(final io.airbyte.config.NotificationItem item, final String notificationName) {
    if (item == null) {
      return;
    }

    if (item.getNotificationType() != null && item.getNotificationType().contains(Notification.NotificationType.SLACK)) {
      if (item.getSlackConfiguration() == null || Strings.isNullOrEmpty(item.getSlackConfiguration().getWebhook())) {
        throw new NotificationMissingUrlProblem(
            new ProblemMessageData().message(String.format("The '%s' notification is enabled but is missing a URL.", notificationName)));
      }
    }

    // email notifications for connectionUpdateActionRequired and syncDisabled can't be disabled.
    // this rule only applies to Airbyte Cloud, because OSS doesn't support email notifications.
    if (airbyteEdition == Configs.AirbyteEdition.CLOUD) {
      if ("connectionUpdateActionRequired".equals(notificationName) || "syncDisabled".equals(notificationName)) {
        if (!item.getNotificationType().contains(Notification.NotificationType.CUSTOMERIO)) {
          throw new NotificationRequiredProblem(
              new ProblemMessageData().message(String.format("The '%s' email notification can't be disabled", notificationName)));
        }
      }
    }
  }

  private void validateWorkspacePatch(final StandardWorkspace persistedWorkspace, final WorkspaceUpdate workspacePatch) {
    Preconditions.checkArgument(persistedWorkspace.getWorkspaceId().equals(workspacePatch.getWorkspaceId()));
  }

  private void applyPatchToStandardWorkspace(final StandardWorkspace workspace, final WorkspaceUpdate workspacePatch) throws IOException {
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
    if (CollectionUtils.isNotEmpty(workspacePatch.getNotifications())) {
      workspace.setNotifications(NotificationConverter.toConfigList(workspacePatch.getNotifications()));
    }

    // The updateWorkspace function that calls this code is overloaded with two different behaviors:
    // it's mostly used for a PUT (complete overwrite) in the internal APIs, but also for PATCH in the
    // public API.
    //
    // The internal PUT APIs use notificationSettings, so if that field is set, then replace those
    // settings.
    // The public PATCH APIs use notificationsConfig, so if that field is set, then merge that config
    // into
    // the notification settings.
    if (workspacePatch.getNotificationSettings() != null) {
      workspace.setNotificationSettings(NotificationSettingsConverter.toConfig(workspacePatch.getNotificationSettings()));
    } else if (workspacePatch.getNotificationsConfig() != null) {
      patchNotifications(workspace, workspacePatch.getNotificationsConfig());
    }

    if (workspacePatch.getDefaultGeography() != null) {
      workspace.setDefaultGeography(apiPojoConverters.toPersistenceGeography(workspacePatch.getDefaultGeography()));
    }
    if (workspacePatch.getName() != null) {
      workspace.setName(workspacePatch.getName());
      workspace.setSlug(generateUniqueSlug(workspacePatch.getName()));
    }
    // Empty List is a valid value for webhookConfigs
    if (workspacePatch.getWebhookConfigs() != null) {
      workspace.setWebhookOperationConfigs(WorkspaceWebhookConfigsConverter.toPersistenceWrite(workspacePatch.getWebhookConfigs(), uuidSupplier));
    }
    if (workspacePatch.getName() != null) {
      workspace.setName(workspacePatch.getName());
    }
  }

  // Apply a patch to the internal notification config model (NotificationSettings).
  private void patchNotifications(final StandardWorkspace workspace, final NotificationsConfig config) {
    io.airbyte.config.NotificationSettings settings = workspace.getNotificationSettings();
    if (settings == null) {
      settings = new io.airbyte.config.NotificationSettings();
      workspace.setNotificationSettings(settings);
    }

    settings.setSendOnSuccess(patchNotificationItem(settings.getSendOnSuccess(), config.getSuccess()));
    settings.setSendOnFailure(patchNotificationItem(settings.getSendOnFailure(), config.getFailure()));
    settings.setSendOnConnectionUpdate(patchNotificationItem(settings.getSendOnConnectionUpdate(), config.getConnectionUpdate()));
    settings.setSendOnConnectionUpdateActionRequired(
        patchNotificationItem(settings.getSendOnConnectionUpdateActionRequired(), config.getConnectionUpdateActionRequired()));
    settings.setSendOnSyncDisabled(patchNotificationItem(settings.getSendOnSyncDisabled(), config.getSyncDisabled()));
    settings.setSendOnSyncDisabledWarning(patchNotificationItem(settings.getSendOnSyncDisabledWarning(), config.getSyncDisabledWarning()));
  }

  // Apply a patch to a specific webhook from the public API to the internal notification config model
  // (NotificationItem).
  @SuppressWarnings("PMD.AvoidReassigningParameters")
  private io.airbyte.config.NotificationItem patchNotificationItem(io.airbyte.config.NotificationItem item, final NotificationConfig config) {
    if (config == null) {
      return item;
    }

    final WebhookNotificationConfig webhook = config.getWebhook();
    final EmailNotificationConfig email = config.getEmail();

    if (webhook == null && email == null) {
      return item;
    }
    if (item == null) {
      item = new io.airbyte.config.NotificationItem();
    }

    if (email != null && email.getEnabled() != null) {
      if (email.getEnabled()) {
        addNotificationType(item, Notification.NotificationType.CUSTOMERIO);
      } else {
        removeNotificationType(item, Notification.NotificationType.CUSTOMERIO);
      }
    }

    if (webhook != null) {
      if (webhook.getEnabled() != null) {
        if (webhook.getEnabled()) {
          addNotificationType(item, Notification.NotificationType.SLACK);
        } else {
          removeNotificationType(item, Notification.NotificationType.SLACK);
        }
      }

      if (webhook.getUrl() != null) {
        if (item.getSlackConfiguration() != null) {
          item.getSlackConfiguration().setWebhook(webhook.getUrl());
        } else {
          item.setSlackConfiguration(new SlackNotificationConfiguration().withWebhook(webhook.getUrl()));
        }
      }
    }

    return item;
  }

  private void addNotificationType(io.airbyte.config.NotificationItem item, final Notification.NotificationType type) {
    if (item.getNotificationType() == null) {
      item.setNotificationType(new ArrayList<>());
    }
    if (item.getNotificationType().contains(type)) {
      return;
    }
    item.getNotificationType().add(type);
  }

  private void removeNotificationType(io.airbyte.config.NotificationItem item, final Notification.NotificationType type) {
    if (item.getNotificationType() == null) {
      return;
    }
    item.getNotificationType().remove(type);
  }

  @SuppressWarnings("PMD.PreserveStackTrace")
  private WorkspaceRead persistStandardWorkspace(final StandardWorkspace workspace)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    workspaceService.writeWorkspaceWithSecrets(workspace);
    return WorkspaceConverter.domainToApiModel(workspace);
  }

}
