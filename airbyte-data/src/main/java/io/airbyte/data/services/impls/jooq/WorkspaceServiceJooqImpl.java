/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_WORKSPACE_GRANT;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION_OPERATION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.NOTIFICATION_CONFIGURATION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.SCHEMA_MANAGEMENT;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.WORKSPACE;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.WORKSPACE_SERVICE_ACCOUNT;
import static io.airbyte.db.instance.jobs.jooq.generated.Tables.JOBS;
import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.noCondition;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.SQLDataType.VARCHAR;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.yaml.Yamls;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.ConfigWithMetadata;
import io.airbyte.config.Geography;
import io.airbyte.config.SecretPersistenceConfig;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.WorkspaceServiceAccount;
import io.airbyte.config.helpers.ScheduleHelpers;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.data.services.shared.ResourcesQueryPaginated;
import io.airbyte.data.services.shared.StandardSyncQuery;
import io.airbyte.db.Database;
import io.airbyte.db.ExceptionWrappingDatabase;
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType;
import io.airbyte.db.instance.configs.jooq.generated.enums.ReleaseStage;
import io.airbyte.db.instance.configs.jooq.generated.enums.ScopeType;
import io.airbyte.db.instance.configs.jooq.generated.enums.StatusType;
import io.airbyte.db.instance.configs.jooq.generated.tables.records.NotificationConfigurationRecord;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.Organization;
import io.airbyte.featureflag.UseRuntimeSecretPersistence;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.JoinType;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.SelectJoinStep;

@Slf4j
@Singleton
public class WorkspaceServiceJooqImpl implements WorkspaceService {

  private final ExceptionWrappingDatabase database;
  private final FeatureFlagClient featureFlagClient;
  private final SecretsRepositoryReader secretsRepositoryReader;
  private final SecretsRepositoryWriter secretsRepositoryWriter;
  private final SecretPersistenceConfigService secretPersistenceConfigService;

  @VisibleForTesting
  public WorkspaceServiceJooqImpl(@Named("configDatabase") final Database database,
                                  final FeatureFlagClient featureFlagClient,
                                  final SecretsRepositoryReader secretsRepositoryReader,
                                  final SecretsRepositoryWriter secretsRepositoryWriter,
                                  final SecretPersistenceConfigService secretPersistenceConfigService) {
    this.database = new ExceptionWrappingDatabase(database);
    this.featureFlagClient = featureFlagClient;
    this.secretsRepositoryReader = secretsRepositoryReader;
    this.secretsRepositoryWriter = secretsRepositoryWriter;
    this.secretPersistenceConfigService = secretPersistenceConfigService;
  }

  /**
   * Get workspace.
   *
   * @param workspaceId workspace id
   * @param includeTombstone include tombestoned workspace
   * @return workspace
   * @throws JsonValidationException - throws if returned sources are invalid
   * @throws IOException - you never know when you IO
   * @throws ConfigNotFoundException - throws if no source with that id can be found.
   */
  @Override
  public StandardWorkspace getStandardWorkspaceNoSecrets(final UUID workspaceId, final boolean includeTombstone)
      throws JsonValidationException, IOException, ConfigNotFoundException {
    return listWorkspaceQuery(Optional.of(List.of(workspaceId)), includeTombstone)
        .findFirst()
        .orElseThrow(() -> new ConfigNotFoundException(ConfigSchema.STANDARD_WORKSPACE, workspaceId));
  }

  /**
   * Get workspace from slug.
   *
   * @param slug to use to find the workspace
   * @param includeTombstone include tombestoned workspace
   * @return workspace, if present.
   * @throws IOException - you never know when you IO
   */
  @Override
  public Optional<StandardWorkspace> getWorkspaceBySlugOptional(final String slug, final boolean includeTombstone)
      throws IOException {
    final Result<Record> result;
    if (includeTombstone) {
      result = database.query(ctx -> ctx.select(WORKSPACE.asterisk())
          .from(WORKSPACE)
          .where(WORKSPACE.SLUG.eq(slug))).fetch();
    } else {
      result = database.query(ctx -> ctx.select(WORKSPACE.asterisk())
          .from(WORKSPACE)
          .where(WORKSPACE.SLUG.eq(slug)).andNot(WORKSPACE.TOMBSTONE)).fetch();
    }

    return result.stream().findFirst().map(DbConverter::buildStandardWorkspace);
  }

  /**
   * Get workspace from slug.
   *
   * @param slug to use to find the workspace
   * @param includeTombstone include tombestoned workspace
   * @return workspace
   * @throws IOException - you never know when you IO
   * @throws ConfigNotFoundException - throws if no source with that id can be found.
   */
  @Override
  public StandardWorkspace getWorkspaceBySlug(final String slug, final boolean includeTombstone) throws IOException, ConfigNotFoundException {
    return getWorkspaceBySlugOptional(slug, includeTombstone).orElseThrow(() -> new ConfigNotFoundException(ConfigSchema.STANDARD_WORKSPACE, slug));
  }

  /**
   * List workspaces.
   *
   * @param includeTombstone include tombstoned workspaces
   * @return workspaces
   * @throws IOException - you never know when you IO
   */
  @Override
  public List<StandardWorkspace> listStandardWorkspaces(final boolean includeTombstone) throws IOException {
    return listWorkspaceQuery(Optional.empty(), includeTombstone).toList();
  }

  /**
   * List ALL workspaces (paginated) with some filtering.
   *
   * @param resourcesQueryPaginated - contains all the information we need to paginate
   * @return A List of StandardWorkspace objects
   * @throws IOException you never know when you IO
   */
  @Override
  public List<StandardWorkspace> listAllWorkspacesPaginated(final ResourcesQueryPaginated resourcesQueryPaginated) throws IOException {
    return database.query(ctx -> ctx.select(WORKSPACE.asterisk())
        .from(WORKSPACE)
        .where(resourcesQueryPaginated.includeDeleted() ? noCondition() : WORKSPACE.TOMBSTONE.notEqual(true))
        .and(resourcesQueryPaginated.nameContains() != null ? WORKSPACE.NAME.contains(resourcesQueryPaginated.nameContains()) : noCondition())
        .limit(resourcesQueryPaginated.pageSize())
        .offset(resourcesQueryPaginated.rowOffset())
        .fetch())
        .stream()
        .map(DbConverter::buildStandardWorkspace)
        .toList();
  }

  @Override
  public Stream<StandardWorkspace> listWorkspaceQuery(final Optional<List<UUID>> workspaceIds, final boolean includeTombstone) throws IOException {
    return database.query(ctx -> ctx.select(WORKSPACE.asterisk())
        .from(WORKSPACE)
        .where(includeTombstone ? noCondition() : WORKSPACE.TOMBSTONE.notEqual(true))
        .and(workspaceIds.map(WORKSPACE.ID::in).orElse(noCondition()))
        .fetch())
        .stream()
        .map(DbConverter::buildStandardWorkspace);
  }

  /**
   * List workspaces (paginated).
   *
   * @param resourcesQueryPaginated - contains all the information we need to paginate
   * @return A List of StandardWorkspace objectjs
   * @throws IOException you never know when you IO
   */
  @Override
  public List<StandardWorkspace> listStandardWorkspacesPaginated(final ResourcesQueryPaginated resourcesQueryPaginated) throws IOException {
    return database.query(ctx -> ctx.select(WORKSPACE.asterisk())
        .from(WORKSPACE)
        .where(resourcesQueryPaginated.includeDeleted() ? noCondition() : WORKSPACE.TOMBSTONE.notEqual(true))
        .and(WORKSPACE.ID.in(resourcesQueryPaginated.workspaceIds()))
        .limit(resourcesQueryPaginated.pageSize())
        .offset(resourcesQueryPaginated.rowOffset())
        .fetch())
        .stream()
        .map(DbConverter::buildStandardWorkspace)
        .toList();
  }

  /**
   * Get workspace for a connection.
   *
   * @param connectionId connection id
   * @param isTombstone include tombstoned workspaces
   * @return workspace to which the connection belongs
   */
  @Override
  public StandardWorkspace getStandardWorkspaceFromConnection(final UUID connectionId, final boolean isTombstone) throws ConfigNotFoundException {
    try {
      final StandardSync sync = getStandardSync(connectionId);
      final SourceConnection source = getSourceConnection(sync.getSourceId());
      return getStandardWorkspaceNoSecrets(source.getWorkspaceId(), isTombstone);
    } catch (final ConfigNotFoundException e) {
      throw e;
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * MUST NOT ACCEPT SECRETS - Should only be called from { @link SecretsRepositoryWriter }.
   * <p>
   * Write a StandardWorkspace to the database.
   *
   * @param workspace - The configuration of the workspace
   * @throws JsonValidationException - throws is the workspace is invalid
   * @throws IOException - you never know when you IO
   */
  @Override
  public void writeStandardWorkspaceNoSecrets(final StandardWorkspace workspace) throws JsonValidationException, IOException {
    database.transaction(ctx -> {
      final OffsetDateTime timestamp = OffsetDateTime.now();
      final boolean isExistingConfig = ctx.fetchExists(select()
          .from(WORKSPACE)
          .where(WORKSPACE.ID.eq(workspace.getWorkspaceId())));

      if (isExistingConfig) {
        ctx.update(WORKSPACE)
            .set(WORKSPACE.ID, workspace.getWorkspaceId())
            .set(WORKSPACE.CUSTOMER_ID, workspace.getCustomerId())
            .set(WORKSPACE.NAME, workspace.getName())
            .set(WORKSPACE.SLUG, workspace.getSlug())
            .set(WORKSPACE.EMAIL, workspace.getEmail())
            .set(WORKSPACE.INITIAL_SETUP_COMPLETE, workspace.getInitialSetupComplete())
            .set(WORKSPACE.ANONYMOUS_DATA_COLLECTION, workspace.getAnonymousDataCollection())
            .set(WORKSPACE.SEND_NEWSLETTER, workspace.getNews())
            .set(WORKSPACE.SEND_SECURITY_UPDATES, workspace.getSecurityUpdates())
            .set(WORKSPACE.DISPLAY_SETUP_WIZARD, workspace.getDisplaySetupWizard())
            .set(WORKSPACE.TOMBSTONE, workspace.getTombstone() != null && workspace.getTombstone())
            .set(WORKSPACE.NOTIFICATIONS, JSONB.valueOf(Jsons.serialize(workspace.getNotifications())))
            .set(WORKSPACE.NOTIFICATION_SETTINGS, JSONB.valueOf(Jsons.serialize(workspace.getNotificationSettings())))
            .set(WORKSPACE.FIRST_SYNC_COMPLETE, workspace.getFirstCompletedSync())
            .set(WORKSPACE.FEEDBACK_COMPLETE, workspace.getFeedbackDone())
            .set(WORKSPACE.GEOGRAPHY, Enums.toEnum(
                workspace.getDefaultGeography().value(),
                io.airbyte.db.instance.configs.jooq.generated.enums.GeographyType.class).orElseThrow())
            .set(WORKSPACE.UPDATED_AT, timestamp)
            .set(WORKSPACE.WEBHOOK_OPERATION_CONFIGS, workspace.getWebhookOperationConfigs() == null ? null
                : JSONB.valueOf(Jsons.serialize(workspace.getWebhookOperationConfigs())))
            .set(WORKSPACE.ORGANIZATION_ID, workspace.getOrganizationId())
            .where(WORKSPACE.ID.eq(workspace.getWorkspaceId()))
            .execute();
      } else {
        ctx.insertInto(WORKSPACE)
            .set(WORKSPACE.ID, workspace.getWorkspaceId())
            .set(WORKSPACE.CUSTOMER_ID, workspace.getCustomerId())
            .set(WORKSPACE.NAME, workspace.getName())
            .set(WORKSPACE.SLUG, workspace.getSlug())
            .set(WORKSPACE.EMAIL, workspace.getEmail())
            .set(WORKSPACE.INITIAL_SETUP_COMPLETE, workspace.getInitialSetupComplete())
            .set(WORKSPACE.ANONYMOUS_DATA_COLLECTION, workspace.getAnonymousDataCollection())
            .set(WORKSPACE.SEND_NEWSLETTER, workspace.getNews())
            .set(WORKSPACE.SEND_SECURITY_UPDATES, workspace.getSecurityUpdates())
            .set(WORKSPACE.DISPLAY_SETUP_WIZARD, workspace.getDisplaySetupWizard())
            .set(WORKSPACE.TOMBSTONE, workspace.getTombstone() != null && workspace.getTombstone())
            .set(WORKSPACE.NOTIFICATIONS, JSONB.valueOf(Jsons.serialize(workspace.getNotifications())))
            .set(WORKSPACE.NOTIFICATION_SETTINGS, JSONB.valueOf(Jsons.serialize(workspace.getNotificationSettings())))
            .set(WORKSPACE.FIRST_SYNC_COMPLETE, workspace.getFirstCompletedSync())
            .set(WORKSPACE.FEEDBACK_COMPLETE, workspace.getFeedbackDone())
            .set(WORKSPACE.CREATED_AT, timestamp)
            .set(WORKSPACE.UPDATED_AT, timestamp)
            .set(WORKSPACE.GEOGRAPHY, Enums.toEnum(
                workspace.getDefaultGeography().value(),
                io.airbyte.db.instance.configs.jooq.generated.enums.GeographyType.class).orElseThrow())
            .set(WORKSPACE.ORGANIZATION_ID, workspace.getOrganizationId())
            .set(WORKSPACE.WEBHOOK_OPERATION_CONFIGS, workspace.getWebhookOperationConfigs() == null ? null
                : JSONB.valueOf(Jsons.serialize(workspace.getWebhookOperationConfigs())))
            .execute();
      }
      return null;

    });
  }

  /**
   * Set user feedback on workspace.
   *
   * @param workspaceId workspace id.
   * @throws IOException - you never know when you IO
   */
  @Override
  public void setFeedback(final UUID workspaceId) throws IOException {
    database.query(ctx -> ctx.update(WORKSPACE).set(WORKSPACE.FEEDBACK_COMPLETE, true).where(WORKSPACE.ID.eq(workspaceId)).execute());
  }

  /**
   * Test if workspace id has access to a connector definition.
   *
   * @param actorDefinitionId actor definition id
   * @param workspaceId id of the workspace
   * @return true, if the workspace has access. otherwise, false.
   * @throws IOException - you never know when you IO
   */
  @Override
  public boolean workspaceCanUseDefinition(final UUID actorDefinitionId, final UUID workspaceId) throws IOException {
    return scopeCanUseDefinition(actorDefinitionId, workspaceId, ScopeType.workspace.toString());
  }

  /**
   * Test if workspace is has access to a custom connector definition.
   *
   * @param actorDefinitionId custom actor definition id
   * @param workspaceId workspace id
   * @return true, if the workspace has access. otherwise, false.
   * @throws IOException - you never know when you IO
   */
  @Override
  public boolean workspaceCanUseCustomDefinition(final UUID actorDefinitionId, final UUID workspaceId) throws IOException {
    final Result<Record> records = actorDefinitionsJoinedWithGrants(
        workspaceId,
        ScopeType.workspace,
        JoinType.JOIN,
        ACTOR_DEFINITION.ID.eq(actorDefinitionId),
        ACTOR_DEFINITION.CUSTOM.eq(true));
    return records.isNotEmpty();
  }

  /**
   * List active workspace IDs with most recently running jobs within a given time window (in hours).
   *
   * @param timeWindowInHours - integer, e.g. 24, 48, etc
   * @return list of workspace IDs
   * @throws IOException - failed to query data
   */
  @Override
  public List<UUID> listActiveWorkspacesByMostRecentlyRunningJobs(final int timeWindowInHours) throws IOException {
    final Result<Record1<UUID>> records = database.query(ctx -> ctx.selectDistinct(ACTOR.WORKSPACE_ID)
        .from(ACTOR)
        .join(WORKSPACE)
        .on(ACTOR.WORKSPACE_ID.eq(WORKSPACE.ID))
        .join(CONNECTION)
        .on(CONNECTION.SOURCE_ID.eq(ACTOR.ID))
        .join(JOBS)
        .on(CONNECTION.ID.cast(VARCHAR(255)).eq(JOBS.SCOPE))
        .where(JOBS.UPDATED_AT.greaterOrEqual(OffsetDateTime.now().minusHours(timeWindowInHours)))
        .and(WORKSPACE.TOMBSTONE.isFalse())
        .fetch());
    return records.stream().map(record -> record.get(ACTOR.WORKSPACE_ID)).collect(Collectors.toList());
  }

  /**
   * Count connections in workspace.
   *
   * @param workspaceId workspace id
   * @return number of connections in workspace
   * @throws IOException if there is an issue while interacting with db.
   */
  @Override
  public int countConnectionsForWorkspace(final UUID workspaceId) throws IOException {
    return database.query(ctx -> ctx.selectCount()
        .from(CONNECTION)
        .join(ACTOR).on(CONNECTION.SOURCE_ID.eq(ACTOR.ID))
        .where(ACTOR.WORKSPACE_ID.eq(workspaceId))
        .and(CONNECTION.STATUS.notEqual(StatusType.deprecated))
        .andNot(ACTOR.TOMBSTONE)).fetchOne().into(int.class);
  }

  /**
   * Count sources in workspace.
   *
   * @param workspaceId workspace id
   * @return number of sources in workspace
   * @throws IOException if there is an issue while interacting with db.
   */
  @Override
  public int countSourcesForWorkspace(final UUID workspaceId) throws IOException {
    return database.query(ctx -> ctx.selectCount()
        .from(ACTOR)
        .where(ACTOR.WORKSPACE_ID.equal(workspaceId))
        .and(ACTOR.ACTOR_TYPE.eq(ActorType.source))
        .andNot(ACTOR.TOMBSTONE)).fetchOne().into(int.class);
  }

  /**
   * Count destinations in workspace.
   *
   * @param workspaceId workspace id
   * @return number of destinations in workspace
   * @throws IOException if there is an issue while interacting with db.
   */
  @Override
  public int countDestinationsForWorkspace(final UUID workspaceId) throws IOException {
    return database.query(ctx -> ctx.selectCount()
        .from(ACTOR)
        .where(ACTOR.WORKSPACE_ID.equal(workspaceId))
        .and(ACTOR.ACTOR_TYPE.eq(ActorType.destination))
        .andNot(ACTOR.TOMBSTONE)).fetchOne().into(int.class);
  }

  /**
   * Get workspace service account without secrets.
   *
   * @param workspaceId workspace id
   * @return workspace service account
   * @throws ConfigNotFoundException if the config does not exist
   * @throws IOException if there is an issue while interacting with db.
   */
  @Override
  public WorkspaceServiceAccount getWorkspaceServiceAccountNoSecrets(final UUID workspaceId) throws IOException, ConfigNotFoundException {
    // breaking the pattern of doing a list query, because we never want to list this resource without
    // scoping by workspace id.
    return database.query(ctx -> ctx.select(asterisk()).from(WORKSPACE_SERVICE_ACCOUNT)
        .where(WORKSPACE_SERVICE_ACCOUNT.WORKSPACE_ID.eq(workspaceId))
        .fetch())
        .map(DbConverter::buildWorkspaceServiceAccount)
        .stream()
        .findFirst()
        .orElseThrow(() -> new ConfigNotFoundException(ConfigSchema.WORKSPACE_SERVICE_ACCOUNT, workspaceId));
  }

  /**
   * Write workspace service account with no secrets.
   *
   * @param workspaceServiceAccount workspace service account
   * @throws IOException if there is an issue while interacting with db.
   */
  @Override
  public void writeWorkspaceServiceAccountNoSecrets(final WorkspaceServiceAccount workspaceServiceAccount) throws IOException {
    database.transaction(ctx -> {
      writeWorkspaceServiceAccount(Collections.singletonList(workspaceServiceAccount), ctx);
      return null;
    });
  }

  /**
   * Get geography for a workspace.
   *
   * @param workspaceId workspace id
   * @return geography
   * @throws IOException exception while interacting with the db
   */
  @Override
  public Geography getGeographyForWorkspace(final UUID workspaceId) throws IOException {
    return database.query(ctx -> ctx.select(WORKSPACE.GEOGRAPHY)
        .from(WORKSPACE)
        .where(WORKSPACE.ID.eq(workspaceId))
        .limit(1))
        .fetchOneInto(Geography.class);
  }

  /**
   * Specialized query for efficiently determining eligibility for the Free Connector Program. If a
   * workspace has at least one Alpha or Beta connector, users of that workspace will be prompted to
   * sign up for the program. This check is performed on nearly every page load so the query needs to
   * be as efficient as possible.
   * <p>
   * This should only be used for efficiently determining eligibility for the Free Connector Program.
   * Anything that involves billing should instead use the ActorDefinitionVersionHelper to determine
   * the ReleaseStages.
   *
   * @param workspaceId ID of the workspace to check connectors for
   * @return boolean indicating if an alpha or beta connector exists within the workspace
   */
  @Override
  public boolean getWorkspaceHasAlphaOrBetaConnector(final UUID workspaceId) throws IOException {
    final Condition releaseStageAlphaOrBeta = ACTOR_DEFINITION_VERSION.RELEASE_STAGE.eq(ReleaseStage.alpha)
        .or(ACTOR_DEFINITION_VERSION.RELEASE_STAGE.eq(ReleaseStage.beta));

    final Integer countResult = database.query(ctx -> ctx.selectCount()
        .from(ACTOR)
        .join(ACTOR_DEFINITION).on(ACTOR_DEFINITION.ID.eq(ACTOR.ACTOR_DEFINITION_ID))
        .join(ACTOR_DEFINITION_VERSION).on(ACTOR_DEFINITION_VERSION.ID.eq(ACTOR_DEFINITION.DEFAULT_VERSION_ID))
        .where(ACTOR.WORKSPACE_ID.eq(workspaceId))
        .and(ACTOR.TOMBSTONE.notEqual(true))
        .and(releaseStageAlphaOrBeta))
        .fetchOneInto(Integer.class);

    return countResult > 0;
  }

  /**
   * List connection IDs for active syncs based on the given query.
   *
   * @param standardSyncQuery query
   * @return list of connection IDs
   * @throws IOException if there is an issue while interacting with db.
   */
  @Override
  public List<UUID> listWorkspaceActiveSyncIds(final StandardSyncQuery standardSyncQuery)
      throws IOException {
    return database.query(ctx -> ctx
        .select(CONNECTION.ID)
        .from(CONNECTION)
        .join(ACTOR).on(CONNECTION.SOURCE_ID.eq(ACTOR.ID))
        .where(ACTOR.WORKSPACE_ID.eq(standardSyncQuery.workspaceId())
            .and(standardSyncQuery.destinationId() == null || standardSyncQuery.destinationId().isEmpty() ? noCondition()
                : CONNECTION.DESTINATION_ID.in(standardSyncQuery.destinationId()))
            .and(standardSyncQuery.sourceId() == null || standardSyncQuery.sourceId().isEmpty() ? noCondition()
                : CONNECTION.SOURCE_ID.in(standardSyncQuery.sourceId()))
            // includeDeleted is not relevant here because it refers to connection status deprecated,
            // and we are only retrieving active syncs anyway
            .and(CONNECTION.STATUS.eq(StatusType.active)))
        .groupBy(CONNECTION.ID)).fetchInto(UUID.class);
  }

  /**
   * List workspaces with given ids.
   *
   * @param includeTombstone include tombstoned workspaces
   * @return workspaces
   * @throws IOException - you never know when you IO
   */
  @Override
  public List<StandardWorkspace> listStandardWorkspacesWithIds(final List<UUID> workspaceIds,
                                                               final boolean includeTombstone)
      throws IOException {
    return listWorkspaceQuery(Optional.of(workspaceIds), includeTombstone).toList();
  }

  /**
   * Write workspace service account.
   *
   * @param configs list of workspace service account
   * @param ctx database context
   */
  private void writeWorkspaceServiceAccount(final List<WorkspaceServiceAccount> configs, final DSLContext ctx) {
    final OffsetDateTime timestamp = OffsetDateTime.now();
    configs.forEach((workspaceServiceAccount) -> {
      final boolean isExistingConfig = ctx.fetchExists(select()
          .from(WORKSPACE_SERVICE_ACCOUNT)
          .where(WORKSPACE_SERVICE_ACCOUNT.WORKSPACE_ID.eq(workspaceServiceAccount.getWorkspaceId())));

      if (isExistingConfig) {
        ctx.update(WORKSPACE_SERVICE_ACCOUNT)
            .set(WORKSPACE_SERVICE_ACCOUNT.WORKSPACE_ID, workspaceServiceAccount.getWorkspaceId())
            .set(WORKSPACE_SERVICE_ACCOUNT.SERVICE_ACCOUNT_ID, workspaceServiceAccount.getServiceAccountId())
            .set(WORKSPACE_SERVICE_ACCOUNT.SERVICE_ACCOUNT_EMAIL, workspaceServiceAccount.getServiceAccountEmail())
            .set(WORKSPACE_SERVICE_ACCOUNT.JSON_CREDENTIAL, JSONB.valueOf(Jsons.serialize(workspaceServiceAccount.getJsonCredential())))
            .set(WORKSPACE_SERVICE_ACCOUNT.HMAC_KEY, JSONB.valueOf(Jsons.serialize(workspaceServiceAccount.getHmacKey())))
            .set(WORKSPACE_SERVICE_ACCOUNT.UPDATED_AT, timestamp)
            .where(WORKSPACE_SERVICE_ACCOUNT.WORKSPACE_ID.eq(workspaceServiceAccount.getWorkspaceId()))
            .execute();
      } else {
        ctx.insertInto(WORKSPACE_SERVICE_ACCOUNT)
            .set(WORKSPACE_SERVICE_ACCOUNT.WORKSPACE_ID, workspaceServiceAccount.getWorkspaceId())
            .set(WORKSPACE_SERVICE_ACCOUNT.SERVICE_ACCOUNT_ID, workspaceServiceAccount.getServiceAccountId())
            .set(WORKSPACE_SERVICE_ACCOUNT.SERVICE_ACCOUNT_EMAIL, workspaceServiceAccount.getServiceAccountEmail())
            .set(WORKSPACE_SERVICE_ACCOUNT.JSON_CREDENTIAL, JSONB.valueOf(Jsons.serialize(workspaceServiceAccount.getJsonCredential())))
            .set(WORKSPACE_SERVICE_ACCOUNT.HMAC_KEY, JSONB.valueOf(Jsons.serialize(workspaceServiceAccount.getHmacKey())))
            .set(WORKSPACE_SERVICE_ACCOUNT.CREATED_AT, timestamp)
            .set(WORKSPACE_SERVICE_ACCOUNT.UPDATED_AT, timestamp)
            .execute();
      }
    });
  }

  /**
   * Get connection.
   *
   * @param connectionId connection id
   * @return connection
   * @throws JsonValidationException if the workspace is or contains invalid json
   * @throws ConfigNotFoundException if the config does not exist
   * @throws IOException if there is an issue while interacting with db.
   */
  @VisibleForTesting
  public StandardSync getStandardSync(final UUID connectionId) throws JsonValidationException, IOException, ConfigNotFoundException {
    return getStandardSyncWithMetadata(connectionId).getConfig();
  }

  private ConfigWithMetadata<StandardSync> getStandardSyncWithMetadata(final UUID connectionId) throws IOException, ConfigNotFoundException {
    final List<ConfigWithMetadata<StandardSync>> result = listStandardSyncWithMetadata(Optional.of(connectionId));

    final boolean foundMoreThanOneConfig = result.size() > 1;
    if (result.isEmpty()) {
      throw new ConfigNotFoundException(ConfigSchema.STANDARD_SYNC, connectionId.toString());
    } else if (foundMoreThanOneConfig) {
      throw new IllegalStateException(String.format("Multiple %s configs found for ID %s: %s", ConfigSchema.STANDARD_SYNC, connectionId, result));
    }
    return result.get(0);
  }

  private List<ConfigWithMetadata<StandardSync>> listStandardSyncWithMetadata(final Optional<UUID> configId) throws IOException {
    final Result<Record> result = database.query(ctx -> {
      final SelectJoinStep<Record> query = ctx.select(CONNECTION.asterisk(),
          SCHEMA_MANAGEMENT.AUTO_PROPAGATION_STATUS)
          .from(CONNECTION)
          // The schema management can be non-existent for a connection id, thus we need to do a left join
          .leftJoin(SCHEMA_MANAGEMENT).on(SCHEMA_MANAGEMENT.CONNECTION_ID.eq(CONNECTION.ID));
      if (configId.isPresent()) {
        return query.where(CONNECTION.ID.eq(configId.get())).fetch();
      }
      return query.fetch();
    });

    final List<ConfigWithMetadata<StandardSync>> standardSyncs = new ArrayList<>();
    for (final Record record : result) {
      final List<NotificationConfigurationRecord> notificationConfigurationRecords = database.query(ctx -> {
        if (configId.isPresent()) {
          return ctx.selectFrom(NOTIFICATION_CONFIGURATION)
              .where(NOTIFICATION_CONFIGURATION.CONNECTION_ID.eq(configId.get()))
              .fetch();
        } else {
          return ctx.selectFrom(NOTIFICATION_CONFIGURATION)
              .fetch();
        }
      });

      final StandardSync standardSync =
          DbConverter.buildStandardSync(record, connectionOperationIds(record.get(CONNECTION.ID)), notificationConfigurationRecords);
      if (ScheduleHelpers.isScheduleTypeMismatch(standardSync)) {
        throw new RuntimeException("unexpected schedule type mismatch");
      }
      standardSyncs.add(new ConfigWithMetadata<>(
          record.get(CONNECTION.ID).toString(),
          ConfigSchema.STANDARD_SYNC.name(),
          record.get(CONNECTION.CREATED_AT).toInstant(),
          record.get(CONNECTION.UPDATED_AT).toInstant(),
          standardSync));
    }
    return standardSyncs;
  }

  private List<UUID> connectionOperationIds(final UUID connectionId) throws IOException {
    final Result<Record> result = database.query(ctx -> ctx.select(asterisk())
        .from(CONNECTION_OPERATION)
        .where(CONNECTION_OPERATION.CONNECTION_ID.eq(connectionId))
        .fetch());

    final List<UUID> ids = new ArrayList<>();
    for (final Record record : result) {
      ids.add(record.get(CONNECTION_OPERATION.OPERATION_ID));
    }

    return ids;
  }

  /**
   * Returns source with a given id. Does not contain secrets. To hydrate with secrets see { @link
   * SourceService#getSourceConnectionWithSecrets(final UUID sourceId) }.
   *
   * @param sourceId - id of source to fetch.
   * @return sources
   * @throws JsonValidationException - throws if returned sources are invalid
   * @throws IOException - you never know when you IO
   * @throws ConfigNotFoundException - throws if no source with that id can be found.
   */
  @VisibleForTesting
  public SourceConnection getSourceConnection(final UUID sourceId) throws JsonValidationException, ConfigNotFoundException, IOException {
    return listSourceQuery(Optional.of(sourceId))
        .findFirst()
        .orElseThrow(() -> new ConfigNotFoundException(ConfigSchema.SOURCE_CONNECTION, sourceId));
  }

  /**
   * Test if workspace or organization id has access to a connector definition.
   *
   * @param actorDefinitionId actor definition id
   * @param scopeId id of the workspace or organization
   * @param scopeType enum of workspace or organization
   * @return true, if the workspace or organization has access. otherwise, false.
   * @throws IOException - you never know when you IO
   */
  private boolean scopeCanUseDefinition(final UUID actorDefinitionId, final UUID scopeId, final String scopeType) throws IOException {
    final Result<Record> records = actorDefinitionsJoinedWithGrants(
        scopeId,
        ScopeType.valueOf(scopeType),
        JoinType.LEFT_OUTER_JOIN,
        ACTOR_DEFINITION.ID.eq(actorDefinitionId),
        ACTOR_DEFINITION.PUBLIC.eq(true).or(ACTOR_DEFINITION_WORKSPACE_GRANT.ACTOR_DEFINITION_ID.eq(actorDefinitionId)));
    return records.isNotEmpty();
  }

  private Stream<SourceConnection> listSourceQuery(final Optional<UUID> configId) throws IOException {
    final Result<Record> result = database.query(ctx -> {
      final SelectJoinStep<Record> query = ctx.select(asterisk()).from(ACTOR);
      if (configId.isPresent()) {
        return query.where(ACTOR.ACTOR_TYPE.eq(ActorType.source), ACTOR.ID.eq(configId.get())).fetch();
      }
      return query.where(ACTOR.ACTOR_TYPE.eq(ActorType.source)).fetch();
    });

    return result.map(DbConverter::buildSourceConnection).stream();
  }

  private Result<Record> actorDefinitionsJoinedWithGrants(final UUID scopeId,
                                                          final ScopeType scopeType,
                                                          final JoinType joinType,
                                                          final Condition... conditions)
      throws IOException {
    Condition scopeConditional = ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_TYPE.eq(ScopeType.valueOf(scopeType.toString())).and(
        ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_ID.eq(scopeId));

    // if scope type is workspace, get organization id as well and add that into OR conditional
    if (scopeType == ScopeType.workspace) {
      final Optional<UUID> organizationId = getOrganizationIdFromWorkspaceId(scopeId);
      if (organizationId.isPresent()) {
        scopeConditional = scopeConditional.or(ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_TYPE.eq(ScopeType.organization).and(
            ACTOR_DEFINITION_WORKSPACE_GRANT.SCOPE_ID.eq(organizationId.get())));
      }
    }

    final Condition finalScopeConditional = scopeConditional;
    return database.query(ctx -> ctx.select(asterisk()).from(ACTOR_DEFINITION)
        .join(ACTOR_DEFINITION_WORKSPACE_GRANT, joinType)
        .on(ACTOR_DEFINITION.ID.eq(ACTOR_DEFINITION_WORKSPACE_GRANT.ACTOR_DEFINITION_ID).and(finalScopeConditional))
        .where(conditions)
        .fetch());
  }

  @Override
  public Optional<UUID> getOrganizationIdFromWorkspaceId(final UUID scopeId) throws IOException {
    if (scopeId == null) {
      return Optional.empty();
    }
    final Optional<Record1<UUID>> optionalRecord = database.query(ctx -> ctx.select(WORKSPACE.ORGANIZATION_ID).from(WORKSPACE)
        .where(WORKSPACE.ID.eq(scopeId)).fetchOptional());
    return optionalRecord.map(Record1::value1);
  }

  /**
   * Get workspace with secrets.
   *
   * @param workspaceId workspace id
   * @param includeTombstone include workspace even if it is tombstoned
   * @return workspace with secrets
   * @throws JsonValidationException if the workspace is or contains invalid json
   * @throws ConfigNotFoundException if the config does not exist
   * @throws IOException if there is an issue while interacting with the secrets store or db.
   */
  @Override
  public StandardWorkspace getWorkspaceWithSecrets(
                                                   final UUID workspaceId,
                                                   final boolean includeTombstone)
      throws JsonValidationException, ConfigNotFoundException, IOException {
    final StandardWorkspace workspace = getStandardWorkspaceNoSecrets(workspaceId, includeTombstone);
    final JsonNode webhookConfigs;
    final UUID organizationId = workspace.getOrganizationId();
    if (organizationId != null && featureFlagClient.boolVariation(UseRuntimeSecretPersistence.INSTANCE, new Organization(organizationId))) {
      final SecretPersistenceConfig secretPersistenceConfig =
          secretPersistenceConfigService.getSecretPersistenceConfig(io.airbyte.config.ScopeType.ORGANIZATION, organizationId);
      webhookConfigs =
          secretsRepositoryReader.hydrateConfigFromRuntimeSecretPersistence(workspace.getWebhookOperationConfigs(),
              new RuntimeSecretPersistence(secretPersistenceConfig));
    } else {
      webhookConfigs = secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(workspace.getWebhookOperationConfigs());
    }
    workspace.withWebhookOperationConfigs(webhookConfigs);
    return workspace;
  }

  @Override
  public void writeWorkspaceWithSecrets(final StandardWorkspace workspace) throws JsonValidationException, IOException, ConfigNotFoundException {
    // Get the schema for the webhook config, so we can split out any secret fields.
    final JsonNode webhookConfigSchema =
        Yamls.deserialize(ConfigSchema.WORKSPACE_WEBHOOK_OPERATION_CONFIGS.getConfigSchemaFile());
    // Check if there's an existing config, so we can re-use the secret coordinates.
    final Optional<StandardWorkspace> previousWorkspace = getWorkspaceIfExists(workspace.getWorkspaceId(), false);
    Optional<JsonNode> previousWebhookConfigs = Optional.empty();
    if (previousWorkspace.isPresent() && previousWorkspace.get().getWebhookOperationConfigs() != null) {
      previousWebhookConfigs = Optional.of(previousWorkspace.get().getWebhookOperationConfigs());
    }
    // Split out the secrets from the webhook config.
    final JsonNode partialConfig;
    if (workspace.getWebhookOperationConfigs() == null) {
      partialConfig = null;
    } else {
      // strip secrets
      final UUID organizationId = workspace.getOrganizationId();
      if (organizationId != null && featureFlagClient.boolVariation(UseRuntimeSecretPersistence.INSTANCE, new Organization(organizationId))) {
        final SecretPersistenceConfig secretPersistenceConfig =
            secretPersistenceConfigService.getSecretPersistenceConfig(io.airbyte.config.ScopeType.ORGANIZATION, organizationId);
        partialConfig = secretsRepositoryWriter.statefulUpdateSecretsToRuntimeSecretPersistence(
            workspace.getWorkspaceId(),
            previousWebhookConfigs,
            workspace.getWebhookOperationConfigs(),
            webhookConfigSchema,
            true,
            new RuntimeSecretPersistence(secretPersistenceConfig));
      } else {
        partialConfig = secretsRepositoryWriter.statefulUpdateSecretsToDefaultSecretPersistence(
            workspace.getWorkspaceId(),
            previousWebhookConfigs,
            workspace.getWebhookOperationConfigs(),
            webhookConfigSchema,
            true);
      }
    }
    final StandardWorkspace partialWorkspace = Jsons.clone(workspace);
    if (partialConfig != null) {
      partialWorkspace.withWebhookOperationConfigs(partialConfig);
    }
    writeStandardWorkspaceNoSecrets(partialWorkspace);
  }

  private Optional<StandardWorkspace> getWorkspaceIfExists(final UUID workspaceId, final boolean tombstone) {
    try {
      return Optional.of(getStandardWorkspaceNoSecrets(workspaceId, tombstone));
    } catch (final ConfigNotFoundException | JsonValidationException | IOException e) {
      log.warn("Unable to find workspace with ID {}", workspaceId);
      return Optional.empty();
    }
  }

}
