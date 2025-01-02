/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTIVE_DECLARATIVE_MANIFEST;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_CONFIG_INJECTION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_DEFINITION_VERSION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTOR_BUILDER_PROJECT;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.DECLARATIVE_MANIFEST;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.select;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ActiveDeclarativeManifest;
import io.airbyte.config.ActorDefinitionConfigInjection;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.ConnectorBuilderProject;
import io.airbyte.config.ConnectorBuilderProjectVersionedManifest;
import io.airbyte.config.DeclarativeManifest;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.ConnectorBuilderService;
import io.airbyte.db.Database;
import io.airbyte.db.ExceptionWrappingDatabase;
import io.airbyte.protocol.models.ConnectorSpecification;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.JoinType;
import org.jooq.Record;
import org.jooq.RecordMapper;
import org.jooq.exception.DataAccessException;

@Singleton
public class ConnectorBuilderServiceJooqImpl implements ConnectorBuilderService {

  private static final List<Field<?>> BASE_CONNECTOR_BUILDER_PROJECT_COLUMNS =
      Arrays.asList(CONNECTOR_BUILDER_PROJECT.ID, CONNECTOR_BUILDER_PROJECT.WORKSPACE_ID, CONNECTOR_BUILDER_PROJECT.NAME,
          CONNECTOR_BUILDER_PROJECT.ACTOR_DEFINITION_ID, CONNECTOR_BUILDER_PROJECT.TOMBSTONE, CONNECTOR_BUILDER_PROJECT.TESTING_VALUES,
          field(CONNECTOR_BUILDER_PROJECT.MANIFEST_DRAFT.isNotNull()).as("hasDraft"), CONNECTOR_BUILDER_PROJECT.BASE_ACTOR_DEFINITION_VERSION_ID,
          CONNECTOR_BUILDER_PROJECT.CONTRIBUTION_PULL_REQUEST_URL, CONNECTOR_BUILDER_PROJECT.CONTRIBUTION_ACTOR_DEFINITION_ID);

  private final ExceptionWrappingDatabase database;

  @VisibleForTesting
  public ConnectorBuilderServiceJooqImpl(@Named("configDatabase") final Database database) {
    this.database = new ExceptionWrappingDatabase(database);
  }

  /**
   * Get connector builder project.
   *
   * @param builderProjectId project id
   * @param fetchManifestDraft manifest draft
   * @return builder project
   * @throws IOException exception while interacting with db
   * @throws ConfigNotFoundException if build project is not found
   */
  @Override
  public ConnectorBuilderProject getConnectorBuilderProject(final UUID builderProjectId,
                                                            final boolean fetchManifestDraft)
      throws IOException, ConfigNotFoundException {
    final Optional<ConnectorBuilderProject> projectOptional = database.query(ctx -> {
      final List columnsToFetch =
          new ArrayList(BASE_CONNECTOR_BUILDER_PROJECT_COLUMNS);
      if (fetchManifestDraft) {
        columnsToFetch.add(CONNECTOR_BUILDER_PROJECT.MANIFEST_DRAFT);
      }
      final RecordMapper<Record, ConnectorBuilderProject> connectionBuilderProjectRecordMapper =
          fetchManifestDraft ? DbConverter::buildConnectorBuilderProject : DbConverter::buildConnectorBuilderProjectWithoutManifestDraft;
      return ctx.select(columnsToFetch)
          .select(ACTIVE_DECLARATIVE_MANIFEST.VERSION)
          .from(CONNECTOR_BUILDER_PROJECT)
          .leftJoin(ACTIVE_DECLARATIVE_MANIFEST).on(CONNECTOR_BUILDER_PROJECT.ACTOR_DEFINITION_ID.eq(ACTIVE_DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID))
          .where(CONNECTOR_BUILDER_PROJECT.ID.eq(builderProjectId).andNot(CONNECTOR_BUILDER_PROJECT.TOMBSTONE))
          .fetch()
          .map(connectionBuilderProjectRecordMapper)
          .stream()
          .findFirst();
    });
    return projectOptional.orElseThrow(() -> new ConfigNotFoundException(ConfigSchema.CONNECTOR_BUILDER_PROJECT, builderProjectId));
  }

  @Override
  public Optional<UUID> getConnectorBuilderProjectIdForActorDefinitionId(final UUID actorDefinitionId) throws IOException {
    return database.query(ctx -> ctx
        .select(CONNECTOR_BUILDER_PROJECT.ID)
        .from(CONNECTOR_BUILDER_PROJECT)
        .where(CONNECTOR_BUILDER_PROJECT.ACTOR_DEFINITION_ID.eq(actorDefinitionId)
            .andNot(CONNECTOR_BUILDER_PROJECT.TOMBSTONE))
        .fetchOptional(CONNECTOR_BUILDER_PROJECT.ID));
  }

  /**
   * Return a versioned manifest associated with a builder project.
   *
   * @param builderProjectId ID of the connector_builder_project
   * @param version the version of the manifest
   * @return ConnectorBuilderProjectVersionedManifest matching the builderProjectId
   * @throws ConfigNotFoundException ensures that there a connector_builder_project matching the
   *         `builderProjectId`, a declarative_manifest with the specified version associated with the
   *         builder project and an active_declarative_manifest. If either of these conditions is not
   *         true, this error is thrown
   * @throws IOException exception while interacting with db
   */
  @Override
  public ConnectorBuilderProjectVersionedManifest getVersionedConnectorBuilderProject(
                                                                                      final UUID builderProjectId,
                                                                                      final Long version)
      throws ConfigNotFoundException, IOException {
    final Optional<ConnectorBuilderProjectVersionedManifest> projectOptional = database.query(ctx -> ctx
        .select(CONNECTOR_BUILDER_PROJECT.ID,
            CONNECTOR_BUILDER_PROJECT.NAME,
            CONNECTOR_BUILDER_PROJECT.ACTOR_DEFINITION_ID,
            CONNECTOR_BUILDER_PROJECT.TESTING_VALUES,
            field(CONNECTOR_BUILDER_PROJECT.MANIFEST_DRAFT.isNotNull()).as("hasDraft"))
        .select(DECLARATIVE_MANIFEST.VERSION, DECLARATIVE_MANIFEST.DESCRIPTION, DECLARATIVE_MANIFEST.MANIFEST)
        .select(ACTIVE_DECLARATIVE_MANIFEST.VERSION)
        .from(CONNECTOR_BUILDER_PROJECT)
        .join(ACTIVE_DECLARATIVE_MANIFEST).on(CONNECTOR_BUILDER_PROJECT.ACTOR_DEFINITION_ID.eq(ACTIVE_DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID))
        .join(DECLARATIVE_MANIFEST).on(CONNECTOR_BUILDER_PROJECT.ACTOR_DEFINITION_ID.eq(DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID))
        .where(CONNECTOR_BUILDER_PROJECT.ID.eq(builderProjectId)
            .andNot(CONNECTOR_BUILDER_PROJECT.TOMBSTONE)
            .and(DECLARATIVE_MANIFEST.VERSION.eq(version)))
        .fetch()
        .map(ConnectorBuilderServiceJooqImpl::buildConnectorBuilderProjectVersionedManifest)
        .stream()
        .findFirst());
    return projectOptional.orElseThrow(() -> new ConfigNotFoundException(
        "CONNECTOR_BUILDER_PROJECTS/DECLARATIVE_MANIFEST/ACTIVE_DECLARATIVE_MANIFEST",
        String.format("connector_builder_projects.id:%s manifest_version:%s", builderProjectId, version)));
  }

  /**
   * Get connector builder project from a workspace id.
   *
   * @param workspaceId workspace id
   * @return builder project
   * @throws IOException exception while interacting with db
   */
  @Override
  public Stream<ConnectorBuilderProject> getConnectorBuilderProjectsByWorkspace(final UUID workspaceId)
      throws IOException {
    final Condition matchByWorkspace = CONNECTOR_BUILDER_PROJECT.WORKSPACE_ID.eq(workspaceId);

    return database
        .query(ctx -> ctx
            .select(BASE_CONNECTOR_BUILDER_PROJECT_COLUMNS)
            .select(ACTIVE_DECLARATIVE_MANIFEST.VERSION)
            .from(CONNECTOR_BUILDER_PROJECT)
            .leftJoin(ACTIVE_DECLARATIVE_MANIFEST)
            .on(CONNECTOR_BUILDER_PROJECT.ACTOR_DEFINITION_ID.eq(ACTIVE_DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID))
            .where(matchByWorkspace.andNot(CONNECTOR_BUILDER_PROJECT.TOMBSTONE))
            .orderBy(CONNECTOR_BUILDER_PROJECT.NAME.asc())
            .fetch())
        .map(DbConverter::buildConnectorBuilderProjectWithoutManifestDraft)
        .stream();
  }

  /**
   * Delete builder project.
   *
   * @param builderProjectId builder project to delete
   * @return true if successful
   * @throws IOException exception while interacting with db
   */
  @Override
  public boolean deleteBuilderProject(final UUID builderProjectId) throws IOException {
    return database.transaction(ctx -> ctx.update(CONNECTOR_BUILDER_PROJECT).set(CONNECTOR_BUILDER_PROJECT.TOMBSTONE, true)
        .set(CONNECTOR_BUILDER_PROJECT.UPDATED_AT, OffsetDateTime.now())
        .where(CONNECTOR_BUILDER_PROJECT.ID.eq(builderProjectId)).execute()) > 0;
  }

  /**
   * Write name and draft of a builder project. If it doesn't exist under the specified id, it is
   * created.
   *
   * @param projectId the id of the project
   * @param workspaceId the id of the workspace the project is associated with
   * @param name the name of the project
   * @param manifestDraft the manifest (can be null for no draft)
   * @throws IOException exception while interacting with db
   */
  @Override
  public void writeBuilderProjectDraft(final UUID projectId,
                                       final UUID workspaceId,
                                       final String name,
                                       final JsonNode manifestDraft,
                                       final UUID baseActorDefinitionVersionId,
                                       final String contributionUrl,
                                       final UUID contributionActorDefinitionId)
      throws IOException {
    database.transaction(ctx -> {
      writeBuilderProjectDraft(projectId, workspaceId, name, manifestDraft, baseActorDefinitionVersionId, contributionUrl,
          contributionActorDefinitionId, ctx);
      return null;
    });
  }

  /**
   * Nullify the manifest draft of a builder project.
   *
   * @param projectId the id of the project
   * @throws IOException exception while interacting with db
   */
  @Override
  public void deleteBuilderProjectDraft(final UUID projectId) throws IOException {
    database.transaction(ctx -> {
      ctx.update(CONNECTOR_BUILDER_PROJECT)
          .setNull(CONNECTOR_BUILDER_PROJECT.MANIFEST_DRAFT)
          .set(CONNECTOR_BUILDER_PROJECT.UPDATED_AT, OffsetDateTime.now())
          .where(CONNECTOR_BUILDER_PROJECT.ID.eq(projectId))
          .execute();
      return null;
    });
  }

  /**
   * Nullify the manifest draft of the builder project associated with the provided actor definition
   * ID and workspace ID.
   *
   * @param actorDefinitionId the id of the actor definition to which the project is linked
   * @param workspaceId the id of the workspace containing the project
   * @throws IOException exception while interacting with db
   */
  @Override
  public void deleteManifestDraftForActorDefinition(final UUID actorDefinitionId, final UUID workspaceId)
      throws IOException {
    database.transaction(ctx -> {
      ctx.update(CONNECTOR_BUILDER_PROJECT)
          .setNull(CONNECTOR_BUILDER_PROJECT.MANIFEST_DRAFT)
          .set(CONNECTOR_BUILDER_PROJECT.UPDATED_AT, OffsetDateTime.now())
          .where(CONNECTOR_BUILDER_PROJECT.ACTOR_DEFINITION_ID.eq(actorDefinitionId)
              .and(CONNECTOR_BUILDER_PROJECT.WORKSPACE_ID.eq(workspaceId)))
          .execute();
      return null;
    });
  }

  /**
   * Write name and draft of a builder project. The actor_definition is also updated to match the new
   * builder project name.
   * <p>
   * Actor definitions updated this way should always be private (i.e. public=false). As an additional
   * protection, we want to shield ourselves from users updating public actor definition and
   * therefore, the name of the actor definition won't be updated if the actor definition is not
   * public. See
   * https://github.com/airbytehq/airbyte-platform-internal/pull/5289#discussion_r1142757109.
   *
   * @param projectId the id of the project
   * @param workspaceId the id of the workspace the project is associated with
   * @param name the name of the project
   * @param manifestDraft the manifest (can be null for no draft)
   * @param actorDefinitionId the id of the associated actor definition
   * @throws IOException exception while interacting with db
   */
  @Override
  public void updateBuilderProjectAndActorDefinition(final UUID projectId,
                                                     final UUID workspaceId,
                                                     final String name,
                                                     final JsonNode manifestDraft,
                                                     final UUID baseActorDefinitionVersionId,
                                                     final String contributionPullRequestUrl,
                                                     final UUID contributionActorDefinitionId,
                                                     final UUID actorDefinitionId)
      throws IOException {
    database.transaction(ctx -> {
      writeBuilderProjectDraft(projectId, workspaceId, name, manifestDraft, baseActorDefinitionVersionId, contributionPullRequestUrl,
          contributionActorDefinitionId, ctx);
      ctx.update(ACTOR_DEFINITION)
          .set(ACTOR_DEFINITION.UPDATED_AT, OffsetDateTime.now())
          .set(ACTOR_DEFINITION.NAME, name)
          .where(ACTOR_DEFINITION.ID.eq(actorDefinitionId).and(ACTOR_DEFINITION.PUBLIC.eq(false)))
          .execute();
      return null;
    });
  }

  /**
   * Write a builder project to the db.
   *
   * @param builderProjectId builder project to update
   * @param actorDefinitionId the actor definition id associated with the connector builder project
   * @throws IOException exception while interacting with db
   */
  @Override
  public void assignActorDefinitionToConnectorBuilderProject(final UUID builderProjectId,
                                                             final UUID actorDefinitionId)
      throws IOException {
    database.transaction(ctx -> {
      ctx.update(CONNECTOR_BUILDER_PROJECT)
          .set(CONNECTOR_BUILDER_PROJECT.ACTOR_DEFINITION_ID, actorDefinitionId)
          .set(CONNECTOR_BUILDER_PROJECT.UPDATED_AT, OffsetDateTime.now())
          .where(CONNECTOR_BUILDER_PROJECT.ID.eq(builderProjectId))
          .execute();
      return null;
    });
  }

  /**
   * Update an actor_definition, active_declarative_manifest and create declarative_manifest.
   * <p>
   * Note that based on this signature, two problems might occur if the user of this method is not
   * diligent. This was done because we value more separation of concerns than consistency of the API
   * of this method. The problems are:
   *
   * <pre>
   * <ul>
   *   <li>DeclarativeManifest.manifest could be different from the one injected ActorDefinitionConfigInjection.</li>
   *   <li>DeclarativeManifest.spec could be different from ConnectorSpecification.connectionSpecification</li>
   * </ul>
   * </pre>
   * <p>
   * Since we decided not to validate this using the signature of the method, we will validate that
   * runtime and IllegalArgumentException if there is a mismatch.
   * <p>
   * The reasoning behind this reasoning is the following: Benefits: Alignment with platform's
   * definition of the repository. Drawbacks: We will need a method
   * configRepository.setDeclarativeSourceActiveVersion(sourceDefinitionId, version, manifest, spec);
   * where version and (manifest, spec) might not be consistent i.e. that a user of this method could
   * call it with configRepository.setDeclarativeSourceActiveVersion(sourceDefinitionId, version_10,
   * manifest_of_version_7, spec_of_version_12); However, we agreed that this was very unlikely.
   * <p>
   * Note that this is all in the context of data consistency i.e. that we want to do this in one
   * transaction. When we split this in many services, we will need to rethink data consistency.
   *
   * @param declarativeManifest declarative manifest version to create and make active
   * @param configInjection configInjection for the manifest
   * @param connectorSpecification connectorSpecification associated with the declarativeManifest
   *        being created
   * @throws IOException exception while interacting with db
   * @throws IllegalArgumentException if there is a mismatch between the different arguments
   */
  @Override
  public void createDeclarativeManifestAsActiveVersion(final DeclarativeManifest declarativeManifest,
                                                       final ActorDefinitionConfigInjection configInjection,
                                                       final ConnectorSpecification connectorSpecification,
                                                       final String cdkVersion)
      throws IOException {
    if (!declarativeManifest.getActorDefinitionId().equals(configInjection.getActorDefinitionId())) {
      throw new IllegalArgumentException("DeclarativeManifest.actorDefinitionId must match ActorDefinitionConfigInjection.actorDefinitionId");
    }
    if (!declarativeManifest.getManifest().equals(configInjection.getJsonToInject())) {
      throw new IllegalArgumentException("The DeclarativeManifest does not match the config injection");
    }
    if (!declarativeManifest.getSpec().get("connectionSpecification").equals(connectorSpecification.getConnectionSpecification())) {
      throw new IllegalArgumentException("DeclarativeManifest.spec must match ConnectorSpecification.connectionSpecification");
    }

    database.transaction(ctx -> {
      updateDeclarativeActorDefinitionVersion(configInjection, connectorSpecification, cdkVersion, ctx);
      insertActiveDeclarativeManifest(declarativeManifest, ctx);
      return null;
    });
  }

  /**
   * Update an actor_definition, active_declarative_manifest and create declarative_manifest.
   * <p>
   * Note that based on this signature, two problems might occur if the user of this method is not
   * diligent. This was done because we value more separation of concerns than consistency of the API
   * of this method. The problems are:
   *
   * <pre>
   * <ul>
   *   <li>DeclarativeManifest.manifest could be different from the one injected ActorDefinitionConfigInjection.</li>
   *   <li>DeclarativeManifest.spec could be different from ConnectorSpecification.connectionSpecification</li>
   * </ul>
   * </pre>
   * <p>
   * At that point, we can only hope the user won't cause data consistency issue using this method
   * <p>
   * The reasoning behind this reasoning is the following: Benefits: Alignment with platform's
   * definition of the repository. Drawbacks: We will need a method
   * configRepository.setDeclarativeSourceActiveVersion(sourceDefinitionId, version, manifest, spec);
   * where version and (manifest, spec) might not be consistent i.e. that a user of this method could
   * call it with configRepository.setDeclarativeSourceActiveVersion(sourceDefinitionId, version_10,
   * manifest_of_version_7, spec_of_version_12); However, we agreed that this was very unlikely.
   * <p>
   * Note that this is all in the context of data consistency i.e. that we want to do this in one
   * transaction. When we split this in many services, we will need to rethink data consistency.
   *
   * @param sourceDefinitionId actor definition to update
   * @param version the version of the manifest to make active. declarative_manifest.version must
   *        already exist
   * @param configInjection configInjection for the manifest
   * @param connectorSpecification connectorSpecification associated with the declarativeManifest
   *        being made active
   * @throws IOException exception while interacting with db
   */
  @Override
  public void setDeclarativeSourceActiveVersion(final UUID sourceDefinitionId,
                                                final Long version,
                                                final ActorDefinitionConfigInjection configInjection,
                                                final ConnectorSpecification connectorSpecification,
                                                final String cdkVersion)
      throws IOException {
    database.transaction(ctx -> {
      updateDeclarativeActorDefinitionVersion(configInjection, connectorSpecification, cdkVersion, ctx);
      upsertActiveDeclarativeManifest(new ActiveDeclarativeManifest().withActorDefinitionId(sourceDefinitionId).withVersion(version), ctx);
      return null;
    });
  }

  /**
   * Load all config injection for an actor definition.
   *
   * @param actorDefinitionId id of the actor definition to fetch
   * @return stream of config injection objects
   * @throws IOException exception while interacting with db
   */
  @Override
  public Stream<ActorDefinitionConfigInjection> getActorDefinitionConfigInjections(
                                                                                   final UUID actorDefinitionId)
      throws IOException {
    return database.query(ctx -> ctx.select(ACTOR_DEFINITION_CONFIG_INJECTION.asterisk())
        .from(ACTOR_DEFINITION_CONFIG_INJECTION)
        .where(ACTOR_DEFINITION_CONFIG_INJECTION.ACTOR_DEFINITION_ID.eq(actorDefinitionId))
        .fetch())
        .map(DbConverter::buildActorDefinitionConfigInjection)
        .stream();
  }

  /**
   * Update or create a config injection object. If there is an existing config injection for the
   * given actor definition and path, it is updated. If there isn't yet, a new config injection is
   * created.
   *
   * @param actorDefinitionConfigInjection the config injection object to write to the database
   * @throws IOException exception while interacting with db
   */
  @Override
  public void writeActorDefinitionConfigInjectionForPath(
                                                         final ActorDefinitionConfigInjection actorDefinitionConfigInjection)
      throws IOException {
    database.transaction(ctx -> {
      writeActorDefinitionConfigInjectionForPath(actorDefinitionConfigInjection, ctx);
      return null;
    });
  }

  /**
   * Insert a declarative manifest. If DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID and
   * DECLARATIVE_MANIFEST.VERSION is already in the DB, an exception will be thrown
   *
   * @param declarativeManifest declarative manifest to insert
   * @throws IOException exception while interacting with db
   */
  @Override
  public void insertDeclarativeManifest(final DeclarativeManifest declarativeManifest)
      throws IOException {
    database.transaction(ctx -> {
      insertDeclarativeManifest(declarativeManifest, ctx);
      return null;
    });
  }

  /**
   * Insert a declarative manifest and its associated active declarative manifest. If
   * DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID and DECLARATIVE_MANIFEST.VERSION is already in the DB,
   * an exception will be thrown
   *
   * @param declarativeManifest declarative manifest to insert
   * @throws IOException exception while interacting with db
   */
  @Override
  public void insertActiveDeclarativeManifest(final DeclarativeManifest declarativeManifest)
      throws IOException {
    database.transaction(ctx -> {
      insertDeclarativeManifest(declarativeManifest, ctx);
      upsertActiveDeclarativeManifest(new ActiveDeclarativeManifest().withActorDefinitionId(declarativeManifest.getActorDefinitionId())
          .withVersion(declarativeManifest.getVersion()), ctx);
      return null;
    });
  }

  /**
   * Read all declarative manifests by actor definition id without the manifest column.
   *
   * @param actorDefinitionId actor definition id
   * @throws IOException exception while interacting with db
   */
  @Override
  public Stream<DeclarativeManifest> getDeclarativeManifestsByActorDefinitionId(
                                                                                final UUID actorDefinitionId)
      throws IOException {
    return database
        .query(ctx -> ctx
            .select(DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID, DECLARATIVE_MANIFEST.DESCRIPTION, DECLARATIVE_MANIFEST.SPEC,
                DECLARATIVE_MANIFEST.VERSION)
            .from(DECLARATIVE_MANIFEST)
            .where(DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID.eq(actorDefinitionId))
            .fetch())
        .map(DbConverter::buildDeclarativeManifestWithoutManifestAndSpec)
        .stream();
  }

  /**
   * Read declarative manifest by actor definition id and version with manifest column.
   *
   * @param actorDefinitionId actor definition id
   * @param version the version of the declarative manifest
   * @throws IOException exception while interacting with db
   * @throws ConfigNotFoundException exception if no match on DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID
   *         and DECLARATIVE_MANIFEST.VERSION
   */
  @Override
  public DeclarativeManifest getDeclarativeManifestByActorDefinitionIdAndVersion(
                                                                                 final UUID actorDefinitionId,
                                                                                 final long version)
      throws IOException, ConfigNotFoundException {
    final Optional<DeclarativeManifest> declarativeManifest = database
        .query(ctx -> ctx
            .select(DECLARATIVE_MANIFEST.asterisk())
            .from(DECLARATIVE_MANIFEST)
            .where(DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID.eq(actorDefinitionId).and(DECLARATIVE_MANIFEST.VERSION.eq(version)))
            .fetch())
        .map(DbConverter::buildDeclarativeManifest)
        .stream().findFirst();
    return declarativeManifest.orElseThrow(() -> new ConfigNotFoundException(ConfigSchema.DECLARATIVE_MANIFEST,
        String.format("actorDefinitionId:%s,version:%s", actorDefinitionId, version)));
  }

  /**
   * Read currently active declarative manifest by actor definition id by joining with
   * active_declarative_manifest for the same actor definition id with manifest.
   *
   * @param actorDefinitionId actor definition id
   * @throws IOException exception while interacting with db
   * @throws ConfigNotFoundException exception if no match on DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID
   *         that matches the version of an active manifest
   */
  @Override
  public DeclarativeManifest getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(
                                                                                       final UUID actorDefinitionId)
      throws IOException, ConfigNotFoundException {
    final Optional<DeclarativeManifest> declarativeManifest = database
        .query(ctx -> ctx
            .select(DECLARATIVE_MANIFEST.asterisk())
            .from(DECLARATIVE_MANIFEST)
            .join(ACTIVE_DECLARATIVE_MANIFEST, JoinType.JOIN)
            .on(DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID.eq(ACTIVE_DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID),
                DECLARATIVE_MANIFEST.VERSION.eq(ACTIVE_DECLARATIVE_MANIFEST.VERSION))
            .where(ACTIVE_DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID.eq(actorDefinitionId))
            .fetch())
        .map(DbConverter::buildDeclarativeManifest)
        .stream().findFirst();
    return declarativeManifest.orElseThrow(() -> new ConfigNotFoundException(ConfigSchema.DECLARATIVE_MANIFEST,
        String.format("ACTIVE_DECLARATIVE_MANIFEST.actor_definition_id:%s and matching DECLARATIVE_MANIFEST.version", actorDefinitionId)));
  }

  /**
   * Update the testing values of a connector builder project.
   *
   * @param projectId builder project to update
   * @param testingValues testing values to set on the project
   * @throws IOException exception while interacting with db
   */
  @Override
  public void updateBuilderProjectTestingValues(final UUID projectId, final JsonNode testingValues) throws IOException {
    final OffsetDateTime timestamp = OffsetDateTime.now();
    database.transaction(ctx -> ctx.update(CONNECTOR_BUILDER_PROJECT)
        .set(CONNECTOR_BUILDER_PROJECT.TESTING_VALUES, JSONB.valueOf(Jsons.serialize(testingValues)))
        .set(CONNECTOR_BUILDER_PROJECT.UPDATED_AT, timestamp)
        .where(CONNECTOR_BUILDER_PROJECT.ID.eq(projectId))
        .execute());
  }

  private void writeBuilderProjectDraft(final UUID projectId,
                                        final UUID workspaceId,
                                        final String name,
                                        final JsonNode manifestDraft,
                                        final UUID baseActorDefinitionVersionId,
                                        final String contributionPullRequestUrl,
                                        final UUID contributionActorDefinitionId,
                                        final DSLContext ctx) {
    final OffsetDateTime timestamp = OffsetDateTime.now();
    final Condition matchId = CONNECTOR_BUILDER_PROJECT.ID.eq(projectId);
    final boolean isExistingConfig = ctx.fetchExists(select()
        .from(CONNECTOR_BUILDER_PROJECT)
        .where(matchId));

    if (isExistingConfig) {
      ctx.update(CONNECTOR_BUILDER_PROJECT)
          .set(CONNECTOR_BUILDER_PROJECT.ID, projectId)
          .set(CONNECTOR_BUILDER_PROJECT.WORKSPACE_ID, workspaceId)
          .set(CONNECTOR_BUILDER_PROJECT.NAME, name)
          .set(CONNECTOR_BUILDER_PROJECT.MANIFEST_DRAFT,
              manifestDraft != null ? JSONB.valueOf(Jsons.serialize(manifestDraft)) : null)
          .set(CONNECTOR_BUILDER_PROJECT.BASE_ACTOR_DEFINITION_VERSION_ID, baseActorDefinitionVersionId)
          .set(CONNECTOR_BUILDER_PROJECT.CONTRIBUTION_PULL_REQUEST_URL, contributionPullRequestUrl)
          .set(CONNECTOR_BUILDER_PROJECT.CONTRIBUTION_ACTOR_DEFINITION_ID, contributionActorDefinitionId)
          .set(CONNECTOR_BUILDER_PROJECT.UPDATED_AT, timestamp)
          .where(matchId)
          .execute();
    } else {
      ctx.insertInto(CONNECTOR_BUILDER_PROJECT)
          .set(CONNECTOR_BUILDER_PROJECT.ID, projectId)
          .set(CONNECTOR_BUILDER_PROJECT.WORKSPACE_ID, workspaceId)
          .set(CONNECTOR_BUILDER_PROJECT.NAME, name)
          .set(CONNECTOR_BUILDER_PROJECT.MANIFEST_DRAFT,
              manifestDraft != null ? JSONB.valueOf(Jsons.serialize(manifestDraft)) : null)
          .set(CONNECTOR_BUILDER_PROJECT.BASE_ACTOR_DEFINITION_VERSION_ID, baseActorDefinitionVersionId)
          .set(CONNECTOR_BUILDER_PROJECT.CONTRIBUTION_PULL_REQUEST_URL, contributionPullRequestUrl)
          .set(CONNECTOR_BUILDER_PROJECT.CONTRIBUTION_ACTOR_DEFINITION_ID, contributionActorDefinitionId)
          .set(CONNECTOR_BUILDER_PROJECT.CREATED_AT, timestamp)
          .set(CONNECTOR_BUILDER_PROJECT.UPDATED_AT, timestamp)
          .set(CONNECTOR_BUILDER_PROJECT.TOMBSTONE, false)
          .execute();
    }
  }

  private void insertActiveDeclarativeManifest(final DeclarativeManifest declarativeManifest, final DSLContext ctx) {
    insertDeclarativeManifest(declarativeManifest, ctx);
    upsertActiveDeclarativeManifest(new ActiveDeclarativeManifest().withActorDefinitionId(declarativeManifest.getActorDefinitionId())
        .withVersion(declarativeManifest.getVersion()), ctx);
  }

  private static void insertDeclarativeManifest(final DeclarativeManifest declarativeManifest, final DSLContext ctx) {
    // Since "null" is a valid JSON object, `JSONB.valueOf(Jsons.serialize(null))` returns a valid JSON
    // object that is not null. Therefore, we will validate null values for JSON fields here
    if (declarativeManifest.getManifest() == null) {
      throw new DataAccessException("null value in column \"manifest\" of relation \"declarative_manifest\" violates not-null constraint");
    }
    if (declarativeManifest.getSpec() == null) {
      throw new DataAccessException("null value in column \"spec\" of relation \"declarative_manifest\" violates not-null constraint");
    }

    final OffsetDateTime timestamp = OffsetDateTime.now();
    ctx.insertInto(DECLARATIVE_MANIFEST)
        .set(DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID, declarativeManifest.getActorDefinitionId())
        .set(DECLARATIVE_MANIFEST.DESCRIPTION, declarativeManifest.getDescription())
        .set(DECLARATIVE_MANIFEST.MANIFEST, JSONB.valueOf(Jsons.serialize(declarativeManifest.getManifest())))
        .set(DECLARATIVE_MANIFEST.SPEC, JSONB.valueOf(Jsons.serialize(declarativeManifest.getSpec())))
        .set(DECLARATIVE_MANIFEST.VERSION, declarativeManifest.getVersion())
        .set(DECLARATIVE_MANIFEST.CREATED_AT, timestamp)
        .execute();
  }

  private void updateDeclarativeActorDefinitionVersion(final ActorDefinitionConfigInjection configInjection,
                                                       final ConnectorSpecification spec,
                                                       final String cdkVersion,
                                                       final DSLContext ctx) {
    // We are updating the same version since connector builder projects have a different concept of
    // versioning.
    ctx.update(ACTOR_DEFINITION_VERSION)
        .set(ACTOR_DEFINITION_VERSION.UPDATED_AT, OffsetDateTime.now())
        .set(ACTOR_DEFINITION_VERSION.SPEC, JSONB.valueOf(Jsons.serialize(spec)))
        .set(ACTOR_DEFINITION_VERSION.DOCKER_IMAGE_TAG, cdkVersion)
        .where(ACTOR_DEFINITION_VERSION.ACTOR_DEFINITION_ID.eq(configInjection.getActorDefinitionId()))
        .execute();

    writeActorDefinitionConfigInjectionForPath(configInjection, ctx);
  }

  private void upsertActiveDeclarativeManifest(final ActiveDeclarativeManifest activeDeclarativeManifest, final DSLContext ctx) {
    final OffsetDateTime timestamp = OffsetDateTime.now();
    final Condition matchId = ACTIVE_DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID.eq(activeDeclarativeManifest.getActorDefinitionId());
    final boolean isExistingConfig = ctx.fetchExists(select()
        .from(ACTIVE_DECLARATIVE_MANIFEST)
        .where(matchId));

    if (isExistingConfig) {
      ctx.update(ACTIVE_DECLARATIVE_MANIFEST)
          .set(ACTIVE_DECLARATIVE_MANIFEST.VERSION, activeDeclarativeManifest.getVersion())
          .set(ACTIVE_DECLARATIVE_MANIFEST.UPDATED_AT, timestamp)
          .where(matchId)
          .execute();
    } else {
      ctx.insertInto(ACTIVE_DECLARATIVE_MANIFEST)
          .set(ACTIVE_DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID, activeDeclarativeManifest.getActorDefinitionId())
          .set(ACTIVE_DECLARATIVE_MANIFEST.VERSION, activeDeclarativeManifest.getVersion())
          .set(ACTIVE_DECLARATIVE_MANIFEST.CREATED_AT, timestamp)
          .set(ACTIVE_DECLARATIVE_MANIFEST.UPDATED_AT, timestamp)
          .execute();
    }
  }

  private void writeActorDefinitionConfigInjectionForPath(final ActorDefinitionConfigInjection actorDefinitionConfigInjection, final DSLContext ctx) {
    final OffsetDateTime timestamp = OffsetDateTime.now();
    final Condition matchActorDefinitionIdAndInjectionPath =
        ACTOR_DEFINITION_CONFIG_INJECTION.ACTOR_DEFINITION_ID.eq(actorDefinitionConfigInjection.getActorDefinitionId())
            .and(ACTOR_DEFINITION_CONFIG_INJECTION.INJECTION_PATH.eq(actorDefinitionConfigInjection.getInjectionPath()));
    final boolean isExistingConfig = ctx.fetchExists(select()
        .from(ACTOR_DEFINITION_CONFIG_INJECTION)
        .where(matchActorDefinitionIdAndInjectionPath));

    if (isExistingConfig) {
      ctx.update(ACTOR_DEFINITION_CONFIG_INJECTION)
          .set(ACTOR_DEFINITION_CONFIG_INJECTION.JSON_TO_INJECT, JSONB.valueOf(Jsons.serialize(actorDefinitionConfigInjection.getJsonToInject())))
          .set(ACTOR_DEFINITION_CONFIG_INJECTION.UPDATED_AT, timestamp)
          .where(matchActorDefinitionIdAndInjectionPath)
          .execute();
    } else {
      ctx.insertInto(ACTOR_DEFINITION_CONFIG_INJECTION)
          .set(ACTOR_DEFINITION_CONFIG_INJECTION.INJECTION_PATH, actorDefinitionConfigInjection.getInjectionPath())
          .set(ACTOR_DEFINITION_CONFIG_INJECTION.ACTOR_DEFINITION_ID, actorDefinitionConfigInjection.getActorDefinitionId())
          .set(ACTOR_DEFINITION_CONFIG_INJECTION.JSON_TO_INJECT, JSONB.valueOf(Jsons.serialize(actorDefinitionConfigInjection.getJsonToInject())))
          .set(ACTOR_DEFINITION_CONFIG_INJECTION.CREATED_AT, timestamp)
          .set(ACTOR_DEFINITION_CONFIG_INJECTION.UPDATED_AT, timestamp)
          .execute();
    }
  }

  private static ConnectorBuilderProjectVersionedManifest buildConnectorBuilderProjectVersionedManifest(final Record record) {
    return new ConnectorBuilderProjectVersionedManifest()
        .withName(record.get(CONNECTOR_BUILDER_PROJECT.NAME))
        .withBuilderProjectId(record.get(CONNECTOR_BUILDER_PROJECT.ID))
        .withHasDraft((Boolean) record.get("hasDraft"))
        .withSourceDefinitionId(record.get(CONNECTOR_BUILDER_PROJECT.ACTOR_DEFINITION_ID))
        .withActiveDeclarativeManifestVersion(record.get(ACTIVE_DECLARATIVE_MANIFEST.VERSION))
        .withManifest(Jsons.deserialize(record.get(DECLARATIVE_MANIFEST.MANIFEST).data()))
        .withManifestVersion(record.get(DECLARATIVE_MANIFEST.VERSION))
        .withManifestDescription(record.get(DECLARATIVE_MANIFEST.DESCRIPTION));
  }

}
