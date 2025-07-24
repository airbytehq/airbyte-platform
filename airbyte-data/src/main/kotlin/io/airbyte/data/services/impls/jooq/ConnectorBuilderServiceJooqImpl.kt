/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ActiveDeclarativeManifest
import io.airbyte.config.ActorDefinitionConfigInjection
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.ConnectorBuilderProject
import io.airbyte.config.ConnectorBuilderProjectVersionedManifest
import io.airbyte.config.DeclarativeManifest
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ConnectorBuilderService
import io.airbyte.db.Database
import io.airbyte.db.ExceptionWrappingDatabase
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.protocol.models.v0.ConnectorSpecification
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.jooq.DSLContext
import org.jooq.Field
import org.jooq.JSONB
import org.jooq.JoinType
import org.jooq.Record
import org.jooq.Record5
import org.jooq.RecordMapper
import org.jooq.exception.DataAccessException
import org.jooq.impl.DSL
import java.io.IOException
import java.time.OffsetDateTime
import java.util.Arrays
import java.util.Optional
import java.util.UUID
import java.util.stream.Stream

@Singleton
class ConnectorBuilderServiceJooqImpl
  @VisibleForTesting
  constructor(
    @Named("configDatabase") database: Database?,
  ) : ConnectorBuilderService {
    private val database = ExceptionWrappingDatabase(database)

    /**
     * Get connector builder project.
     *
     * @param builderProjectId project id
     * @param fetchManifestDraft manifest draft
     * @return builder project
     * @throws IOException exception while interacting with db
     * @throws ConfigNotFoundException if build project is not found
     */
    @Throws(IOException::class, ConfigNotFoundException::class)
    override fun getConnectorBuilderProject(
      builderProjectId: UUID,
      fetchManifestDraft: Boolean,
    ): ConnectorBuilderProject {
      val projectOptional: Optional<ConnectorBuilderProject> =
        database.query { ctx: DSLContext ->
          val columnsToFetch: MutableList<Field<*>> = ArrayList(BASE_CONNECTOR_BUILDER_PROJECT_COLUMNS)
          if (fetchManifestDraft) {
            columnsToFetch.add(Tables.CONNECTOR_BUILDER_PROJECT.MANIFEST_DRAFT)
          }

          val connectionBuilderProjectRecordMapper =
            if (fetchManifestDraft) {
              RecordMapper { record: Record -> DbConverter.buildConnectorBuilderProject(record) }
            } else {
              RecordMapper { record: Record -> DbConverter.buildConnectorBuilderProjectWithoutManifestDraft(record) }
            }

          ctx
            .select(columnsToFetch)
            .select(Tables.ACTIVE_DECLARATIVE_MANIFEST.VERSION)
            .from(Tables.CONNECTOR_BUILDER_PROJECT)
            .leftJoin(Tables.ACTIVE_DECLARATIVE_MANIFEST)
            .on(Tables.CONNECTOR_BUILDER_PROJECT.ACTOR_DEFINITION_ID.eq(Tables.ACTIVE_DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID))
            .where(
              Tables.CONNECTOR_BUILDER_PROJECT.ID
                .eq(builderProjectId)
                .andNot(Tables.CONNECTOR_BUILDER_PROJECT.TOMBSTONE),
            ).fetch()
            .map(connectionBuilderProjectRecordMapper)
            .stream()
            .findFirst()
        }
      return projectOptional.orElseThrow {
        ConfigNotFoundException(
          ConfigNotFoundType.CONNECTOR_BUILDER_PROJECT,
          builderProjectId,
        )
      }
    }

    @Throws(IOException::class)
    override fun getConnectorBuilderProjectIdForActorDefinitionId(actorDefinitionId: UUID): Optional<UUID> =
      database.query { ctx: DSLContext ->
        ctx
          .select(Tables.CONNECTOR_BUILDER_PROJECT.ID)
          .from(Tables.CONNECTOR_BUILDER_PROJECT)
          .where(
            Tables.CONNECTOR_BUILDER_PROJECT.ACTOR_DEFINITION_ID
              .eq(actorDefinitionId)
              .andNot(Tables.CONNECTOR_BUILDER_PROJECT.TOMBSTONE),
          ).fetchOptional(Tables.CONNECTOR_BUILDER_PROJECT.ID)
      }

    /**
     * Return a versioned manifest associated with a builder project.
     *
     * @param builderProjectId ID of the connector_builder_project
     * @param version the version of the manifest
     * @return ConnectorBuilderProjectVersionedManifest matching the builderProjectId
     * @throws ConfigNotFoundException ensures that there a connector_builder_project matching the
     * `builderProjectId`, a declarative_manifest with the specified version associated with the
     * builder project and an active_declarative_manifest. If either of these conditions is not
     * true, this error is thrown
     * @throws IOException exception while interacting with db
     */
    @Throws(ConfigNotFoundException::class, IOException::class)
    override fun getVersionedConnectorBuilderProject(
      builderProjectId: UUID,
      version: Long,
    ): ConnectorBuilderProjectVersionedManifest {
      val projectOptional =
        database.query { ctx: DSLContext ->
          ctx
            .select(
              Tables.CONNECTOR_BUILDER_PROJECT.ID,
              Tables.CONNECTOR_BUILDER_PROJECT.NAME,
              Tables.CONNECTOR_BUILDER_PROJECT.ACTOR_DEFINITION_ID,
              Tables.CONNECTOR_BUILDER_PROJECT.TESTING_VALUES,
              Tables.CONNECTOR_BUILDER_PROJECT.COMPONENTS_FILE_CONTENT,
              DSL
                .field(Tables.CONNECTOR_BUILDER_PROJECT.MANIFEST_DRAFT.isNotNull())
                .`as`("hasDraft"),
            ).select(
              Tables.DECLARATIVE_MANIFEST.VERSION,
              Tables.DECLARATIVE_MANIFEST.DESCRIPTION,
              Tables.DECLARATIVE_MANIFEST.MANIFEST,
              Tables.DECLARATIVE_MANIFEST.COMPONENTS_FILE_CONTENT,
            ).select(Tables.ACTIVE_DECLARATIVE_MANIFEST.VERSION)
            .from(Tables.CONNECTOR_BUILDER_PROJECT)
            .join(Tables.ACTIVE_DECLARATIVE_MANIFEST)
            .on(Tables.CONNECTOR_BUILDER_PROJECT.ACTOR_DEFINITION_ID.eq(Tables.ACTIVE_DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID))
            .join(Tables.DECLARATIVE_MANIFEST)
            .on(Tables.CONNECTOR_BUILDER_PROJECT.ACTOR_DEFINITION_ID.eq(Tables.DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID))
            .where(
              Tables.CONNECTOR_BUILDER_PROJECT.ID
                .eq(builderProjectId)
                .andNot(Tables.CONNECTOR_BUILDER_PROJECT.TOMBSTONE)
                .and(Tables.DECLARATIVE_MANIFEST.VERSION.eq(version)),
            ).fetch()
            .map { record: Record ->
              buildConnectorBuilderProjectVersionedManifest(
                record,
              )
            }.stream()
            .findFirst()
        }
      return projectOptional.orElseThrow {
        ConfigNotFoundException(
          "CONNECTOR_BUILDER_PROJECTS/DECLARATIVE_MANIFEST/ACTIVE_DECLARATIVE_MANIFEST",
          String.format("connector_builder_projects.id:%s manifest_version:%s", builderProjectId, version),
        )
      }
    }

    /**
     * Get connector builder project from a workspace id.
     *
     * @param workspaceId workspace id
     * @return builder project
     * @throws IOException exception while interacting with db
     */
    @Throws(IOException::class)
    override fun getConnectorBuilderProjectsByWorkspace(workspaceId: UUID): Stream<ConnectorBuilderProject> {
      val matchByWorkspace = Tables.CONNECTOR_BUILDER_PROJECT.WORKSPACE_ID.eq(workspaceId)

      return database
        .query { ctx: DSLContext ->
          ctx
            .select(BASE_CONNECTOR_BUILDER_PROJECT_COLUMNS)
            .select(Tables.ACTIVE_DECLARATIVE_MANIFEST.VERSION)
            .from(Tables.CONNECTOR_BUILDER_PROJECT)
            .leftJoin(Tables.ACTIVE_DECLARATIVE_MANIFEST)
            .on(Tables.CONNECTOR_BUILDER_PROJECT.ACTOR_DEFINITION_ID.eq(Tables.ACTIVE_DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID))
            .where(matchByWorkspace.andNot(Tables.CONNECTOR_BUILDER_PROJECT.TOMBSTONE))
            .orderBy(Tables.CONNECTOR_BUILDER_PROJECT.NAME.asc())
            .fetch()
        }.map { record: Record -> DbConverter.buildConnectorBuilderProjectWithoutManifestDraft(record) }
        .stream()
    }

    /**
     * Delete builder project.
     *
     * @param builderProjectId builder project to delete
     * @return true if successful
     * @throws IOException exception while interacting with db
     */
    @Throws(IOException::class)
    override fun deleteBuilderProject(builderProjectId: UUID): Boolean =
      database.transaction { ctx: DSLContext ->
        ctx
          .update(Tables.CONNECTOR_BUILDER_PROJECT)
          .set(Tables.CONNECTOR_BUILDER_PROJECT.TOMBSTONE, true)
          .set(
            Tables.CONNECTOR_BUILDER_PROJECT.UPDATED_AT,
            OffsetDateTime.now(),
          ).where(Tables.CONNECTOR_BUILDER_PROJECT.ID.eq(builderProjectId))
          .execute()
      } > 0

    /**
     * Write name and draft of a builder project. If it doesn't exist under the specified id, it is
     * created.
     *
     * @param projectId the id of the project
     * @param workspaceId the id of the workspace the project is associated with
     * @param name the name of the project
     * @param manifestDraft the manifest (can be null for no draft)
     * @param componentsFileContent the content of the components file (can be null)
     * @throws IOException exception while interacting with db
     */
    @Throws(IOException::class)
    override fun writeBuilderProjectDraft(
      projectId: UUID,
      workspaceId: UUID,
      name: String,
      manifestDraft: JsonNode?,
      componentsFileContent: String?,
      baseActorDefinitionVersionId: UUID?,
      contributionUrl: String?,
      contributionActorDefinitionId: UUID?,
    ) {
      database.transaction<Any?> { ctx: DSLContext ->
        writeBuilderProjectDraft(
          projectId,
          workspaceId,
          name,
          manifestDraft,
          componentsFileContent,
          baseActorDefinitionVersionId,
          contributionUrl,
          contributionActorDefinitionId,
          ctx,
        )
        null
      }
    }

    /**
     * Nullify the manifest draft of a builder project.
     *
     * @param projectId the id of the project
     * @throws IOException exception while interacting with db
     */
    @Throws(IOException::class)
    override fun deleteBuilderProjectDraft(projectId: UUID) {
      database.transaction<Any?> { ctx: DSLContext ->
        ctx
          .update(Tables.CONNECTOR_BUILDER_PROJECT)
          .setNull(Tables.CONNECTOR_BUILDER_PROJECT.MANIFEST_DRAFT)
          .setNull(Tables.CONNECTOR_BUILDER_PROJECT.COMPONENTS_FILE_CONTENT)
          .set(
            Tables.CONNECTOR_BUILDER_PROJECT.UPDATED_AT,
            OffsetDateTime.now(),
          ).where(Tables.CONNECTOR_BUILDER_PROJECT.ID.eq(projectId))
          .execute()
        null
      }
    }

    /**
     * Nullify the manifest draft of the builder project associated with the provided actor definition
     * ID and workspace ID.
     *
     * @param actorDefinitionId the id of the actor definition to which the project is linked
     * @param workspaceId the id of the workspace containing the project
     * @throws IOException exception while interacting with db
     */
    @Throws(IOException::class)
    override fun deleteManifestDraftForActorDefinition(
      actorDefinitionId: UUID,
      workspaceId: UUID,
    ) {
      database.transaction<Any?> { ctx: DSLContext ->
        ctx
          .update(Tables.CONNECTOR_BUILDER_PROJECT)
          .setNull(Tables.CONNECTOR_BUILDER_PROJECT.MANIFEST_DRAFT)
          .setNull(Tables.CONNECTOR_BUILDER_PROJECT.COMPONENTS_FILE_CONTENT)
          .set(
            Tables.CONNECTOR_BUILDER_PROJECT.UPDATED_AT,
            OffsetDateTime.now(),
          ).where(
            Tables.CONNECTOR_BUILDER_PROJECT.ACTOR_DEFINITION_ID
              .eq(actorDefinitionId)
              .and(Tables.CONNECTOR_BUILDER_PROJECT.WORKSPACE_ID.eq(workspaceId)),
          ).execute()
        null
      }
    }

    /**
     * Write name and draft of a builder project. The actor_definition is also updated to match the new
     * builder project name.
     *
     *
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
     * @param componentsFileContent the content of the components file (can be null)
     * @param actorDefinitionId the id of the associated actor definition
     * @throws IOException exception while interacting with db
     */
    @Throws(IOException::class)
    override fun updateBuilderProjectAndActorDefinition(
      projectId: UUID,
      workspaceId: UUID,
      name: String,
      manifestDraft: JsonNode?,
      componentsFileContent: String?,
      baseActorDefinitionVersionId: UUID?,
      contributionPullRequestUrl: String?,
      contributionActorDefinitionId: UUID?,
      actorDefinitionId: UUID,
    ) {
      database.transaction<Any?> { ctx: DSLContext ->
        writeBuilderProjectDraft(
          projectId,
          workspaceId,
          name,
          manifestDraft,
          componentsFileContent,
          baseActorDefinitionVersionId,
          contributionPullRequestUrl,
          contributionActorDefinitionId,
          ctx,
        )
        ctx
          .update(Tables.ACTOR_DEFINITION)
          .set(
            Tables.ACTOR_DEFINITION.UPDATED_AT,
            OffsetDateTime.now(),
          ).set(Tables.ACTOR_DEFINITION.NAME, name)
          .where(
            Tables.ACTOR_DEFINITION.ID
              .eq(actorDefinitionId)
              .and(Tables.ACTOR_DEFINITION.PUBLIC.eq(false)),
          ).execute()
        null
      }
    }

    /**
     * Write a builder project to the db.
     *
     * @param builderProjectId builder project to update
     * @param actorDefinitionId the actor definition id associated with the connector builder project
     * @throws IOException exception while interacting with db
     */
    @Throws(IOException::class)
    override fun assignActorDefinitionToConnectorBuilderProject(
      builderProjectId: UUID,
      actorDefinitionId: UUID,
    ) {
      database.transaction<Any?> { ctx: DSLContext ->
        ctx
          .update(Tables.CONNECTOR_BUILDER_PROJECT)
          .set(
            Tables.CONNECTOR_BUILDER_PROJECT.ACTOR_DEFINITION_ID,
            actorDefinitionId,
          ).set(
            Tables.CONNECTOR_BUILDER_PROJECT.UPDATED_AT,
            OffsetDateTime.now(),
          ).where(Tables.CONNECTOR_BUILDER_PROJECT.ID.eq(builderProjectId))
          .execute()
        null
      }
    }

    /**
     * Deletes all config injections associated with a given actor definition ID.
     *
     * @param actorDefinitionId The ID of the actor definition whose config injections should be deleted
     * @param ctx The JOOQ DSL context to use for the deletion
     */
    private fun deleteActorDefinitionConfigInjections(
      actorDefinitionId: UUID,
      ctx: DSLContext,
    ) {
      ctx
        .deleteFrom(Tables.ACTOR_DEFINITION_CONFIG_INJECTION)
        .where(Tables.ACTOR_DEFINITION_CONFIG_INJECTION.ACTOR_DEFINITION_ID.eq(actorDefinitionId))
        .execute()
    }

    /**
     * Update an actor_definition, active_declarative_manifest and create declarative_manifest.
     *
     *
     * Note that based on this signature, two problems might occur if the user of this method is not
     * diligent. This was done because we value more separation of concerns than consistency of the API
     * of this method. The problems are:
     *
     * <pre>
     *
     *  * DeclarativeManifest.manifest could be different from the one injected ActorDefinitionConfigInjection.
     *  * DeclarativeManifest.spec could be different from ConnectorSpecification.connectionSpecification
     *
     </pre> *
     *
     *
     * Since we decided not to validate this using the signature of the method, we will validate that
     * runtime and IllegalArgumentException if there is a mismatch.
     *
     *
     * The reasoning behind this reasoning is the following: Benefits: Alignment with platform's
     * definition of the repository. Drawbacks: We will need a method
     * configRepository.setDeclarativeSourceActiveVersion(sourceDefinitionId, version, manifest, spec);
     * where version and (manifest, spec) might not be consistent i.e. that a user of this method could
     * call it with configRepository.setDeclarativeSourceActiveVersion(sourceDefinitionId, version_10,
     * manifest_of_version_7, spec_of_version_12); However, we agreed that this was very unlikely.
     *
     *
     * Note that this is all in the context of data consistency i.e. that we want to do this in one
     * transaction. When we split this in many services, we will need to rethink data consistency.
     *
     * @param declarativeManifest declarative manifest version to create and make active
     * @param configInjections configInjection for the manifest
     * @param connectorSpecification connectorSpecification associated with the declarativeManifest
     * being created
     * @throws IOException exception while interacting with db
     * @throws IllegalArgumentException if there is a mismatch between the different arguments
     */
    @Throws(IOException::class)
    override fun createDeclarativeManifestAsActiveVersion(
      declarativeManifest: DeclarativeManifest,
      configInjections: List<ActorDefinitionConfigInjection>,
      connectorSpecification: ConnectorSpecification,
      cdkVersion: String,
    ) {
      // find one manifest config injection

      val manifestConfigInjection =
        configInjections
          .stream()
          .filter { injection: ActorDefinitionConfigInjection -> isManifestInjection(injection) }
          .findFirst()

      require(!manifestConfigInjection.isEmpty) { "No manifest config injection found" }

      require(declarativeManifest.actorDefinitionId == manifestConfigInjection.get().actorDefinitionId) {
        "DeclarativeManifest.actorDefinitionId must match ActorDefinitionConfigInjection.actorDefinitionId"
      }
      require(
        declarativeManifest.manifest == manifestConfigInjection.get().jsonToInject,
      ) { "The DeclarativeManifest does not match the config injection" }
      require(declarativeManifest.spec["connectionSpecification"] == connectorSpecification.connectionSpecification) {
        "DeclarativeManifest.spec must match ConnectorSpecification.connectionSpecification"
      }

      database.transaction<Any?> { ctx: DSLContext ->
        // Set new version of the actor definition
        updateDeclarativeActorDefinitionVersion(manifestConfigInjection.get().actorDefinitionId, connectorSpecification, cdkVersion, ctx)

        // Replace all existing config injections
        upsertActorDefinitionConfigInjections(configInjections, ctx)

        // Insert the new declarative manifest
        insertActiveDeclarativeManifest(declarativeManifest, ctx)
        null
      }
    }

    /**
     * Update an actor_definition, active_declarative_manifest and create declarative_manifest.
     *
     *
     * Note that based on this signature, two problems might occur if the user of this method is not
     * diligent. This was done because we value more separation of concerns than consistency of the API
     * of this method. The problems are:
     *
     * <pre>
     *
     *  * DeclarativeManifest.manifest could be different from the one injected ActorDefinitionConfigInjection.
     *  * DeclarativeManifest.spec could be different from ConnectorSpecification.connectionSpecification
     *
     </pre> *
     *
     *
     * At that point, we can only hope the user won't cause data consistency issue using this method
     *
     *
     * The reasoning behind this reasoning is the following: Benefits: Alignment with platform's
     * definition of the repository. Drawbacks: We will need a method
     * configRepository.setDeclarativeSourceActiveVersion(sourceDefinitionId, version, manifest, spec);
     * where version and (manifest, spec) might not be consistent i.e. that a user of this method could
     * call it with configRepository.setDeclarativeSourceActiveVersion(sourceDefinitionId, version_10,
     * manifest_of_version_7, spec_of_version_12); However, we agreed that this was very unlikely.
     *
     *
     * Note that this is all in the context of data consistency i.e. that we want to do this in one
     * transaction. When we split this in many services, we will need to rethink data consistency.
     *
     * @param sourceDefinitionId actor definition to update
     * @param version the version of the manifest to make active. declarative_manifest.version must
     * already exist
     * @param configInjections configInjections for the manifest
     * @param connectorSpecification connectorSpecification associated with the declarativeManifest
     * being made active
     * @throws IOException exception while interacting with db
     */
    @Throws(IOException::class)
    override fun setDeclarativeSourceActiveVersion(
      sourceDefinitionId: UUID,
      version: Long,
      configInjections: List<ActorDefinitionConfigInjection>,
      connectorSpecification: ConnectorSpecification,
      cdkVersion: String,
    ) {
      database.transaction<Any?> { ctx: DSLContext ->
        updateDeclarativeActorDefinitionVersion(sourceDefinitionId, connectorSpecification, cdkVersion, ctx)
        upsertActorDefinitionConfigInjections(configInjections, ctx)
        upsertActiveDeclarativeManifest(
          ActiveDeclarativeManifest().withActorDefinitionId(sourceDefinitionId).withVersion(version),
          ctx,
        )
        null
      }
    }

    /**
     * Load all config injection for an actor definition.
     *
     * @param actorDefinitionId id of the actor definition to fetch
     * @return stream of config injection objects
     * @throws IOException exception while interacting with db
     */
    @Throws(IOException::class)
    override fun getActorDefinitionConfigInjections(actorDefinitionId: UUID): Stream<ActorDefinitionConfigInjection> =
      database
        .query { ctx: DSLContext ->
          ctx
            .select(Tables.ACTOR_DEFINITION_CONFIG_INJECTION.asterisk())
            .from(Tables.ACTOR_DEFINITION_CONFIG_INJECTION)
            .where(Tables.ACTOR_DEFINITION_CONFIG_INJECTION.ACTOR_DEFINITION_ID.eq(actorDefinitionId))
            .fetch()
        }.map { record: Record -> DbConverter.buildActorDefinitionConfigInjection(record) }
        .stream()

    /**
     * Update or create a config injection object. If there is an existing config injection for the
     * given actor definition and path, it is updated. If there isn't yet, a new config injection is
     * created.
     *
     * @param actorDefinitionConfigInjection the config injection object to write to the database
     * @throws IOException exception while interacting with db
     */
    @Throws(IOException::class)
    override fun writeActorDefinitionConfigInjectionForPath(actorDefinitionConfigInjection: ActorDefinitionConfigInjection) {
      database.transaction<Any?> { ctx: DSLContext ->
        writeActorDefinitionConfigInjectionForPath(actorDefinitionConfigInjection, ctx)
        null
      }
    }

    /**
     * Update or create one or more config injection object. If there is an existing config injection
     * for the given actor definition and path, it is updated. If there isn't yet, a new config
     * injection is created.
     *
     * @param actorDefinitionConfigInjections the config injections to write to the database
     * @throws IOException exception while interacting with db
     */
    @Throws(IOException::class)
    override fun writeActorDefinitionConfigInjectionsForPath(actorDefinitionConfigInjections: List<ActorDefinitionConfigInjection>) {
      database.transaction<Any?> { ctx: DSLContext ->
        writeActorDefinitionConfigInjectionsForPath(actorDefinitionConfigInjections, ctx)
        null
      }
    }

    /**
     * Write multiple config injections to the database using the provided DSL context. This method
     * iterates through the list of config injections and writes each one individually.
     *
     * @param actorDefinitionConfigInjections List of config injections to write
     * @param ctx The JOOQ DSL context to use for database operations
     */
    private fun writeActorDefinitionConfigInjectionsForPath(
      actorDefinitionConfigInjections: List<ActorDefinitionConfigInjection>,
      ctx: DSLContext,
    ) {
      for (configInjection in actorDefinitionConfigInjections) {
        writeActorDefinitionConfigInjectionForPath(configInjection, ctx)
      }
    }

    /**
     * Update or insert a list of config injections for an actor definition. This method: 1. Validates
     * that a manifest config injection exists in the list 2. Ensures all config injections are for the
     * same actor definition 3. Deletes any existing config injections for the actor definition 4.
     * Writes the new config injections
     *
     * @param configInjections List of config injections to upsert
     * @param ctx The JOOQ DSL context to use for database operations
     * @throws IllegalArgumentException if no manifest injection is found or if config injections have
     * different actor definition IDs
     */
    private fun upsertActorDefinitionConfigInjections(
      configInjections: List<ActorDefinitionConfigInjection>,
      ctx: DSLContext,
    ) {
      // find one manifest config injection

      val manifestConfigInjection =
        configInjections
          .stream()
          .filter { injection: ActorDefinitionConfigInjection -> isManifestInjection(injection) }
          .findFirst()

      // Ensure all config injections have the same actor definition id
      val uniqueActorDefinitionIds =
        configInjections
          .stream()
          .map { obj: ActorDefinitionConfigInjection -> obj.actorDefinitionId }
          .distinct()
          .count()
      val hasMoreThanOneActorDefinitionId = uniqueActorDefinitionIds > 1
      require(!hasMoreThanOneActorDefinitionId) { "All config injections must have the same actor definition id" }

      require(!manifestConfigInjection.isEmpty) { "No manifest config injection found" }

      // Delete all existing config injections for the actor definition
      deleteActorDefinitionConfigInjections(manifestConfigInjection.get().actorDefinitionId, ctx)

      // Write all new config injections
      writeActorDefinitionConfigInjectionsForPath(configInjections, ctx)
    }

    /**
     * Insert a declarative manifest. If DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID and
     * DECLARATIVE_MANIFEST.VERSION is already in the DB, an exception will be thrown
     *
     * @param declarativeManifest declarative manifest to insert
     * @throws IOException exception while interacting with db
     */
    @Throws(IOException::class)
    override fun insertDeclarativeManifest(declarativeManifest: DeclarativeManifest) {
      database.transaction<Any?> { ctx: DSLContext ->
        insertDeclarativeManifest(declarativeManifest, ctx)
        null
      }
    }

    /**
     * Insert a declarative manifest and its associated active declarative manifest. If
     * DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID and DECLARATIVE_MANIFEST.VERSION is already in the DB,
     * an exception will be thrown
     *
     * @param declarativeManifest declarative manifest to insert
     * @throws IOException exception while interacting with db
     */
    @Throws(IOException::class)
    override fun insertActiveDeclarativeManifest(declarativeManifest: DeclarativeManifest) {
      database.transaction<Any?> { ctx: DSLContext ->
        insertDeclarativeManifest(declarativeManifest, ctx)
        upsertActiveDeclarativeManifest(
          ActiveDeclarativeManifest()
            .withActorDefinitionId(declarativeManifest.actorDefinitionId)
            .withVersion(declarativeManifest.version),
          ctx,
        )
        null
      }
    }

    /**
     * Read all declarative manifests by actor definition id without the manifest column.
     *
     * @param actorDefinitionId actor definition id
     * @throws IOException exception while interacting with db
     */
    @Throws(IOException::class)
    override fun getDeclarativeManifestsByActorDefinitionId(actorDefinitionId: UUID): Stream<DeclarativeManifest> =
      database
        .query { ctx: DSLContext ->
          ctx
            .select(
              Tables.DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID,
              Tables.DECLARATIVE_MANIFEST.DESCRIPTION,
              Tables.DECLARATIVE_MANIFEST.SPEC,
              Tables.DECLARATIVE_MANIFEST.VERSION,
              Tables.DECLARATIVE_MANIFEST.COMPONENTS_FILE_CONTENT,
            ).from(Tables.DECLARATIVE_MANIFEST)
            .where(Tables.DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID.eq(actorDefinitionId))
            .fetch()
        }.map { record: Record5<UUID, String, JSONB, Long, String> ->
          DbConverter.buildDeclarativeManifestWithoutManifestAndSpec(record)
        }.stream()

    /**
     * Read declarative manifest by actor definition id and version with manifest column.
     *
     * @param actorDefinitionId actor definition id
     * @param version the version of the declarative manifest
     * @throws IOException exception while interacting with db
     * @throws ConfigNotFoundException exception if no match on DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID
     * and DECLARATIVE_MANIFEST.VERSION
     */
    @Throws(IOException::class, ConfigNotFoundException::class)
    override fun getDeclarativeManifestByActorDefinitionIdAndVersion(
      actorDefinitionId: UUID,
      version: Long,
    ): DeclarativeManifest {
      val declarativeManifest =
        database
          .query { ctx: DSLContext ->
            ctx
              .select(Tables.DECLARATIVE_MANIFEST.asterisk())
              .from(Tables.DECLARATIVE_MANIFEST)
              .where(
                Tables.DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID
                  .eq(actorDefinitionId)
                  .and(Tables.DECLARATIVE_MANIFEST.VERSION.eq(version)),
              ).fetch()
          }.map { record: Record -> DbConverter.buildDeclarativeManifest(record) }
          .stream()
          .findFirst()
      return declarativeManifest.orElseThrow {
        ConfigNotFoundException(
          ConfigNotFoundType.DECLARATIVE_MANIFEST,
          String.format("actorDefinitionId:%s,version:%s", actorDefinitionId, version),
        )
      }
    }

    /**
     * Read currently active declarative manifest by actor definition id by joining with
     * active_declarative_manifest for the same actor definition id with manifest.
     *
     * @param actorDefinitionId actor definition id
     * @throws IOException exception while interacting with db
     * @throws ConfigNotFoundException exception if no match on DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID
     * that matches the version of an active manifest
     */
    @Throws(IOException::class, ConfigNotFoundException::class)
    override fun getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(actorDefinitionId: UUID): DeclarativeManifest {
      val declarativeManifest =
        database
          .query { ctx: DSLContext ->
            ctx
              .select(Tables.DECLARATIVE_MANIFEST.asterisk())
              .from(Tables.DECLARATIVE_MANIFEST)
              .join(Tables.ACTIVE_DECLARATIVE_MANIFEST, JoinType.JOIN)
              .on(
                Tables.DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID.eq(Tables.ACTIVE_DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID),
                Tables.DECLARATIVE_MANIFEST.VERSION.eq(Tables.ACTIVE_DECLARATIVE_MANIFEST.VERSION),
              ).where(Tables.ACTIVE_DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID.eq(actorDefinitionId))
              .fetch()
          }.map { record: Record -> DbConverter.buildDeclarativeManifest(record) }
          .stream()
          .findFirst()
      return declarativeManifest.orElseThrow {
        ConfigNotFoundException(
          ConfigNotFoundType.DECLARATIVE_MANIFEST,
          String.format(
            "ACTIVE_DECLARATIVE_MANIFEST.actor_definition_id:%s and matching DECLARATIVE_MANIFEST.version",
            actorDefinitionId,
          ),
        )
      }
    }

    /**
     * Update the testing values of a connector builder project.
     *
     * @param projectId builder project to update
     * @param testingValues testing values to set on the project
     * @throws IOException exception while interacting with db
     */
    @Throws(IOException::class)
    override fun updateBuilderProjectTestingValues(
      projectId: UUID,
      testingValues: JsonNode,
    ) {
      val timestamp = OffsetDateTime.now()
      database.transaction { ctx: DSLContext ->
        ctx
          .update(Tables.CONNECTOR_BUILDER_PROJECT)
          .set(
            Tables.CONNECTOR_BUILDER_PROJECT.TESTING_VALUES,
            JSONB.valueOf(Jsons.serialize(testingValues)),
          ).set(Tables.CONNECTOR_BUILDER_PROJECT.UPDATED_AT, timestamp)
          .where(Tables.CONNECTOR_BUILDER_PROJECT.ID.eq(projectId))
          .execute()
      }
    }

    private fun writeBuilderProjectDraft(
      projectId: UUID,
      workspaceId: UUID,
      name: String,
      manifestDraft: JsonNode?,
      componentsFileContent: String?,
      baseActorDefinitionVersionId: UUID?,
      contributionPullRequestUrl: String?,
      contributionActorDefinitionId: UUID?,
      ctx: DSLContext,
    ) {
      val timestamp = OffsetDateTime.now()
      val matchId = Tables.CONNECTOR_BUILDER_PROJECT.ID.eq(projectId)
      val isExistingConfig =
        ctx.fetchExists(
          DSL
            .select()
            .from(Tables.CONNECTOR_BUILDER_PROJECT)
            .where(matchId),
        )

      if (isExistingConfig) {
        ctx
          .update(Tables.CONNECTOR_BUILDER_PROJECT)
          .set(Tables.CONNECTOR_BUILDER_PROJECT.ID, projectId)
          .set(Tables.CONNECTOR_BUILDER_PROJECT.WORKSPACE_ID, workspaceId)
          .set(Tables.CONNECTOR_BUILDER_PROJECT.NAME, name)
          .set(
            Tables.CONNECTOR_BUILDER_PROJECT.MANIFEST_DRAFT,
            if (manifestDraft != null) JSONB.valueOf(Jsons.serialize(manifestDraft)) else null,
          ).set(Tables.CONNECTOR_BUILDER_PROJECT.BASE_ACTOR_DEFINITION_VERSION_ID, baseActorDefinitionVersionId)
          .set(Tables.CONNECTOR_BUILDER_PROJECT.CONTRIBUTION_PULL_REQUEST_URL, contributionPullRequestUrl)
          .set(Tables.CONNECTOR_BUILDER_PROJECT.CONTRIBUTION_ACTOR_DEFINITION_ID, contributionActorDefinitionId)
          .set(Tables.CONNECTOR_BUILDER_PROJECT.COMPONENTS_FILE_CONTENT, componentsFileContent)
          .set(Tables.CONNECTOR_BUILDER_PROJECT.UPDATED_AT, timestamp)
          .where(matchId)
          .execute()
      } else {
        ctx
          .insertInto(Tables.CONNECTOR_BUILDER_PROJECT)
          .set(Tables.CONNECTOR_BUILDER_PROJECT.ID, projectId)
          .set(Tables.CONNECTOR_BUILDER_PROJECT.WORKSPACE_ID, workspaceId)
          .set(Tables.CONNECTOR_BUILDER_PROJECT.NAME, name)
          .set(
            Tables.CONNECTOR_BUILDER_PROJECT.MANIFEST_DRAFT,
            if (manifestDraft != null) JSONB.valueOf(Jsons.serialize(manifestDraft)) else null,
          ).set(Tables.CONNECTOR_BUILDER_PROJECT.BASE_ACTOR_DEFINITION_VERSION_ID, baseActorDefinitionVersionId)
          .set(Tables.CONNECTOR_BUILDER_PROJECT.CONTRIBUTION_PULL_REQUEST_URL, contributionPullRequestUrl)
          .set(Tables.CONNECTOR_BUILDER_PROJECT.CONTRIBUTION_ACTOR_DEFINITION_ID, contributionActorDefinitionId)
          .set(Tables.CONNECTOR_BUILDER_PROJECT.COMPONENTS_FILE_CONTENT, componentsFileContent)
          .set(Tables.CONNECTOR_BUILDER_PROJECT.CREATED_AT, timestamp)
          .set(Tables.CONNECTOR_BUILDER_PROJECT.UPDATED_AT, timestamp)
          .set(Tables.CONNECTOR_BUILDER_PROJECT.TOMBSTONE, false)
          .execute()
      }
    }

    private fun insertActiveDeclarativeManifest(
      declarativeManifest: DeclarativeManifest,
      ctx: DSLContext,
    ) {
      insertDeclarativeManifest(declarativeManifest, ctx)
      upsertActiveDeclarativeManifest(
        ActiveDeclarativeManifest()
          .withActorDefinitionId(declarativeManifest.actorDefinitionId)
          .withVersion(declarativeManifest.version),
        ctx,
      )
    }

    private fun updateDeclarativeActorDefinitionVersion(
      actorDefinitionId: UUID,
      spec: ConnectorSpecification,
      cdkVersion: String,
      ctx: DSLContext,
    ) {
      // We are updating the same version since connector builder projects have a different concept of
      // versioning.
      ctx
        .update(Tables.ACTOR_DEFINITION_VERSION)
        .set(Tables.ACTOR_DEFINITION_VERSION.UPDATED_AT, OffsetDateTime.now())
        .set(Tables.ACTOR_DEFINITION_VERSION.SPEC, JSONB.valueOf(Jsons.serialize(spec)))
        .set(Tables.ACTOR_DEFINITION_VERSION.DOCKER_IMAGE_TAG, cdkVersion)
        .where(Tables.ACTOR_DEFINITION_VERSION.ACTOR_DEFINITION_ID.eq(actorDefinitionId))
        .execute()
    }

    private fun upsertActiveDeclarativeManifest(
      activeDeclarativeManifest: ActiveDeclarativeManifest,
      ctx: DSLContext,
    ) {
      val timestamp = OffsetDateTime.now()
      val matchId = Tables.ACTIVE_DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID.eq(activeDeclarativeManifest.actorDefinitionId)
      val isExistingConfig =
        ctx.fetchExists(
          DSL
            .select()
            .from(Tables.ACTIVE_DECLARATIVE_MANIFEST)
            .where(matchId),
        )

      if (isExistingConfig) {
        ctx
          .update(Tables.ACTIVE_DECLARATIVE_MANIFEST)
          .set(Tables.ACTIVE_DECLARATIVE_MANIFEST.VERSION, activeDeclarativeManifest.version)
          .set(Tables.ACTIVE_DECLARATIVE_MANIFEST.UPDATED_AT, timestamp)
          .where(matchId)
          .execute()
      } else {
        ctx
          .insertInto(Tables.ACTIVE_DECLARATIVE_MANIFEST)
          .set(Tables.ACTIVE_DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID, activeDeclarativeManifest.actorDefinitionId)
          .set(Tables.ACTIVE_DECLARATIVE_MANIFEST.VERSION, activeDeclarativeManifest.version)
          .set(Tables.ACTIVE_DECLARATIVE_MANIFEST.CREATED_AT, timestamp)
          .set(Tables.ACTIVE_DECLARATIVE_MANIFEST.UPDATED_AT, timestamp)
          .execute()
      }
    }

    private fun writeActorDefinitionConfigInjectionForPath(
      actorDefinitionConfigInjection: ActorDefinitionConfigInjection,
      ctx: DSLContext,
    ) {
      val timestamp = OffsetDateTime.now()
      val matchActorDefinitionIdAndInjectionPath =
        Tables.ACTOR_DEFINITION_CONFIG_INJECTION.ACTOR_DEFINITION_ID
          .eq(actorDefinitionConfigInjection.actorDefinitionId)
          .and(Tables.ACTOR_DEFINITION_CONFIG_INJECTION.INJECTION_PATH.eq(actorDefinitionConfigInjection.injectionPath))
      val isExistingConfig =
        ctx.fetchExists(
          DSL
            .select()
            .from(Tables.ACTOR_DEFINITION_CONFIG_INJECTION)
            .where(matchActorDefinitionIdAndInjectionPath),
        )

      if (isExistingConfig) {
        ctx
          .update(Tables.ACTOR_DEFINITION_CONFIG_INJECTION)
          .set(
            Tables.ACTOR_DEFINITION_CONFIG_INJECTION.JSON_TO_INJECT,
            JSONB.valueOf(Jsons.serialize(actorDefinitionConfigInjection.jsonToInject)),
          ).set(Tables.ACTOR_DEFINITION_CONFIG_INJECTION.UPDATED_AT, timestamp)
          .where(matchActorDefinitionIdAndInjectionPath)
          .execute()
      } else {
        ctx
          .insertInto(Tables.ACTOR_DEFINITION_CONFIG_INJECTION)
          .set(Tables.ACTOR_DEFINITION_CONFIG_INJECTION.INJECTION_PATH, actorDefinitionConfigInjection.injectionPath)
          .set(Tables.ACTOR_DEFINITION_CONFIG_INJECTION.ACTOR_DEFINITION_ID, actorDefinitionConfigInjection.actorDefinitionId)
          .set(
            Tables.ACTOR_DEFINITION_CONFIG_INJECTION.JSON_TO_INJECT,
            JSONB.valueOf(Jsons.serialize(actorDefinitionConfigInjection.jsonToInject)),
          ).set(Tables.ACTOR_DEFINITION_CONFIG_INJECTION.CREATED_AT, timestamp)
          .set(Tables.ACTOR_DEFINITION_CONFIG_INJECTION.UPDATED_AT, timestamp)
          .execute()
      }
    }

    companion object {
      private const val INJECTED_DECLARATIVE_MANIFEST_KEY = "__injected_declarative_manifest"

      private val BASE_CONNECTOR_BUILDER_PROJECT_COLUMNS: List<Field<*>> =
        Arrays.asList<Field<*>>(
          Tables.CONNECTOR_BUILDER_PROJECT.ID,
          Tables.CONNECTOR_BUILDER_PROJECT.WORKSPACE_ID,
          Tables.CONNECTOR_BUILDER_PROJECT.NAME,
          Tables.CONNECTOR_BUILDER_PROJECT.ACTOR_DEFINITION_ID,
          Tables.CONNECTOR_BUILDER_PROJECT.TOMBSTONE,
          Tables.CONNECTOR_BUILDER_PROJECT.TESTING_VALUES,
          DSL.field(Tables.CONNECTOR_BUILDER_PROJECT.MANIFEST_DRAFT.isNotNull()).`as`("hasDraft"),
          Tables.CONNECTOR_BUILDER_PROJECT.UPDATED_AT,
          Tables.CONNECTOR_BUILDER_PROJECT.BASE_ACTOR_DEFINITION_VERSION_ID,
          Tables.CONNECTOR_BUILDER_PROJECT.CONTRIBUTION_PULL_REQUEST_URL,
          Tables.CONNECTOR_BUILDER_PROJECT.CONTRIBUTION_ACTOR_DEFINITION_ID,
          Tables.CONNECTOR_BUILDER_PROJECT.COMPONENTS_FILE_CONTENT,
        )

      /**
       * Checks if a given config injection is a manifest injection by comparing its injection path.
       *
       * @param injection The config injection to check
       * @return true if the injection is a manifest injection, false otherwise
       */
      private fun isManifestInjection(injection: ActorDefinitionConfigInjection): Boolean =
        INJECTED_DECLARATIVE_MANIFEST_KEY == injection.injectionPath

      private fun insertDeclarativeManifest(
        declarativeManifest: DeclarativeManifest,
        ctx: DSLContext,
      ) {
        // Since "null" is a valid JSON object, `JSONB.valueOf(Jsons.serialize(null))` returns a valid JSON
        // object that is not null. Therefore, we will validate null values for JSON fields here
        if (declarativeManifest.manifest == null) {
          throw DataAccessException("null value in column \"manifest\" of relation \"declarative_manifest\" violates not-null constraint")
        }
        if (declarativeManifest.spec == null) {
          throw DataAccessException("null value in column \"spec\" of relation \"declarative_manifest\" violates not-null constraint")
        }

        val timestamp = OffsetDateTime.now()
        ctx
          .insertInto(Tables.DECLARATIVE_MANIFEST)
          .set(Tables.DECLARATIVE_MANIFEST.ACTOR_DEFINITION_ID, declarativeManifest.actorDefinitionId)
          .set(Tables.DECLARATIVE_MANIFEST.DESCRIPTION, declarativeManifest.description)
          .set(Tables.DECLARATIVE_MANIFEST.MANIFEST, JSONB.valueOf(Jsons.serialize(declarativeManifest.manifest)))
          .set(Tables.DECLARATIVE_MANIFEST.SPEC, JSONB.valueOf(Jsons.serialize(declarativeManifest.spec)))
          .set(Tables.DECLARATIVE_MANIFEST.VERSION, declarativeManifest.version)
          .set(Tables.DECLARATIVE_MANIFEST.CREATED_AT, timestamp)
          .set(Tables.DECLARATIVE_MANIFEST.COMPONENTS_FILE_CONTENT, declarativeManifest.componentsFileContent)
          .execute()
      }

      private fun buildConnectorBuilderProjectVersionedManifest(record: Record): ConnectorBuilderProjectVersionedManifest =
        ConnectorBuilderProjectVersionedManifest()
          .withName(record.get(Tables.CONNECTOR_BUILDER_PROJECT.NAME))
          .withBuilderProjectId(record.get(Tables.CONNECTOR_BUILDER_PROJECT.ID))
          .withHasDraft(record["hasDraft"] as Boolean?)
          .withSourceDefinitionId(record.get(Tables.CONNECTOR_BUILDER_PROJECT.ACTOR_DEFINITION_ID))
          .withActiveDeclarativeManifestVersion(record.get(Tables.ACTIVE_DECLARATIVE_MANIFEST.VERSION))
          .withManifest(Jsons.deserialize(record.get(Tables.DECLARATIVE_MANIFEST.MANIFEST).data()))
          .withManifestVersion(record.get(Tables.DECLARATIVE_MANIFEST.VERSION))
          .withManifestDescription(record.get(Tables.DECLARATIVE_MANIFEST.DESCRIPTION))
          .withComponentsFileContent(record.get(Tables.DECLARATIVE_MANIFEST.COMPONENTS_FILE_CONTENT))
    }
  }
