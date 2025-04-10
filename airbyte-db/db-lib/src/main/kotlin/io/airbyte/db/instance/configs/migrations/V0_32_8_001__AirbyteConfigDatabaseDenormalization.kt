/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

@file:Suppress("ktlint:standard:filename")
/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.enums.Enums
import io.airbyte.commons.json.Jsons
import io.airbyte.config.AirbyteConfig
import io.airbyte.config.ConfigSchema
import io.airbyte.config.ConfigWithMetadata
import io.airbyte.config.DestinationConnection
import io.airbyte.config.DestinationOAuthParameter
import io.airbyte.config.SourceConnection
import io.airbyte.config.SourceOAuthParameter
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardSyncOperation
import io.airbyte.config.StandardSyncState
import io.airbyte.config.StandardWorkspace
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.Catalog
import org.jooq.DSLContext
import org.jooq.EnumType
import org.jooq.JSONB
import org.jooq.Record
import org.jooq.Schema
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.jooq.impl.SchemaImpl
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID
import java.util.stream.Collectors

private val log = KotlinLogging.logger {}

/**
 * Migrate from config table to well-typed tables migration.
 */
@Suppress("ktlint:standard:class-naming")
class V0_32_8_001__AirbyteConfigDatabaseDenormalization : BaseJavaMigration() {
  override fun migrate(context: Context) {
    val ctx = DSL.using(context.connection)
    migrate(ctx)
  }

  @Suppress("ktlint:standard:enum-entry-name-case")
  enum class ActorType(
    private val literal: String,
  ) : EnumType {
    source("source"),
    destination("destination"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

    override fun getName(): String = "actor_type"

    override fun getLiteral(): String = literal
  }

  @Suppress("ktlint:standard:enum-entry-name-case")
  enum class SourceType(
    private val literal: String,
  ) : EnumType {
    api("api"),
    file("file"),
    database("database"),
    custom("custom"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

    override fun getName(): String = "source_type"

    override fun getLiteral(): String = literal
  }

  @Suppress("ktlint:standard:enum-entry-name-case")
  enum class NamespaceDefinitionType(
    private val literal: String,
  ) : EnumType {
    source("source"),
    destination("destination"),
    customformat("customformat"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

    override fun getName(): String = "namespace_definition_type"

    override fun getLiteral(): String = literal
  }

  @Suppress("ktlint:standard:enum-entry-name-case")
  enum class StatusType(
    private val literal: String,
  ) : EnumType {
    active("active"),
    inactive("inactive"),
    deprecated("deprecated"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

    override fun getName(): String = "status_type"

    override fun getLiteral(): String = literal
  }

  @Suppress("ktlint:standard:enum-entry-name-case")
  enum class OperatorType(
    private val literal: String,
  ) : EnumType {
    normalization("normalization"),
    dbt("dbt"),
    ;

    override fun getCatalog(): Catalog? = schema.catalog

    override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

    override fun getName(): String = "operator_type"

    override fun getLiteral(): String = literal
  }

  @JvmRecord
  data class StandardSourceDefinition(
    @JvmField val sourceDefinitionId: UUID,
    val name: String,
    val dockerRepository: String,
    val dockerImageTag: String,
    val documentationUrl: String,
    val icon: String,
    val sourceType: SourceType,
    val spec: ConnectorSpecification?,
  )

  @JvmRecord
  data class StandardDestinationDefinition(
    @JvmField val destinationDefinitionId: UUID,
    val name: String,
    val dockerRepository: String,
    val dockerImageTag: String,
    val documentationUrl: String,
    val icon: String,
    val spec: ConnectorSpecification?,
  )

  companion object {
    @JvmStatic
    @VisibleForTesting
    fun migrate(ctx: DSLContext) {
      createEnums(ctx)
      createAndPopulateWorkspace(ctx)
      createAndPopulateActorDefinition(ctx)
      createAndPopulateActor(ctx)
      crateAndPopulateActorOauthParameter(ctx)
      createAndPopulateOperation(ctx)
      createAndPopulateConnection(ctx)
      createAndPopulateState(ctx)
    }

    private fun createEnums(ctx: DSLContext) {
      ctx.createType("source_type").asEnum("api", "file", "database", "custom").execute()
      log.info { "source_type enum created" }
      ctx.createType("actor_type").asEnum("source", "destination").execute()
      log.info { "actor_type enum created" }
      ctx.createType("operator_type").asEnum("normalization", "dbt").execute()
      log.info { "operator_type enum created" }
      ctx.createType("namespace_definition_type").asEnum("source", "destination", "customformat").execute()
      log.info { "namespace_definition_type enum created" }
      ctx.createType("status_type").asEnum("active", "inactive", "deprecated").execute()
      log.info { "status_type enum created" }
    }

    private fun createAndPopulateWorkspace(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val name = DSL.field("name", SQLDataType.VARCHAR(256).nullable(false))
      val slug = DSL.field("slug", SQLDataType.VARCHAR(256).nullable(false))
      val initialSetupComplete = DSL.field("initial_setup_complete", SQLDataType.BOOLEAN.nullable(false))
      val customerId = DSL.field("customer_id", SQLDataType.UUID.nullable(true))
      val email = DSL.field("email", SQLDataType.VARCHAR(256).nullable(true))
      val anonymousDataCollection = DSL.field("anonymous_data_collection", SQLDataType.BOOLEAN.nullable(true))
      val sendNewsletter = DSL.field("send_newsletter", SQLDataType.BOOLEAN.nullable(true))
      val sendSecurityUpdates = DSL.field("send_security_updates", SQLDataType.BOOLEAN.nullable(true))
      val displaySetupWizard = DSL.field("display_setup_wizard", SQLDataType.BOOLEAN.nullable(true))
      val tombstone = DSL.field("tombstone", SQLDataType.BOOLEAN.nullable(false).defaultValue(false))
      val notifications = DSL.field("notifications", SQLDataType.JSONB.nullable(true))
      val firstSyncComplete = DSL.field("first_sync_complete", SQLDataType.BOOLEAN.nullable(true))
      val feedbackComplete = DSL.field("feedback_complete", SQLDataType.BOOLEAN.nullable(true))
      val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

      ctx
        .createTableIfNotExists("workspace")
        .columns(
          id,
          customerId,
          name,
          slug,
          email,
          initialSetupComplete,
          anonymousDataCollection,
          sendNewsletter,
          sendSecurityUpdates,
          displaySetupWizard,
          tombstone,
          notifications,
          firstSyncComplete,
          feedbackComplete,
          createdAt,
          updatedAt,
        ).constraints(DSL.primaryKey(id))
        .execute()
      log.info { "workspace table created" }
      val configsWithMetadata =
        listConfigsWithMetadata(
          ConfigSchema.STANDARD_WORKSPACE,
          StandardWorkspace::class.java,
          ctx,
        )

      for (configWithMetadata in configsWithMetadata) {
        val standardWorkspace = configWithMetadata.config
        ctx
          .insertInto(DSL.table("workspace"))
          .set(id, standardWorkspace.workspaceId)
          .set(customerId, standardWorkspace.customerId)
          .set(name, standardWorkspace.name)
          .set(slug, standardWorkspace.slug)
          .set(email, standardWorkspace.email)
          .set(initialSetupComplete, standardWorkspace.initialSetupComplete)
          .set(anonymousDataCollection, standardWorkspace.anonymousDataCollection)
          .set(sendNewsletter, standardWorkspace.news)
          .set(sendSecurityUpdates, standardWorkspace.securityUpdates)
          .set(displaySetupWizard, standardWorkspace.displaySetupWizard)
          .set(tombstone, standardWorkspace.tombstone != null && standardWorkspace.tombstone)
          .set(notifications, JSONB.valueOf(Jsons.serialize(standardWorkspace.notifications)))
          .set(firstSyncComplete, standardWorkspace.firstCompletedSync)
          .set(feedbackComplete, standardWorkspace.feedbackDone)
          .set(createdAt, OffsetDateTime.ofInstant(configWithMetadata.createdAt, ZoneOffset.UTC))
          .set(updatedAt, OffsetDateTime.ofInstant(configWithMetadata.updatedAt, ZoneOffset.UTC))
          .execute()
      }
      log.info { "workspace table populated with ${configsWithMetadata.size} records" }
    }

    private fun createAndPopulateActorDefinition(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val name = DSL.field("name", SQLDataType.VARCHAR(256).nullable(false))
      val dockerRepository = DSL.field("docker_repository", SQLDataType.VARCHAR(256).nullable(false))
      val dockerImageTag = DSL.field("docker_image_tag", SQLDataType.VARCHAR(256).nullable(false))
      val documentationUrl = DSL.field("documentation_url", SQLDataType.VARCHAR(256).nullable(true))
      val spec = DSL.field("spec", SQLDataType.JSONB.nullable(false))
      val icon = DSL.field("icon", SQLDataType.VARCHAR(256).nullable(true))
      val actorType = DSL.field("actor_type", SQLDataType.VARCHAR.asEnumDataType(ActorType::class.java).nullable(false))
      val sourceType = DSL.field("source_type", SQLDataType.VARCHAR.asEnumDataType(SourceType::class.java).nullable(true))
      val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

      ctx
        .createTableIfNotExists("actor_definition")
        .columns(
          id,
          name,
          dockerRepository,
          dockerImageTag,
          documentationUrl,
          icon,
          actorType,
          sourceType,
          spec,
          createdAt,
          updatedAt,
        ).constraints(DSL.primaryKey(id))
        .execute()

      log.info { "actor_definition table created" }

      val sourceDefinitionsWithMetadata =
        listConfigsWithMetadata(
          ConfigSchema.STANDARD_SOURCE_DEFINITION,
          StandardSourceDefinition::class.java,
          ctx,
        )

      for (configWithMetadata in sourceDefinitionsWithMetadata) {
        val standardSourceDefinition = configWithMetadata.config
        ctx
          .insertInto(DSL.table("actor_definition"))
          .set(id, standardSourceDefinition.sourceDefinitionId)
          .set(name, standardSourceDefinition.name)
          .set(dockerRepository, standardSourceDefinition.dockerRepository)
          .set(dockerImageTag, standardSourceDefinition.dockerImageTag)
          .set(documentationUrl, standardSourceDefinition.documentationUrl)
          .set(icon, standardSourceDefinition.icon)
          .set(actorType, ActorType.source)
          .set(sourceType, standardSourceDefinition.sourceType)
          .set(spec, JSONB.valueOf(Jsons.serialize(standardSourceDefinition.spec)))
          .set(createdAt, OffsetDateTime.ofInstant(configWithMetadata.createdAt, ZoneOffset.UTC))
          .set(updatedAt, OffsetDateTime.ofInstant(configWithMetadata.updatedAt, ZoneOffset.UTC))
          .execute()
      }
      log.info { "actor_definition table populated with ${sourceDefinitionsWithMetadata.size} source definition records" }

      val destinationDefinitionsWithMetadata =
        listConfigsWithMetadata(
          ConfigSchema.STANDARD_DESTINATION_DEFINITION,
          StandardDestinationDefinition::class.java,
          ctx,
        )

      for (configWithMetadata in destinationDefinitionsWithMetadata) {
        val standardDestinationDefinition = configWithMetadata.config
        ctx
          .insertInto(DSL.table("actor_definition"))
          .set(id, standardDestinationDefinition.destinationDefinitionId)
          .set(name, standardDestinationDefinition.name)
          .set(dockerRepository, standardDestinationDefinition.dockerRepository)
          .set(dockerImageTag, standardDestinationDefinition.dockerImageTag)
          .set(documentationUrl, standardDestinationDefinition.documentationUrl)
          .set(icon, standardDestinationDefinition.icon)
          .set(actorType, ActorType.destination)
          .set(spec, JSONB.valueOf(Jsons.serialize(standardDestinationDefinition.spec)))
          .set(createdAt, OffsetDateTime.ofInstant(configWithMetadata.createdAt, ZoneOffset.UTC))
          .set(updatedAt, OffsetDateTime.ofInstant(configWithMetadata.updatedAt, ZoneOffset.UTC))
          .execute()
      }
      log.info { "actor_definition table populated with ${destinationDefinitionsWithMetadata.size} destination definition records" }
    }

    private fun createAndPopulateActor(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val name = DSL.field("name", SQLDataType.VARCHAR(256).nullable(false))
      val actorDefinitionId = DSL.field("actor_definition_id", SQLDataType.UUID.nullable(false))
      val workspaceId = DSL.field("workspace_id", SQLDataType.UUID.nullable(false))
      val configuration = DSL.field("configuration", SQLDataType.JSONB.nullable(false))
      val actorType = DSL.field("actor_type", SQLDataType.VARCHAR.asEnumDataType(ActorType::class.java).nullable(false))
      val tombstone = DSL.field("tombstone", SQLDataType.BOOLEAN.nullable(false).defaultValue(false))
      val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

      ctx
        .createTableIfNotExists("actor")
        .columns(
          id,
          workspaceId,
          actorDefinitionId,
          name,
          configuration,
          actorType,
          tombstone,
          createdAt,
          updatedAt,
        ).constraints(
          DSL.primaryKey(id),
          DSL.foreignKey(workspaceId).references("workspace", "id").onDeleteCascade(),
          DSL.foreignKey(actorDefinitionId).references("actor_definition", "id").onDeleteCascade(),
        ).execute()
      ctx.createIndex("actor_actor_definition_id_idx").on("actor", "actor_definition_id").execute()

      log.info { "actor table created" }

      val sourcesWithMetadata =
        listConfigsWithMetadata(
          ConfigSchema.SOURCE_CONNECTION,
          SourceConnection::class.java,
          ctx,
        )
      var sourceRecords = 0L
      for (configWithMetadata in sourcesWithMetadata) {
        val sourceConnection = configWithMetadata.config
        if (workspaceDoesNotExist(sourceConnection.workspaceId, ctx)) {
          log.warn {
            "Skipping source connection ${sourceConnection.sourceId} because the specified workspace " +
              "${sourceConnection.workspaceId} doesn't exist and violates foreign key constraint."
          }
          continue
        } else if (actorDefinitionDoesNotExist(sourceConnection.sourceDefinitionId, ctx)) {
          log.warn {
            "Skipping source connection ${sourceConnection.sourceId} because the specified source definition " +
              "${sourceConnection.sourceDefinitionId} doesn't exist and violates foreign key constraint."
          }
          continue
        }

        ctx
          .insertInto(DSL.table("actor"))
          .set(id, sourceConnection.sourceId)
          .set(workspaceId, sourceConnection.workspaceId)
          .set(actorDefinitionId, sourceConnection.sourceDefinitionId)
          .set(name, sourceConnection.name)
          .set(configuration, JSONB.valueOf(Jsons.serialize(sourceConnection.configuration)))
          .set(actorType, ActorType.source)
          .set(tombstone, sourceConnection.tombstone != null && sourceConnection.tombstone)
          .set(createdAt, OffsetDateTime.ofInstant(configWithMetadata.createdAt, ZoneOffset.UTC))
          .set(updatedAt, OffsetDateTime.ofInstant(configWithMetadata.updatedAt, ZoneOffset.UTC))
          .execute()
        sourceRecords++
      }
      log.info { "actor table populated with $sourceRecords source records" }

      val destinationsWithMetadata =
        listConfigsWithMetadata(
          ConfigSchema.DESTINATION_CONNECTION,
          DestinationConnection::class.java,
          ctx,
        )
      var destinationRecords = 0L
      for (configWithMetadata in destinationsWithMetadata) {
        val destinationConnection = configWithMetadata.config
        if (workspaceDoesNotExist(destinationConnection.workspaceId, ctx)) {
          log.warn {
            "Skipping destination connection " + destinationConnection.destinationId + " because the specified workspace " +
              destinationConnection.workspaceId +
              " doesn't exist and violates foreign key constraint."
          }
          continue
        } else if (actorDefinitionDoesNotExist(destinationConnection.destinationDefinitionId, ctx)) {
          log.warn {
            "Skipping destination connection " + destinationConnection.destinationId + " because the specified source definition " +
              destinationConnection.destinationDefinitionId +
              " doesn't exist and violates foreign key constraint."
          }
          continue
        }

        ctx
          .insertInto(DSL.table("actor"))
          .set(id, destinationConnection.destinationId)
          .set(workspaceId, destinationConnection.workspaceId)
          .set(actorDefinitionId, destinationConnection.destinationDefinitionId)
          .set(name, destinationConnection.name)
          .set(configuration, JSONB.valueOf(Jsons.serialize(destinationConnection.configuration)))
          .set(actorType, ActorType.destination)
          .set(tombstone, destinationConnection.tombstone != null && destinationConnection.tombstone)
          .set(createdAt, OffsetDateTime.ofInstant(configWithMetadata.createdAt, ZoneOffset.UTC))
          .set(updatedAt, OffsetDateTime.ofInstant(configWithMetadata.updatedAt, ZoneOffset.UTC))
          .execute()
        destinationRecords++
      }
      log.info { "actor table populated with $destinationRecords destination records" }
    }

    @JvmStatic
    @VisibleForTesting
    fun workspaceDoesNotExist(
      workspaceId: UUID?,
      ctx: DSLContext,
    ): Boolean {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      return !ctx.fetchExists(
        DSL
          .select()
          .from(DSL.table("workspace"))
          .where(id.eq(workspaceId)),
      )
    }

    @JvmStatic
    @VisibleForTesting
    fun actorDefinitionDoesNotExist(
      definitionId: UUID,
      ctx: DSLContext,
    ): Boolean {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      return !ctx.fetchExists(
        DSL
          .select()
          .from(DSL.table("actor_definition"))
          .where(id.eq(definitionId)),
      )
    }

    @JvmStatic
    @VisibleForTesting
    fun actorDoesNotExist(
      actorId: UUID,
      ctx: DSLContext,
    ): Boolean {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      return !ctx.fetchExists(
        DSL
          .select()
          .from(DSL.table("actor"))
          .where(id.eq(actorId)),
      )
    }

    @JvmStatic
    @VisibleForTesting
    fun connectionDoesNotExist(
      connectionId: UUID,
      ctx: DSLContext,
    ): Boolean {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      return !ctx.fetchExists(
        DSL
          .select()
          .from(DSL.table("connection"))
          .where(id.eq(connectionId)),
      )
    }

    @JvmStatic
    @VisibleForTesting
    fun operationDoesNotExist(
      operationId: UUID,
      ctx: DSLContext,
    ): Boolean {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      return !ctx.fetchExists(
        DSL
          .select()
          .from(DSL.table("operation"))
          .where(id.eq(operationId)),
      )
    }

    private fun crateAndPopulateActorOauthParameter(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val actorDefinitionId = DSL.field("actor_definition_id", SQLDataType.UUID.nullable(false))
      val configuration = DSL.field("configuration", SQLDataType.JSONB.nullable(false))
      val workspaceId = DSL.field("workspace_id", SQLDataType.UUID.nullable(true))
      val actorType = DSL.field("actor_type", SQLDataType.VARCHAR.asEnumDataType(ActorType::class.java).nullable(false))
      val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

      ctx
        .createTableIfNotExists("actor_oauth_parameter")
        .columns(
          id,
          workspaceId,
          actorDefinitionId,
          configuration,
          actorType,
          createdAt,
          updatedAt,
        ).constraints(
          DSL.primaryKey(id),
          DSL.foreignKey(workspaceId).references("workspace", "id").onDeleteCascade(),
          DSL.foreignKey(actorDefinitionId).references("actor_definition", "id").onDeleteCascade(),
        ).execute()

      log.info { "actor_oauth_parameter table created" }

      val sourceOauthParamsWithMetadata =
        listConfigsWithMetadata(
          ConfigSchema.SOURCE_OAUTH_PARAM,
          SourceOAuthParameter::class.java,
          ctx,
        )
      var sourceOauthParamRecords = 0L
      for (configWithMetadata in sourceOauthParamsWithMetadata) {
        val sourceOAuthParameter = configWithMetadata.config
        if (workspaceDoesNotExist(sourceOAuthParameter.workspaceId, ctx)) {
          log.warn {
            (
              "Skipping source oauth parameter " + sourceOAuthParameter.oauthParameterId + " because the specified workspace " +
                sourceOAuthParameter.workspaceId +
                " doesn't exist and violates foreign key constraint."
            )
          }
          continue
        } else if (actorDefinitionDoesNotExist(sourceOAuthParameter.sourceDefinitionId, ctx)) {
          log.warn {
            (
              "Skipping source oauth parameter " + sourceOAuthParameter.sourceDefinitionId + " because the specified source definition " +
                sourceOAuthParameter.sourceDefinitionId +
                " doesn't exist and violates foreign key constraint."
            )
          }
          continue
        }
        ctx
          .insertInto(DSL.table("actor_oauth_parameter"))
          .set(id, sourceOAuthParameter.oauthParameterId)
          .set(workspaceId, sourceOAuthParameter.workspaceId)
          .set(actorDefinitionId, sourceOAuthParameter.sourceDefinitionId)
          .set(configuration, JSONB.valueOf(Jsons.serialize(sourceOAuthParameter.configuration)))
          .set(actorType, ActorType.source)
          .set(createdAt, OffsetDateTime.ofInstant(configWithMetadata.createdAt, ZoneOffset.UTC))
          .set(updatedAt, OffsetDateTime.ofInstant(configWithMetadata.updatedAt, ZoneOffset.UTC))
          .execute()
        sourceOauthParamRecords++
      }

      log.info { "actor_oauth_parameter table populated with $sourceOauthParamRecords source oauth params records" }

      val destinationOauthParamsWithMetadata =
        listConfigsWithMetadata(
          ConfigSchema.DESTINATION_OAUTH_PARAM,
          DestinationOAuthParameter::class.java,
          ctx,
        )
      var destinationOauthParamRecords = 0L
      for (configWithMetadata in destinationOauthParamsWithMetadata) {
        val destinationOAuthParameter = configWithMetadata.config
        if (workspaceDoesNotExist(destinationOAuthParameter.workspaceId, ctx)) {
          log.warn {
            (
              "Skipping destination oauth parameter " + destinationOAuthParameter.oauthParameterId + " because the specified workspace " +
                destinationOAuthParameter.workspaceId +
                " doesn't exist and violates foreign key constraint."
            )
          }
          continue
        } else if (actorDefinitionDoesNotExist(destinationOAuthParameter.destinationDefinitionId, ctx)) {
          log.warn {
            (
              "Skipping destination oauth parameter " + destinationOAuthParameter.oauthParameterId +
                " because the specified destination definition " +
                destinationOAuthParameter.destinationDefinitionId +
                " doesn't exist and violates foreign key constraint."
            )
          }
          continue
        }
        ctx
          .insertInto(DSL.table("actor_oauth_parameter"))
          .set(id, destinationOAuthParameter.oauthParameterId)
          .set(workspaceId, destinationOAuthParameter.workspaceId)
          .set(actorDefinitionId, destinationOAuthParameter.destinationDefinitionId)
          .set(configuration, JSONB.valueOf(Jsons.serialize(destinationOAuthParameter.configuration)))
          .set(actorType, ActorType.destination)
          .set(createdAt, OffsetDateTime.ofInstant(configWithMetadata.createdAt, ZoneOffset.UTC))
          .set(updatedAt, OffsetDateTime.ofInstant(configWithMetadata.updatedAt, ZoneOffset.UTC))
          .execute()
        destinationOauthParamRecords++
      }

      log.info { "actor_oauth_parameter table populated with $destinationOauthParamRecords destination oauth params records" }
    }

    private fun createAndPopulateOperation(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val workspaceId = DSL.field("workspace_id", SQLDataType.UUID.nullable(false))
      val name = DSL.field("name", SQLDataType.VARCHAR(256).nullable(false))
      val operatorType =
        DSL.field(
          "operator_type",
          SQLDataType.VARCHAR
            .asEnumDataType(
              OperatorType::class.java,
            ).nullable(false),
        )
      val operatorNormalization = DSL.field("operator_normalization", SQLDataType.JSONB.nullable(true))
      val operatorDbt = DSL.field("operator_dbt", SQLDataType.JSONB.nullable(true))
      val tombstone = DSL.field("tombstone", SQLDataType.BOOLEAN.nullable(false).defaultValue(false))
      val createdAt =
        DSL.field(
          "created_at",
          SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()),
        )
      val updatedAt =
        DSL.field(
          "updated_at",
          SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()),
        )

      ctx
        .createTableIfNotExists("operation")
        .columns(
          id,
          workspaceId,
          name,
          operatorType,
          operatorNormalization,
          operatorDbt,
          tombstone,
          createdAt,
          updatedAt,
        ).constraints(
          DSL.primaryKey(id),
          DSL.foreignKey(workspaceId).references("workspace", "id").onDeleteCascade(),
        ).execute()

      log.info { "operation table created" }

      val configsWithMetadata =
        listConfigsWithMetadata(
          ConfigSchema.STANDARD_SYNC_OPERATION,
          StandardSyncOperation::class.java,
          ctx,
        )
      var standardSyncOperationRecords = 0L
      for (configWithMetadata in configsWithMetadata) {
        val standardSyncOperation = configWithMetadata.config
        if (workspaceDoesNotExist(standardSyncOperation.workspaceId, ctx)) {
          log.warn {
            (
              "Skipping standard sync operation " + standardSyncOperation.operationId + " because the specified workspace " +
                standardSyncOperation.workspaceId +
                " doesn't exist and violates foreign key constraint."
            )
          }
          continue
        }
        ctx
          .insertInto(DSL.table("operation"))
          .set(id, standardSyncOperation.operationId)
          .set(workspaceId, standardSyncOperation.workspaceId)
          .set(name, standardSyncOperation.name)
          .set(
            operatorType,
            if (standardSyncOperation.operatorType == null) {
              null
            } else {
              Enums
                .toEnum(
                  standardSyncOperation.operatorType.value(),
                  OperatorType::class.java,
                ).orElse(OperatorType.normalization)
            },
          ).set(tombstone, standardSyncOperation.tombstone != null && standardSyncOperation.tombstone)
          .set(createdAt, OffsetDateTime.ofInstant(configWithMetadata.createdAt, ZoneOffset.UTC))
          .set(updatedAt, OffsetDateTime.ofInstant(configWithMetadata.updatedAt, ZoneOffset.UTC))
          .execute()
        standardSyncOperationRecords++
      }

      log.info { "operation table populated with $standardSyncOperationRecords records" }
    }

    private fun createConnectionOperation(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val connectionId = DSL.field("connection_id", SQLDataType.UUID.nullable(false))
      val operationId = DSL.field("operation_id", SQLDataType.UUID.nullable(false))
      val createdAt =
        DSL.field(
          "created_at",
          SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()),
        )
      val updatedAt =
        DSL.field(
          "updated_at",
          SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()),
        )

      ctx
        .createTableIfNotExists("connection_operation")
        .columns(
          id,
          connectionId,
          operationId,
          createdAt,
          updatedAt,
        ).constraints(
          DSL.primaryKey(id, connectionId, operationId),
          DSL.foreignKey(connectionId).references("connection", "id").onDeleteCascade(),
          DSL.foreignKey(operationId).references("operation", "id").onDeleteCascade(),
        ).execute()
      log.info { "connection_operation table created" }
    }

    private fun createAndPopulateConnection(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val namespaceDefinition =
        DSL
          .field(
            "namespace_definition",
            SQLDataType.VARCHAR
              .asEnumDataType(
                NamespaceDefinitionType::class.java,
              ).nullable(false),
          )
      val namespaceFormat = DSL.field("namespace_format", SQLDataType.VARCHAR(256).nullable(true))
      val prefix = DSL.field("prefix", SQLDataType.VARCHAR(256).nullable(true))
      val sourceId = DSL.field("source_id", SQLDataType.UUID.nullable(false))
      val destinationId = DSL.field("destination_id", SQLDataType.UUID.nullable(false))
      val name = DSL.field("name", SQLDataType.VARCHAR(256).nullable(false))
      val catalog = DSL.field("catalog", SQLDataType.JSONB.nullable(false))
      val status =
        DSL.field(
          "status",
          SQLDataType.VARCHAR
            .asEnumDataType(
              StatusType::class.java,
            ).nullable(true),
        )
      val schedule = DSL.field("schedule", SQLDataType.JSONB.nullable(true))
      val manual = DSL.field("manual", SQLDataType.BOOLEAN.nullable(false))
      val resourceRequirements = DSL.field("resource_requirements", SQLDataType.JSONB.nullable(true))
      val createdAt =
        DSL.field(
          "created_at",
          SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()),
        )
      val updatedAt =
        DSL.field(
          "updated_at",
          SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()),
        )

      ctx
        .createTableIfNotExists("connection")
        .columns(
          id,
          namespaceDefinition,
          namespaceFormat,
          prefix,
          sourceId,
          destinationId,
          name,
          catalog,
          status,
          schedule,
          manual,
          resourceRequirements,
          createdAt,
          updatedAt,
        ).constraints(
          DSL.primaryKey(id),
          DSL.foreignKey(sourceId).references("actor", "id").onDeleteCascade(),
          DSL.foreignKey(destinationId).references("actor", "id").onDeleteCascade(),
        ).execute()
      ctx.createIndex("connection_source_id_idx").on("connection", "source_id").execute()
      ctx.createIndex("connection_destination_id_idx").on("connection", "destination_id").execute()

      log.info { "connection table created" }
      createConnectionOperation(ctx)

      val configsWithMetadata =
        listConfigsWithMetadata(
          ConfigSchema.STANDARD_SYNC,
          StandardSync::class.java,
          ctx,
        )
      var standardSyncRecords = 0L
      for (configWithMetadata in configsWithMetadata) {
        val standardSync = configWithMetadata.config
        if (actorDoesNotExist(standardSync.sourceId, ctx)) {
          log.warn {
            (
              "Skipping standard sync " + standardSync.connectionId + " because the specified source " + standardSync.sourceId +
                " doesn't exist and violates foreign key constraint."
            )
          }
          continue
        } else if (actorDoesNotExist(standardSync.destinationId, ctx)) {
          log.warn {
            (
              "Skipping standard sync " + standardSync.connectionId + " because the specified destination " + standardSync.destinationId +
                " doesn't exist and violates foreign key constraint."
            )
          }
          continue
        }
        ctx
          .insertInto(DSL.table("connection"))
          .set(id, standardSync.connectionId)
          .set(
            namespaceDefinition,
            if (standardSync.namespaceDefinition == null) {
              null
            } else {
              Enums
                .toEnum(
                  standardSync.namespaceDefinition.value(),
                  NamespaceDefinitionType::class.java,
                ).orElseThrow()
            },
          ).set(namespaceFormat, standardSync.namespaceFormat)
          .set(prefix, standardSync.prefix)
          .set(sourceId, standardSync.sourceId)
          .set(destinationId, standardSync.destinationId)
          .set(name, standardSync.name)
          .set(catalog, JSONB.valueOf(Jsons.serialize(standardSync.catalog)))
          .set(
            status,
            if (standardSync.status == null) {
              null
            } else {
              Enums
                .toEnum(
                  standardSync.status.value(),
                  StatusType::class.java,
                ).orElseThrow()
            },
          ).set(schedule, JSONB.valueOf(Jsons.serialize(standardSync.schedule)))
          .set(manual, standardSync.manual)
          .set(resourceRequirements, JSONB.valueOf(Jsons.serialize(standardSync.resourceRequirements)))
          .set(createdAt, OffsetDateTime.ofInstant(configWithMetadata.createdAt, ZoneOffset.UTC))
          .set(updatedAt, OffsetDateTime.ofInstant(configWithMetadata.updatedAt, ZoneOffset.UTC))
          .execute()
        standardSyncRecords++
        populateConnectionOperation(ctx, configWithMetadata)
      }

      log.info { "connection table populated with $standardSyncRecords records" }
    }

    private fun createAndPopulateState(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val connectionId = DSL.field("connection_id", SQLDataType.UUID.nullable(false))
      val state = DSL.field("state", SQLDataType.JSONB.nullable(true))
      val createdAt =
        DSL.field(
          "created_at",
          SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()),
        )
      val updatedAt =
        DSL.field(
          "updated_at",
          SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()),
        )

      ctx
        .createTableIfNotExists("state")
        .columns(
          id,
          connectionId,
          state,
          createdAt,
          updatedAt,
        ).constraints(
          DSL.primaryKey(id, connectionId),
          DSL.foreignKey(connectionId).references("connection", "id").onDeleteCascade(),
        ).execute()

      log.info { "state table created" }

      val configsWithMetadata =
        listConfigsWithMetadata(
          ConfigSchema.STANDARD_SYNC_STATE,
          StandardSyncState::class.java,
          ctx,
        )
      var standardSyncStateRecords = 0L
      for (configWithMetadata in configsWithMetadata) {
        val standardSyncState = configWithMetadata.config
        if (connectionDoesNotExist(standardSyncState.connectionId, ctx)) {
          log.warn {
            (
              "Skipping standard sync state because the specified standard sync " + standardSyncState.connectionId +
                " doesn't exist and violates foreign key constraint."
            )
          }
          continue
        }
        ctx
          .insertInto(DSL.table("state"))
          .set(id, UUID.randomUUID())
          .set(connectionId, standardSyncState.connectionId)
          .set(state, JSONB.valueOf(Jsons.serialize(standardSyncState.state)))
          .set(createdAt, OffsetDateTime.ofInstant(configWithMetadata.createdAt, ZoneOffset.UTC))
          .set(updatedAt, OffsetDateTime.ofInstant(configWithMetadata.updatedAt, ZoneOffset.UTC))
          .execute()
        standardSyncStateRecords++
      }

      log.info { "state table populated with $standardSyncStateRecords records" }
    }

    private fun populateConnectionOperation(
      ctx: DSLContext,
      standardSyncWithMetadata: ConfigWithMetadata<StandardSync>,
    ) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val connectionId = DSL.field("connection_id", SQLDataType.UUID.nullable(false))
      val operationId = DSL.field("operation_id", SQLDataType.UUID.nullable(false))
      val createdAt =
        DSL.field(
          "created_at",
          SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()),
        )
      val updatedAt =
        DSL.field(
          "updated_at",
          SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()),
        )

      val standardSync = standardSyncWithMetadata.config

      if (connectionDoesNotExist(standardSync.connectionId, ctx)) {
        log.warn {
          (
            "Skipping connection_operations because the specified standard sync " + standardSync.connectionId +
              " doesn't exist and violates foreign key constraint."
          )
        }
        return
      }
      var connectionOperationRecords = 0L
      for (operationIdFromStandardSync in standardSync.operationIds) {
        if (operationDoesNotExist(operationIdFromStandardSync, ctx)) {
          log.warn {
            (
              "Skipping connection_operations because the specified standard sync operation " + operationIdFromStandardSync +
                " doesn't exist and violates foreign key constraint."
            )
          }
          continue
        }
        ctx
          .insertInto(DSL.table("connection_operation"))
          .set(id, UUID.randomUUID())
          .set(connectionId, standardSync.connectionId)
          .set(operationId, operationIdFromStandardSync)
          .set(createdAt, OffsetDateTime.ofInstant(standardSyncWithMetadata.createdAt, ZoneOffset.UTC))
          .set(updatedAt, OffsetDateTime.ofInstant(standardSyncWithMetadata.updatedAt, ZoneOffset.UTC))
          .execute()
        connectionOperationRecords++
      }
      log.info { "connection_operation table populated with $connectionOperationRecords records" }
    }

    @JvmStatic
    fun <T> listConfigsWithMetadata(
      airbyteConfigType: AirbyteConfig,
      clazz: Class<T>?,
      ctx: DSLContext,
    ): List<ConfigWithMetadata<T>> {
      val configId = DSL.field("config_id", SQLDataType.VARCHAR(36).nullable(false))
      val configType = DSL.field("config_type", SQLDataType.VARCHAR(60).nullable(false))
      val createdAt =
        DSL.field(
          "created_at",
          SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()),
        )
      val updatedAt =
        DSL.field(
          "updated_at",
          SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()),
        )
      val configBlob = DSL.field("config_blob", SQLDataType.JSONB.nullable(false))
      val results =
        ctx
          .select(DSL.asterisk())
          .from(DSL.table("airbyte_configs"))
          .where(configType.eq(airbyteConfigType.name()))
          .fetch()

      return results
        .stream()
        .map { record: Record ->
          ConfigWithMetadata(
            record.get(configId),
            record.get(configType),
            record.get(createdAt).toInstant(),
            record.get(updatedAt).toInstant(),
            Jsons.deserialize(record.get(configBlob).data(), clazz),
          )
        }.collect(Collectors.toList())
    }
  }
}
