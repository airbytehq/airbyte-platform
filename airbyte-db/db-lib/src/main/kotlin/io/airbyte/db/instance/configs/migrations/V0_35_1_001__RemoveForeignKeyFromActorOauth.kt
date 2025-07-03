/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.json.Jsons
import io.airbyte.config.DestinationOAuthParameter
import io.airbyte.config.SourceOAuthParameter
import io.airbyte.db.instance.configs.migrations.V0_32_8_001__AirbyteConfigDatabaseDenormalization.Companion.actorDefinitionDoesNotExist
import io.airbyte.db.instance.configs.migrations.V0_32_8_001__AirbyteConfigDatabaseDenormalization.Companion.listConfigsWithMetadata
import io.airbyte.db.legacy.ConfigSchema
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

private val log = KotlinLogging.logger {}

/**
 * Remove foreign key from actor oauth migration.
 */
@Suppress("ktlint:standard:class-naming")
class V0_35_1_001__RemoveForeignKeyFromActorOauth : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    migrate(ctx)
  }

  companion object {
    @JvmStatic
    @VisibleForTesting
    fun migrate(ctx: DSLContext) {
      dropForeignKeyConstraintFromActorOauthTable(ctx)
      populateActorOauthParameter(ctx)
    }

    private fun dropForeignKeyConstraintFromActorOauthTable(ctx: DSLContext) {
      ctx.alterTable("actor_oauth_parameter").dropForeignKey("actor_oauth_parameter_workspace_id_fkey").execute()
      log.info { "actor_oauth_parameter_workspace_id_fkey constraint dropped" }
    }

    private fun populateActorOauthParameter(ctx: DSLContext) {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      val actorDefinitionId = DSL.field("actor_definition_id", SQLDataType.UUID.nullable(false))
      val configuration = DSL.field("configuration", SQLDataType.JSONB.nullable(false))
      val workspaceId = DSL.field("workspace_id", SQLDataType.UUID.nullable(true))
      val actorType =
        DSL.field(
          "actor_type",
          SQLDataType.VARCHAR.asEnumDataType(V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType::class.java).nullable(false),
        )
      val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
      val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

      val sourceOauthParamsWithMetadata = listConfigsWithMetadata(ConfigSchema.SOURCE_OAUTH_PARAM, SourceOAuthParameter::class.java, ctx)
      var sourceOauthParamRecords = 0L
      for (configWithMetadata in sourceOauthParamsWithMetadata) {
        val sourceOAuthParameter = configWithMetadata.config
        if (actorDefinitionDoesNotExist(sourceOAuthParameter.sourceDefinitionId, ctx)) {
          log.warn {
            (
              "Skipping source oauth parameter " + sourceOAuthParameter.sourceDefinitionId + " because the specified source definition " +
                sourceOAuthParameter.sourceDefinitionId +
                " doesn't exist and violates foreign key constraint."
            )
          }
          continue
        } else if (actorOAuthParamExists(sourceOAuthParameter.oauthParameterId, ctx)) {
          log.warn {
            (
              "Skipping source oauth parameter " + sourceOAuthParameter.oauthParameterId +
                " because the specified parameter already exists in the table."
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
          .set(actorType, V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType.source)
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
        if (actorDefinitionDoesNotExist(destinationOAuthParameter.destinationDefinitionId, ctx)) {
          log.warn {
            (
              "Skipping destination oauth parameter " + destinationOAuthParameter.oauthParameterId +
                " because the specified destination definition " +
                destinationOAuthParameter.destinationDefinitionId +
                " doesn't exist and violates foreign key constraint."
            )
          }
          continue
        } else if (actorOAuthParamExists(destinationOAuthParameter.oauthParameterId, ctx)) {
          log.warn {
            (
              "Skipping destination oauth parameter " + destinationOAuthParameter.oauthParameterId +
                " because the specified parameter already exists in the table."
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
          .set(actorType, V0_32_8_001__AirbyteConfigDatabaseDenormalization.ActorType.destination)
          .set(createdAt, OffsetDateTime.ofInstant(configWithMetadata.createdAt, ZoneOffset.UTC))
          .set(updatedAt, OffsetDateTime.ofInstant(configWithMetadata.updatedAt, ZoneOffset.UTC))
          .execute()
        destinationOauthParamRecords++
      }

      log.info { "actor_oauth_parameter table populated with $destinationOauthParamRecords destination oauth params records" }
    }

    private fun actorOAuthParamExists(
      oauthParamId: UUID,
      ctx: DSLContext,
    ): Boolean {
      val id = DSL.field("id", SQLDataType.UUID.nullable(false))
      return ctx.fetchExists(DSL.select().from(DSL.table("actor_oauth_parameter")).where(id.eq(oauthParamId)))
    }
  }
}
