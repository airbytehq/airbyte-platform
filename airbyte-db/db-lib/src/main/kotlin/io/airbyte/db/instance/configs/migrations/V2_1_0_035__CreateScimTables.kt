/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.DatabaseConstants.GROUP_TABLE
import io.airbyte.db.instance.DatabaseConstants.ORGANIZATION_TABLE
import io.airbyte.db.instance.DatabaseConstants.SCIM_CONFIGURATION_TABLE
import io.airbyte.db.instance.DatabaseConstants.SCIM_RESOURCE_MAPPING_TABLE
import io.airbyte.db.instance.DatabaseConstants.USER_TABLE
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

/**
 * Creates the organization-scoped SCIM configuration and resource-mapping schema.
 *
 * The schema is reversible with a follow-up migration that drops the resource mapping table, the
 * configuration table, and the resource-type enum, in that order. Flyway migrations are not rolled
 * back in place.
 */
@Suppress("ktlint:standard:class-naming")
class V2_1_0_035__CreateScimTables : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    createScimTables(ctx)
  }

  companion object {
    private const val SCIM_RESOURCE_TYPE = "scim_resource_type"

    @JvmStatic
    fun createScimTables(ctx: DSLContext) {
      createScimResourceType(ctx)
      createScimConfigurationTable(ctx)
      createScimResourceMappingTable(ctx)
      createScimIndexes(ctx)
    }

    private fun createScimResourceType(ctx: DSLContext) {
      ctx.execute("CREATE TYPE $SCIM_RESOURCE_TYPE AS ENUM ('USER', 'GROUP')")
    }

    private fun createScimConfigurationTable(ctx: DSLContext) {
      ctx.execute(
        """
        CREATE TABLE $SCIM_CONFIGURATION_TABLE (
          id UUID PRIMARY KEY,
          organization_id UUID NOT NULL,
          token_hash TEXT,
          idp_provider TEXT,
          enabled BOOLEAN NOT NULL DEFAULT FALSE,
          created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
          created_by_user_id UUID,
          token_issued_at TIMESTAMPTZ,
          token_issued_by_user_id UUID,
          disabled_at TIMESTAMPTZ,
          disabled_by_user_id UUID,
          CONSTRAINT scim_configuration_organization_id_key UNIQUE (organization_id),
          CONSTRAINT scim_configuration_id_organization_id_key UNIQUE (id, organization_id),
          CONSTRAINT scim_configuration_organization_id_fkey
            FOREIGN KEY (organization_id) REFERENCES $ORGANIZATION_TABLE(id) ON DELETE CASCADE,
          CONSTRAINT scim_configuration_created_by_user_id_fkey
            FOREIGN KEY (created_by_user_id) REFERENCES "$USER_TABLE"(id) ON DELETE SET NULL,
          CONSTRAINT scim_configuration_token_issued_by_user_id_fkey
            FOREIGN KEY (token_issued_by_user_id) REFERENCES "$USER_TABLE"(id) ON DELETE SET NULL,
          CONSTRAINT scim_configuration_disabled_by_user_id_fkey
            FOREIGN KEY (disabled_by_user_id) REFERENCES "$USER_TABLE"(id) ON DELETE SET NULL,
          CONSTRAINT scim_configuration_state_check CHECK (
            (
              enabled
              AND token_hash IS NOT NULL
              AND token_issued_at IS NOT NULL
              AND disabled_at IS NULL
              AND disabled_by_user_id IS NULL
            )
            OR
            (
              NOT enabled
              AND token_hash IS NULL
              AND token_issued_at IS NULL
              AND token_issued_by_user_id IS NULL
              AND disabled_at IS NOT NULL
            )
          )
        )
        """.trimIndent(),
      )
    }

    private fun createScimResourceMappingTable(ctx: DSLContext) {
      ctx.execute(
        """
        CREATE TABLE $SCIM_RESOURCE_MAPPING_TABLE (
          id UUID PRIMARY KEY,
          scim_configuration_id UUID NOT NULL,
          organization_id UUID NOT NULL,
          resource_type $SCIM_RESOURCE_TYPE NOT NULL,
          user_id UUID,
          group_id UUID,
          external_id TEXT,
          user_name TEXT,
          primary_email TEXT,
          user_active BOOLEAN,
          attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
          CONSTRAINT scim_resource_mapping_configuration_organization_fkey
            FOREIGN KEY (scim_configuration_id, organization_id)
            REFERENCES $SCIM_CONFIGURATION_TABLE(id, organization_id) ON DELETE CASCADE,
          CONSTRAINT scim_resource_mapping_user_id_fkey
            FOREIGN KEY (user_id) REFERENCES "$USER_TABLE"(id) ON DELETE CASCADE,
          CONSTRAINT scim_resource_mapping_group_organization_fkey
            FOREIGN KEY (group_id, organization_id)
            REFERENCES "$GROUP_TABLE"(id, organization_id) ON DELETE CASCADE,
          CONSTRAINT scim_resource_mapping_external_id_non_blank_check
            CHECK (external_id IS NULL OR external_id ~ '[^[:space:]]'),
          CONSTRAINT scim_resource_mapping_attributes_object_check
            CHECK (jsonb_typeof(attributes) = 'object'),
          CONSTRAINT scim_resource_mapping_resource_shape_check CHECK (
            (
              resource_type = 'USER'
              AND user_id IS NOT NULL
              AND group_id IS NULL
              AND user_name IS NOT NULL
              AND user_name ~ '[^[:space:]]'
              AND primary_email IS NOT NULL
              AND primary_email ~ '[^[:space:]]'
              AND user_active IS NOT NULL
            )
            OR
            (
              resource_type = 'GROUP'
              AND user_id IS NULL
              AND group_id IS NOT NULL
              AND user_name IS NULL
              AND primary_email IS NULL
              AND user_active IS NULL
              AND attributes = '{}'::jsonb
            )
          )
        )
        """.trimIndent(),
      )
    }

    private fun createScimIndexes(ctx: DSLContext) {
      ctx.execute(
        """
        CREATE UNIQUE INDEX scim_configuration_token_hash_key
          ON $SCIM_CONFIGURATION_TABLE (token_hash)
          WHERE token_hash IS NOT NULL
        """.trimIndent(),
      )
      ctx.execute(
        """
        CREATE UNIQUE INDEX scim_resource_mapping_external_id_key
          ON $SCIM_RESOURCE_MAPPING_TABLE (scim_configuration_id, resource_type, external_id)
          WHERE external_id IS NOT NULL
        """.trimIndent(),
      )
      ctx.execute(
        """
        CREATE UNIQUE INDEX scim_resource_mapping_user_name_key
          ON $SCIM_RESOURCE_MAPPING_TABLE (scim_configuration_id, resource_type, LOWER(user_name))
          WHERE resource_type = 'USER'
        """.trimIndent(),
      )
      ctx.execute(
        """
        CREATE UNIQUE INDEX scim_resource_mapping_primary_email_key
          ON $SCIM_RESOURCE_MAPPING_TABLE (scim_configuration_id, resource_type, LOWER(primary_email))
          WHERE resource_type = 'USER'
        """.trimIndent(),
      )
      ctx.execute(
        """
        CREATE INDEX scim_resource_mapping_primary_email_lookup_idx
          ON $SCIM_RESOURCE_MAPPING_TABLE (LOWER(primary_email))
          WHERE resource_type = 'USER'
        """.trimIndent(),
      )
      ctx.execute(
        """
        CREATE UNIQUE INDEX scim_resource_mapping_configuration_user_key
          ON $SCIM_RESOURCE_MAPPING_TABLE (scim_configuration_id, resource_type, user_id)
          WHERE resource_type = 'USER'
        """.trimIndent(),
      )
      ctx.execute(
        """
        CREATE UNIQUE INDEX scim_resource_mapping_group_key
          ON $SCIM_RESOURCE_MAPPING_TABLE (group_id)
          WHERE resource_type = 'GROUP'
        """.trimIndent(),
      )
      ctx.execute(
        """
        CREATE INDEX scim_resource_mapping_configuration_resource_idx
          ON $SCIM_RESOURCE_MAPPING_TABLE (
            scim_configuration_id,
            resource_type,
            organization_id,
            created_at,
            id
          )
        """.trimIndent(),
      )
    }
  }
}
