/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.migrations.V0_50_33_014__AddScopedConfigurationTable.Companion.createOriginTypeEnum
import io.airbyte.db.instance.configs.migrations.V0_50_33_014__AddScopedConfigurationTable.Companion.createResourceTypeEnum
import io.airbyte.db.instance.configs.migrations.V0_50_33_014__AddScopedConfigurationTable.Companion.createScopeTypeEnum
import io.airbyte.db.instance.configs.migrations.V0_50_33_014__AddScopedConfigurationTable.Companion.createScopedConfigurationTable
import org.jooq.exception.DataAccessException
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.io.IOException
import java.sql.SQLException
import java.time.OffsetDateTime
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_50_33_014__AddScopedConfigurationTableTest : AbstractConfigsDatabaseTest() {
  @Test
  @Throws(SQLException::class, IOException::class)
  fun test() {
    val context = dslContext!!

    createResourceTypeEnum(context)
    createScopeTypeEnum(context)
    createOriginTypeEnum(context)
    createScopedConfigurationTable(context)

    val configId = UUID.randomUUID()
    val configKey = "connectorVersion"
    val value = "0.1.0"
    val description = "Version change for OC"
    val referenceUrl = "https://github.com/airbytehq/airbyte"
    val resourceType = V0_50_33_014__AddScopedConfigurationTable.ConfigResourceType.ACTOR_DEFINITION
    val resourceId = UUID.randomUUID()
    val scopeType = V0_50_33_014__AddScopedConfigurationTable.ConfigScopeType.WORKSPACE
    val scopeId = UUID.randomUUID()
    val originType = V0_50_33_014__AddScopedConfigurationTable.ConfigOriginType.USER
    val origin = UUID.randomUUID().toString()
    val expiresAt = OffsetDateTime.now()

    // assert can insert
    Assertions.assertDoesNotThrow {
      context
        .insertInto(DSL.table(SCOPED_CONFIGURATION))
        .columns(
          DSL.field(ID),
          DSL.field(KEY),
          DSL.field(RESOURCE_TYPE),
          DSL.field(RESOURCE_ID),
          DSL.field(SCOPE_TYPE),
          DSL.field(SCOPE_ID),
          DSL.field(VALUE),
          DSL.field(DESCRIPTION),
          DSL.field(REFERENCE_URL),
          DSL.field(ORIGIN_TYPE),
          DSL.field(ORIGIN),
          DSL.field(EXPIRES_AT),
        ).values(
          configId,
          configKey,
          resourceType,
          resourceId,
          scopeType,
          scopeId,
          value,
          description,
          referenceUrl,
          originType,
          origin,
          expiresAt,
        ).execute()
    }

    val configId2 = UUID.randomUUID()
    val scopeId2 = UUID.randomUUID()
    val value2 = "0.2.0"

    // insert another one
    Assertions.assertDoesNotThrow {
      context
        .insertInto(DSL.table(SCOPED_CONFIGURATION))
        .columns(
          DSL.field(ID),
          DSL.field(KEY),
          DSL.field(RESOURCE_TYPE),
          DSL.field(RESOURCE_ID),
          DSL.field(SCOPE_TYPE),
          DSL.field(SCOPE_ID),
          DSL.field(VALUE),
          DSL.field(DESCRIPTION),
          DSL.field(REFERENCE_URL),
          DSL.field(ORIGIN_TYPE),
          DSL.field(ORIGIN),
        ).values(
          configId2,
          configKey,
          resourceType,
          resourceId,
          scopeType,
          scopeId2,
          value2,
          description,
          referenceUrl,
          originType,
          origin,
        ).execute()
    }

    val configId3 = UUID.randomUUID()

    // assert key + resource_type + resource_id + scope_type + scope_id is unique
    val e: Exception =
      Assertions.assertThrows(
        DataAccessException::class.java,
      ) {
        context
          .insertInto(
            DSL.table(
              SCOPED_CONFIGURATION,
            ),
          ).columns(
            DSL.field(ID),
            DSL.field(KEY),
            DSL.field(RESOURCE_TYPE),
            DSL.field(RESOURCE_ID),
            DSL.field(SCOPE_TYPE),
            DSL.field(SCOPE_ID),
            DSL.field(VALUE),
            DSL.field(DESCRIPTION),
            DSL.field(REFERENCE_URL),
            DSL.field(ORIGIN_TYPE),
            DSL.field(ORIGIN),
            DSL.field(EXPIRES_AT),
          ).values(
            configId3,
            configKey,
            resourceType,
            resourceId,
            scopeType,
            scopeId,
            value2,
            description,
            referenceUrl,
            originType,
            origin,
            expiresAt,
          ).execute()
      }
    Assertions.assertTrue(
      e.message?.contains("duplicate key value violates unique constraint \"scoped_configuration_key_resource_type_resource_id_scope_ty_key\"")
        ?: false,
    )
  }

  companion object {
    private const val SCOPED_CONFIGURATION = "scoped_configuration"
    private const val ID = "id"
    private const val KEY = "key"
    private const val RESOURCE_TYPE = "resource_type"
    private const val RESOURCE_ID = "resource_id"
    private const val SCOPE_TYPE = "scope_type"
    private const val SCOPE_ID = "scope_id"
    private const val VALUE = "value"
    private const val DESCRIPTION = "description"
    private const val REFERENCE_URL = "reference_url"
    private const val ORIGIN_TYPE = "origin_type"
    private const val ORIGIN = "origin"
    private const val EXPIRES_AT = "expires_at"
  }
}
