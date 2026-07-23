/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.factory.FlywayFactory
import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.ConfigsDatabaseMigrator
import io.airbyte.db.instance.development.DevDatabaseMigrator
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.jooq.DSLContext
import org.jooq.exception.DataAccessException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
internal class V2_1_0_035__CreateScimTablesTest : AbstractConfigsDatabaseTest() {
  private lateinit var ctx: DSLContext

  @BeforeEach
  fun beforeEach() {
    val flyway =
      FlywayFactory.create(
        dataSource,
        "V2_1_0_035__CreateScimTablesTest",
        ConfigsDatabaseMigrator.DB_IDENTIFIER,
        ConfigsDatabaseMigrator.MIGRATION_FILE_LOCATION,
      )

    val configsDbMigrator = ConfigsDatabaseMigrator(database!!, flyway)
    val previousMigration: BaseJavaMigration = V2_1_0_034__AddGroupOrganizationKey()
    val devConfigsDbMigrator = DevDatabaseMigrator(configsDbMigrator, previousMigration.version)
    devConfigsDbMigrator.createBaseline()

    ctx = dslContext!!
    ctx.execute("DROP TABLE IF EXISTS scim_resource_mapping")
    ctx.execute("DROP TABLE IF EXISTS scim_external_id")
    ctx.execute("DROP TABLE IF EXISTS scim_configuration")
    ctx.execute("DROP TYPE IF EXISTS scim_resource_type")

    V2_1_0_035__CreateScimTables.createScimTables(ctx)
  }

  @Test
  fun `creates the configuration and resource mapping schema`() {
    assertFalse(tableExists("scim_external_id"))
    assertTrue(tableExists("scim_configuration"))
    assertTrue(tableExists("scim_resource_mapping"))
    assertEquals(listOf("USER", "GROUP"), enumValues("scim_resource_type"))

    assertColumn("scim_configuration", "id", "uuid", isNullable = false)
    assertColumn("scim_configuration", "organization_id", "uuid", isNullable = false)
    assertColumn("scim_configuration", "token_hash", "text", isNullable = true)
    assertColumn("scim_configuration", "idp_provider", "text", isNullable = true)
    assertColumn("scim_configuration", "enabled", "boolean", isNullable = false, defaultContains = "false")
    assertColumn("scim_configuration", "created_at", "timestamp with time zone", isNullable = false)
    assertColumn("scim_configuration", "updated_at", "timestamp with time zone", isNullable = false)
    assertColumn("scim_configuration", "created_by_user_id", "uuid", isNullable = true)
    assertColumn("scim_configuration", "token_issued_at", "timestamp with time zone", isNullable = true)
    assertColumn("scim_configuration", "token_issued_by_user_id", "uuid", isNullable = true)
    assertColumn("scim_configuration", "disabled_at", "timestamp with time zone", isNullable = true)
    assertColumn("scim_configuration", "disabled_by_user_id", "uuid", isNullable = true)
    listOf(
      "token_created_at",
      "token_rotated_at",
      "token_expires_at",
      "token_last_auth_at",
      "token_last_auth_failure_at",
      "token_auth_failure_count",
    ).forEach { assertColumnDoesNotExist("scim_configuration", it) }

    assertColumn("scim_resource_mapping", "id", "uuid", isNullable = false)
    assertColumn("scim_resource_mapping", "scim_configuration_id", "uuid", isNullable = false)
    assertColumn("scim_resource_mapping", "organization_id", "uuid", isNullable = false)
    assertColumn(
      "scim_resource_mapping",
      "resource_type",
      "USER-DEFINED",
      isNullable = false,
      expectedUdtName = "scim_resource_type",
    )
    assertColumn("scim_resource_mapping", "user_id", "uuid", isNullable = true)
    assertColumn("scim_resource_mapping", "group_id", "uuid", isNullable = true)
    assertColumn("scim_resource_mapping", "external_id", "text", isNullable = true)
    assertColumn("scim_resource_mapping", "user_name", "text", isNullable = true)
    assertColumn("scim_resource_mapping", "primary_email", "text", isNullable = true)
    assertColumn("scim_resource_mapping", "user_active", "boolean", isNullable = true)
    assertColumn("scim_resource_mapping", "attributes", "jsonb", isNullable = false, defaultContains = "{}")
    assertColumn("scim_resource_mapping", "created_at", "timestamp with time zone", isNullable = false)
    assertColumn("scim_resource_mapping", "updated_at", "timestamp with time zone", isNullable = false)
    assertColumnDoesNotExist("scim_resource_mapping", "internal_id")

    assertUniqueConstraint("scim_configuration", "scim_configuration_organization_id_key", "organization_id")
    assertUniqueConstraint(
      "scim_configuration",
      "scim_configuration_id_organization_id_key",
      "id,organization_id",
    )
    assertCheckConstraintContains(
      "scim_configuration",
      "scim_configuration_state_check",
      "token_hash IS NOT NULL",
      "disabled_at IS NOT NULL",
    )
    assertCheckConstraintContains(
      "scim_resource_mapping",
      "scim_resource_mapping_external_id_non_blank_check",
      "external_id ~ '[^[:space:]]'",
    )
    assertCheckConstraintContains(
      "scim_resource_mapping",
      "scim_resource_mapping_attributes_object_check",
      "jsonb_typeof(attributes)",
    )
    assertCheckConstraintContains(
      "scim_resource_mapping",
      "scim_resource_mapping_resource_shape_check",
      "resource_type = 'USER'",
      "resource_type = 'GROUP'",
    )

    assertForeignKey(
      tableName = "scim_configuration",
      constraintName = "scim_configuration_organization_id_fkey",
      expectedColumns = "organization_id",
      expectedReferencedTable = "organization",
      expectedReferencedColumns = "id",
      expectedDeleteAction = "c",
    )
    listOf(
      "scim_configuration_created_by_user_id_fkey" to "created_by_user_id",
      "scim_configuration_token_issued_by_user_id_fkey" to "token_issued_by_user_id",
      "scim_configuration_disabled_by_user_id_fkey" to "disabled_by_user_id",
    ).forEach { (constraintName, columnName) ->
      assertForeignKey(
        tableName = "scim_configuration",
        constraintName = constraintName,
        expectedColumns = columnName,
        expectedReferencedTable = "user",
        expectedReferencedColumns = "id",
        expectedDeleteAction = "n",
      )
    }
    assertForeignKey(
      tableName = "scim_resource_mapping",
      constraintName = "scim_resource_mapping_configuration_organization_fkey",
      expectedColumns = "scim_configuration_id,organization_id",
      expectedReferencedTable = "scim_configuration",
      expectedReferencedColumns = "id,organization_id",
      expectedDeleteAction = "c",
    )
    assertForeignKey(
      tableName = "scim_resource_mapping",
      constraintName = "scim_resource_mapping_user_id_fkey",
      expectedColumns = "user_id",
      expectedReferencedTable = "user",
      expectedReferencedColumns = "id",
      expectedDeleteAction = "c",
    )
    assertForeignKey(
      tableName = "scim_resource_mapping",
      constraintName = "scim_resource_mapping_group_organization_fkey",
      expectedColumns = "group_id,organization_id",
      expectedReferencedTable = "group",
      expectedReferencedColumns = "id,organization_id",
      expectedDeleteAction = "c",
    )

    assertIndexContains("scim_configuration_token_hash_key", "UNIQUE", "(token_hash)", "token_hash IS NOT NULL")
    assertIndexContains(
      "scim_resource_mapping_external_id_key",
      "UNIQUE",
      "(scim_configuration_id, resource_type, external_id)",
      "external_id IS NOT NULL",
    )
    assertIndexContains(
      "scim_resource_mapping_user_name_key",
      "UNIQUE",
      "scim_configuration_id, resource_type, lower(user_name)",
      "resource_type = 'USER'",
    )
    assertIndexContains(
      "scim_resource_mapping_primary_email_key",
      "UNIQUE",
      "scim_configuration_id, resource_type, lower(primary_email)",
      "resource_type = 'USER'",
    )
    assertIndexContains(
      "scim_resource_mapping_primary_email_lookup_idx",
      "lower(primary_email)",
      "resource_type = 'USER'",
    )
    assertIndexContains(
      "scim_resource_mapping_configuration_user_key",
      "UNIQUE",
      "(scim_configuration_id, resource_type, user_id)",
      "resource_type = 'USER'",
    )
    assertIndexContains("scim_resource_mapping_group_key", "UNIQUE", "(group_id)", "resource_type = 'GROUP'")
    assertIndexContains(
      "scim_resource_mapping_configuration_resource_idx",
      "(scim_configuration_id, resource_type, organization_id, created_at, id)",
    )
  }

  @Test
  fun `enforces configuration lifecycle state and scoped token uniqueness`() {
    val organizationId = insertOrganization()
    val otherOrganizationId = insertOrganization()
    val enabledConfigurationId = UUID.randomUUID()

    insertEnabledConfiguration(enabledConfigurationId, organizationId, "shared-hash")
    insertDisabledConfiguration(UUID.randomUUID(), otherOrganizationId)

    assertEquals(false, ctx.fetchValue("SELECT enabled FROM scim_configuration WHERE organization_id = ?", otherOrganizationId))
    assertThrows(DataAccessException::class.java) {
      insertDisabledConfiguration(UUID.randomUUID(), organizationId)
    }
    assertThrows(DataAccessException::class.java) {
      ctx.execute(
        """
        INSERT INTO scim_configuration (id, organization_id, enabled, token_hash, token_issued_at)
        VALUES (?, ?, TRUE, NULL, CURRENT_TIMESTAMP)
        """.trimIndent(),
        UUID.randomUUID(),
        insertOrganization(),
      )
    }
    assertThrows(DataAccessException::class.java) {
      ctx.execute(
        """
        INSERT INTO scim_configuration (
          id, organization_id, enabled, token_issued_by_user_id, disabled_at
        )
        VALUES (?, ?, FALSE, ?, CURRENT_TIMESTAMP)
        """.trimIndent(),
        UUID.randomUUID(),
        insertOrganization(),
        insertUser(),
      )
    }
    assertThrows(DataAccessException::class.java) {
      ctx.execute(
        """
        INSERT INTO scim_configuration (id, organization_id, enabled, token_hash, token_issued_at)
        VALUES (?, ?, TRUE, 'missing-issued-at', NULL)
        """.trimIndent(),
        UUID.randomUUID(),
        insertOrganization(),
      )
    }
    assertThrows(DataAccessException::class.java) {
      ctx.execute(
        """
        INSERT INTO scim_configuration (id, organization_id, enabled, token_hash, token_issued_at, disabled_at)
        VALUES (?, ?, TRUE, 'enabled-with-disabled-state', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """.trimIndent(),
        UUID.randomUUID(),
        insertOrganization(),
      )
    }
    assertThrows(DataAccessException::class.java) {
      ctx.execute(
        """
        INSERT INTO scim_configuration (id, organization_id, enabled, disabled_at)
        VALUES (?, ?, FALSE, NULL)
        """.trimIndent(),
        UUID.randomUUID(),
        insertOrganization(),
      )
    }
    assertThrows(DataAccessException::class.java) {
      ctx.execute(
        """
        INSERT INTO scim_configuration (id, organization_id, enabled, token_hash, disabled_at)
        VALUES (?, ?, FALSE, 'disabled-with-token', CURRENT_TIMESTAMP)
        """.trimIndent(),
        UUID.randomUUID(),
        insertOrganization(),
      )
    }

    val duplicateTokenOrganizationId = insertOrganization()
    assertThrows(DataAccessException::class.java) {
      insertEnabledConfiguration(UUID.randomUUID(), duplicateTokenOrganizationId, "shared-hash")
    }
    insertDisabledConfiguration(UUID.randomUUID(), duplicateTokenOrganizationId)
  }

  @Test
  fun `sets deleted configuration actors to null`() {
    val organizationId = insertOrganization()
    val createdByUserId = insertUser()
    val issuedByUserId = insertUser()
    val configurationId = UUID.randomUUID()
    insertEnabledConfiguration(
      id = configurationId,
      organizationId = organizationId,
      tokenHash = "actor-hash",
      createdByUserId = createdByUserId,
      tokenIssuedByUserId = issuedByUserId,
    )

    ctx.execute("DELETE FROM \"user\" WHERE id IN (?, ?)", createdByUserId, issuedByUserId)

    val enabledActors =
      ctx.fetchOne(
        """
        SELECT created_by_user_id, token_issued_by_user_id
        FROM scim_configuration
        WHERE id = ?
        """.trimIndent(),
        configurationId,
      )
    assertNotNull(enabledActors)
    assertNull(enabledActors?.get("created_by_user_id"))
    assertNull(enabledActors?.get("token_issued_by_user_id"))

    val disabledOrganizationId = insertOrganization()
    val disabledByUserId = insertUser()
    val disabledConfigurationId = UUID.randomUUID()
    insertDisabledConfiguration(disabledConfigurationId, disabledOrganizationId, disabledByUserId)
    ctx.execute("DELETE FROM \"user\" WHERE id = ?", disabledByUserId)

    assertNull(
      ctx.fetchValue(
        "SELECT disabled_by_user_id FROM scim_configuration WHERE id = ?",
        disabledConfigurationId,
      ),
    )
  }

  @Test
  fun `enforces tenant composite references and USER and GROUP row shapes`() {
    val organizationId = insertOrganization()
    val otherOrganizationId = insertOrganization()
    val configurationId = UUID.randomUUID()
    insertEnabledConfiguration(configurationId, organizationId, "shape-hash")
    val userId = insertUser()
    val groupId = insertGroup(organizationId)
    val otherOrganizationGroupId = insertGroup(otherOrganizationId)

    insertUserMapping(configurationId = configurationId, organizationId = organizationId, userId = userId)
    insertGroupMapping(configurationId = configurationId, organizationId = organizationId, groupId = groupId)

    assertThrows(DataAccessException::class.java) {
      insertUserMapping(
        configurationId = configurationId,
        organizationId = otherOrganizationId,
        userId = insertUser(),
      )
    }
    assertThrows(DataAccessException::class.java) {
      insertGroupMapping(
        configurationId = configurationId,
        organizationId = organizationId,
        groupId = otherOrganizationGroupId,
      )
    }
    assertThrows(DataAccessException::class.java) {
      insertUserMapping(
        configurationId = configurationId,
        organizationId = organizationId,
        userId = insertUser(),
        groupId = groupId,
      )
    }
    assertThrows(DataAccessException::class.java) {
      insertUserMapping(
        configurationId = configurationId,
        organizationId = organizationId,
        userId = insertUser(),
        userName = "   ",
      )
    }
    assertThrows(DataAccessException::class.java) {
      insertUserMapping(
        configurationId = configurationId,
        organizationId = organizationId,
        userId = insertUser(),
        primaryEmail = " ",
      )
    }
    assertThrows(DataAccessException::class.java) {
      insertUserMapping(
        configurationId = configurationId,
        organizationId = organizationId,
        userId = insertUser(),
        primaryEmail = "\t\n",
      )
    }
    assertThrows(DataAccessException::class.java) {
      insertGroupMapping(
        configurationId = configurationId,
        organizationId = organizationId,
        groupId = insertGroup(organizationId),
        userName = "group-must-not-have-user-fields",
      )
    }
    assertThrows(DataAccessException::class.java) {
      insertGroupMapping(
        configurationId = configurationId,
        organizationId = organizationId,
        groupId = insertGroup(organizationId),
        attributes = """{"displayName":"not-stored-here"}""",
      )
    }
    assertThrows(DataAccessException::class.java) {
      insertUserMapping(
        configurationId = configurationId,
        organizationId = organizationId,
        userId = insertUser(),
        attributes = """["not", "an", "object"]""",
      )
    }
    assertThrows(DataAccessException::class.java) {
      insertUserMapping(
        configurationId = configurationId,
        organizationId = organizationId,
        userId = insertUser(),
        attributes = "null",
      )
    }
    assertThrows(DataAccessException::class.java) {
      ctx.execute(
        """
        INSERT INTO scim_resource_mapping (
          id, scim_configuration_id, organization_id, resource_type,
          user_name, primary_email, user_active
        )
        VALUES (?, ?, ?, 'USER', 'missing-user', 'missing-user@example.com', TRUE)
        """.trimIndent(),
        UUID.randomUUID(),
        configurationId,
        organizationId,
      )
    }
    assertThrows(DataAccessException::class.java) {
      ctx.execute(
        """
        INSERT INTO scim_resource_mapping (
          id, scim_configuration_id, organization_id, resource_type, user_id,
          user_name, primary_email, user_active
        )
        VALUES (?, ?, ?, 'WORKSPACE', ?, 'invalid-type', 'invalid-type@example.com', TRUE)
        """.trimIndent(),
        UUID.randomUUID(),
        configurationId,
        organizationId,
        insertUser(),
      )
    }
  }

  @Test
  fun `enforces mapping identifier uniqueness with the required comparison semantics`() {
    val organizationId = insertOrganization()
    val configurationId = UUID.randomUUID()
    insertEnabledConfiguration(configurationId, organizationId, "identifier-hash")

    val firstUserId = insertUser()
    insertUserMapping(
      configurationId = configurationId,
      organizationId = organizationId,
      userId = firstUserId,
      externalId = null,
      userName = "Case.User",
      primaryEmail = "Case.User@example.com",
    )
    insertUserMapping(
      configurationId = configurationId,
      organizationId = organizationId,
      userId = insertUser(),
      externalId = null,
      userName = "another-user",
      primaryEmail = "another-user@example.com",
    )

    assertThrows(DataAccessException::class.java) {
      insertUserMapping(
        configurationId = configurationId,
        organizationId = organizationId,
        userId = insertUser(),
        userName = "case.user",
        primaryEmail = "unique-email@example.com",
      )
    }
    assertThrows(DataAccessException::class.java) {
      insertUserMapping(
        configurationId = configurationId,
        organizationId = organizationId,
        userId = insertUser(),
        userName = "unique-user-name",
        primaryEmail = "case.user@EXAMPLE.COM",
      )
    }
    assertThrows(DataAccessException::class.java) {
      insertUserMapping(
        configurationId = configurationId,
        organizationId = organizationId,
        userId = firstUserId,
        userName = "same-underlying-user",
        primaryEmail = "same-underlying-user@example.com",
      )
    }
    assertThrows(DataAccessException::class.java) {
      insertUserMapping(
        configurationId = configurationId,
        organizationId = organizationId,
        userId = insertUser(),
        externalId = "   ",
        userName = "blank-external-id",
        primaryEmail = "blank-external-id@example.com",
      )
    }
    assertThrows(DataAccessException::class.java) {
      insertUserMapping(
        configurationId = configurationId,
        organizationId = organizationId,
        userId = insertUser(),
        externalId = "\t\n",
        userName = "whitespace-external-id",
        primaryEmail = "whitespace-external-id@example.com",
      )
    }

    insertUserMapping(
      configurationId = configurationId,
      organizationId = organizationId,
      userId = insertUser(),
      externalId = "Case-Exact",
      userName = "case-exact-one",
      primaryEmail = "case-exact-one@example.com",
    )
    insertUserMapping(
      configurationId = configurationId,
      organizationId = organizationId,
      userId = insertUser(),
      externalId = "case-exact",
      userName = "case-exact-two",
      primaryEmail = "case-exact-two@example.com",
    )
    assertThrows(DataAccessException::class.java) {
      insertUserMapping(
        configurationId = configurationId,
        organizationId = organizationId,
        userId = insertUser(),
        externalId = "Case-Exact",
        userName = "duplicate-external-id",
        primaryEmail = "duplicate-external-id@example.com",
      )
    }

    val groupId = insertGroup(organizationId)
    insertGroupMapping(
      configurationId = configurationId,
      organizationId = organizationId,
      groupId = groupId,
      externalId = "Case-Exact",
    )
    assertThrows(DataAccessException::class.java) {
      insertGroupMapping(
        configurationId = configurationId,
        organizationId = organizationId,
        groupId = groupId,
        externalId = "different-group-external-id",
      )
    }

    val otherOrganizationId = insertOrganization()
    val otherConfigurationId = UUID.randomUUID()
    insertEnabledConfiguration(otherConfigurationId, otherOrganizationId, "other-identifier-hash")
    insertUserMapping(
      configurationId = otherConfigurationId,
      organizationId = otherOrganizationId,
      userId = firstUserId,
      externalId = "Case-Exact",
      userName = "Case.User",
      primaryEmail = "Case.User@example.com",
    )
  }

  @Test
  fun `parent deletion cascades only mappings and mapping deletion preserves resources`() {
    val organizationId = insertOrganization()
    val configurationId = UUID.randomUUID()
    insertEnabledConfiguration(configurationId, organizationId, "cascade-hash")
    val userId = insertUser()
    val groupId = insertGroup(organizationId)
    val userMappingId = UUID.randomUUID()
    val groupMappingId = UUID.randomUUID()
    insertUserMapping(userMappingId, configurationId, organizationId, userId)
    insertGroupMapping(groupMappingId, configurationId, organizationId, groupId)

    ctx.execute("DELETE FROM scim_resource_mapping WHERE id IN (?, ?)", userMappingId, groupMappingId)
    assertEquals(1, rowCount("user", userId))
    assertEquals(1, rowCount("group", groupId))

    insertUserMapping(UUID.randomUUID(), configurationId, organizationId, userId)
    insertGroupMapping(UUID.randomUUID(), configurationId, organizationId, groupId)
    ctx.execute("DELETE FROM \"user\" WHERE id = ?", userId)
    assertEquals(0, mappingCount(configurationId, "USER"))
    assertEquals(1, mappingCount(configurationId, "GROUP"))

    ctx.execute("DELETE FROM \"group\" WHERE id = ?", groupId)
    assertEquals(0, mappingCount(configurationId, "GROUP"))

    val preservedUserId = insertUser()
    val preservedGroupId = insertGroup(organizationId)
    insertUserMapping(UUID.randomUUID(), configurationId, organizationId, preservedUserId)
    insertGroupMapping(UUID.randomUUID(), configurationId, organizationId, preservedGroupId)
    ctx.execute("DELETE FROM scim_configuration WHERE id = ?", configurationId)

    assertEquals(0, mappingCount(configurationId))
    assertEquals(1, rowCount("user", preservedUserId))
    assertEquals(1, rowCount("group", preservedGroupId))
  }

  private fun insertOrganization(id: UUID = UUID.randomUUID()): UUID {
    ctx.execute(
      """
      INSERT INTO organization (id, name, email)
      VALUES (?, ?, ?)
      """.trimIndent(),
      id,
      "SCIM test org $id",
      "scim-$id@example.com",
    )
    return id
  }

  private fun insertUser(id: UUID = UUID.randomUUID()): UUID {
    ctx.execute(
      """
      INSERT INTO "user" (id, name, email)
      VALUES (?, ?, ?)
      """.trimIndent(),
      id,
      "SCIM test user $id",
      "scim-$id@example.com",
    )
    return id
  }

  private fun insertGroup(
    organizationId: UUID,
    id: UUID = UUID.randomUUID(),
  ): UUID {
    ctx.execute(
      """
      INSERT INTO "group" (id, name, organization_id)
      VALUES (?, ?, ?)
      """.trimIndent(),
      id,
      "SCIM test group $id",
      organizationId,
    )
    return id
  }

  private fun insertEnabledConfiguration(
    id: UUID,
    organizationId: UUID,
    tokenHash: String,
    createdByUserId: UUID? = null,
    tokenIssuedByUserId: UUID? = null,
  ) {
    ctx.execute(
      """
      INSERT INTO scim_configuration (
        id,
        organization_id,
        token_hash,
        enabled,
        created_by_user_id,
        token_issued_at,
        token_issued_by_user_id
      )
      VALUES (?, ?, ?, TRUE, ?, CURRENT_TIMESTAMP, ?)
      """.trimIndent(),
      id,
      organizationId,
      tokenHash,
      createdByUserId,
      tokenIssuedByUserId,
    )
  }

  private fun insertDisabledConfiguration(
    id: UUID,
    organizationId: UUID,
    disabledByUserId: UUID? = null,
  ) {
    ctx.execute(
      """
      INSERT INTO scim_configuration (id, organization_id, disabled_at, disabled_by_user_id)
      VALUES (?, ?, CURRENT_TIMESTAMP, ?)
      """.trimIndent(),
      id,
      organizationId,
      disabledByUserId,
    )
  }

  private fun insertUserMapping(
    id: UUID = UUID.randomUUID(),
    configurationId: UUID,
    organizationId: UUID,
    userId: UUID,
    groupId: UUID? = null,
    externalId: String? = null,
    userName: String = "user-$id",
    primaryEmail: String = "user-$id@example.com",
    userActive: Boolean = true,
    attributes: String = "{}",
  ) {
    ctx.execute(
      """
      INSERT INTO scim_resource_mapping (
        id,
        scim_configuration_id,
        organization_id,
        resource_type,
        user_id,
        group_id,
        external_id,
        user_name,
        primary_email,
        user_active,
        attributes
      )
      VALUES (?, ?, ?, 'USER', ?, ?, ?, ?, ?, ?, ?::jsonb)
      """.trimIndent(),
      id,
      configurationId,
      organizationId,
      userId,
      groupId,
      externalId,
      userName,
      primaryEmail,
      userActive,
      attributes,
    )
  }

  private fun insertGroupMapping(
    id: UUID = UUID.randomUUID(),
    configurationId: UUID,
    organizationId: UUID,
    groupId: UUID,
    externalId: String? = null,
    userName: String? = null,
    attributes: String = "{}",
  ) {
    ctx.execute(
      """
      INSERT INTO scim_resource_mapping (
        id,
        scim_configuration_id,
        organization_id,
        resource_type,
        group_id,
        external_id,
        user_name,
        attributes
      )
      VALUES (?, ?, ?, 'GROUP', ?, ?, ?, ?::jsonb)
      """.trimIndent(),
      id,
      configurationId,
      organizationId,
      groupId,
      externalId,
      userName,
      attributes,
    )
  }

  private fun tableExists(tableName: String): Boolean =
    ctx.fetchValue(
      """
      SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = ?
      )
      """.trimIndent(),
      tableName,
    ) as Boolean

  private fun enumValues(typeName: String): List<String> =
    ctx
      .fetch(
        """
        SELECT enum_label.enumlabel
        FROM pg_type type_metadata
        JOIN pg_enum enum_label ON enum_label.enumtypid = type_metadata.oid
        WHERE type_metadata.typname = ?
        ORDER BY enum_label.enumsortorder
        """.trimIndent(),
        typeName,
      ).map { it.get("enumlabel") as String }

  private fun assertColumn(
    tableName: String,
    columnName: String,
    dataType: String,
    isNullable: Boolean,
    expectedUdtName: String? = null,
    defaultContains: String? = null,
  ) {
    val column =
      ctx.fetchOne(
        """
        SELECT data_type, is_nullable, udt_name, column_default
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = ?
          AND column_name = ?
        """.trimIndent(),
        tableName,
        columnName,
      )

    assertNotNull(column, "$tableName.$columnName should exist")
    assertEquals(dataType, column?.get("data_type"))
    assertEquals(if (isNullable) "YES" else "NO", column?.get("is_nullable"))
    expectedUdtName?.let { assertEquals(it, column?.get("udt_name")) }
    defaultContains?.let {
      assertTrue(
        column?.get("column_default", String::class.java)?.contains(it) == true,
        "$tableName.$columnName should have a default containing $it",
      )
    }
  }

  private fun assertColumnDoesNotExist(
    tableName: String,
    columnName: String,
  ) {
    val exists =
      ctx.fetchValue(
        """
        SELECT EXISTS (
          SELECT 1
          FROM information_schema.columns
          WHERE table_schema = 'public'
            AND table_name = ?
            AND column_name = ?
        )
        """.trimIndent(),
        tableName,
        columnName,
      )
    assertEquals(false, exists, "$tableName.$columnName should not exist")
  }

  private fun assertUniqueConstraint(
    tableName: String,
    constraintName: String,
    expectedColumns: String,
  ) {
    val columns =
      ctx.fetchValue(
        """
        SELECT string_agg(attribute_metadata.attname, ',' ORDER BY constraint_columns.ordinality)
        FROM pg_constraint constraint_metadata
        JOIN pg_class table_metadata ON table_metadata.oid = constraint_metadata.conrelid
        JOIN unnest(constraint_metadata.conkey) WITH ORDINALITY AS constraint_columns(attnum, ordinality) ON TRUE
        JOIN pg_attribute attribute_metadata
          ON attribute_metadata.attrelid = table_metadata.oid
          AND attribute_metadata.attnum = constraint_columns.attnum
        WHERE table_metadata.relname = ?
          AND constraint_metadata.conname = ?
          AND constraint_metadata.contype = 'u'
        """.trimIndent(),
        tableName,
        constraintName,
      )
    assertEquals(expectedColumns, columns, "$tableName.$constraintName should have the expected columns")
  }

  private fun assertForeignKey(
    tableName: String,
    constraintName: String,
    expectedColumns: String,
    expectedReferencedTable: String,
    expectedReferencedColumns: String,
    expectedDeleteAction: String,
  ) {
    val foreignKey =
      ctx.fetchOne(
        """
        SELECT
          string_agg(column_attribute.attname, ',' ORDER BY foreign_key_columns.ordinality) AS columns,
          referenced_table.relname AS referenced_table,
          string_agg(referenced_attribute.attname, ',' ORDER BY foreign_key_columns.ordinality) AS referenced_columns,
          constraint_metadata.confdeltype::text AS delete_action
        FROM pg_constraint constraint_metadata
        JOIN pg_class table_metadata ON table_metadata.oid = constraint_metadata.conrelid
        JOIN pg_class referenced_table ON referenced_table.oid = constraint_metadata.confrelid
        JOIN unnest(constraint_metadata.conkey, constraint_metadata.confkey)
          WITH ORDINALITY AS foreign_key_columns(column_attnum, referenced_attnum, ordinality) ON TRUE
        JOIN pg_attribute column_attribute
          ON column_attribute.attrelid = table_metadata.oid
          AND column_attribute.attnum = foreign_key_columns.column_attnum
        JOIN pg_attribute referenced_attribute
          ON referenced_attribute.attrelid = referenced_table.oid
          AND referenced_attribute.attnum = foreign_key_columns.referenced_attnum
        WHERE table_metadata.relname = ?
          AND constraint_metadata.conname = ?
          AND constraint_metadata.contype = 'f'
        GROUP BY referenced_table.relname, constraint_metadata.confdeltype
        """.trimIndent(),
        tableName,
        constraintName,
      )

    assertNotNull(foreignKey, "$tableName.$constraintName should exist")
    assertEquals(expectedColumns, foreignKey?.get("columns"))
    assertEquals(expectedReferencedTable, foreignKey?.get("referenced_table"))
    assertEquals(expectedReferencedColumns, foreignKey?.get("referenced_columns"))
    assertEquals(expectedDeleteAction, foreignKey?.get("delete_action"))
  }

  private fun assertCheckConstraintContains(
    tableName: String,
    constraintName: String,
    vararg expectedParts: String,
  ) {
    val constraintDefinition =
      ctx.fetchValue(
        """
        SELECT pg_get_constraintdef(constraint_metadata.oid)
        FROM pg_constraint constraint_metadata
        JOIN pg_class table_metadata ON table_metadata.oid = constraint_metadata.conrelid
        WHERE table_metadata.relname = ?
          AND constraint_metadata.conname = ?
          AND constraint_metadata.contype = 'c'
        """.trimIndent(),
        tableName,
        constraintName,
      ) as String?
    assertNotNull(constraintDefinition, "$tableName.$constraintName should exist")
    expectedParts.forEach {
      assertTrue(
        constraintDefinition?.contains(it) == true,
        "$tableName.$constraintName should contain $it but was $constraintDefinition",
      )
    }
  }

  private fun assertIndexContains(
    indexName: String,
    vararg expectedParts: String,
  ) {
    val indexDefinition =
      ctx.fetchValue(
        """
        SELECT indexdef
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND indexname = ?
        """.trimIndent(),
        indexName,
      ) as String?
    assertNotNull(indexDefinition, "$indexName should exist")
    expectedParts.forEach {
      assertTrue(indexDefinition?.contains(it) == true, "$indexName should contain $it but was $indexDefinition")
    }
  }

  private fun mappingCount(
    configurationId: UUID,
    resourceType: String? = null,
  ): Int =
    if (resourceType == null) {
      (
        ctx.fetchValue(
          "SELECT count(*) FROM scim_resource_mapping WHERE scim_configuration_id = ?",
          configurationId,
        ) as Number
      ).toInt()
    } else {
      (
        ctx.fetchValue(
          """
          SELECT count(*)
          FROM scim_resource_mapping
          WHERE scim_configuration_id = ?
            AND resource_type = ?::scim_resource_type
          """.trimIndent(),
          configurationId,
          resourceType,
        ) as Number
      ).toInt()
    }

  private fun rowCount(
    tableName: String,
    id: UUID,
  ): Int = (ctx.fetchValue("SELECT count(*) FROM \"$tableName\" WHERE id = ?", id) as Number).toInt()
}
