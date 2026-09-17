/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq

import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.test.utils.BaseConfigDatabaseTest
import org.jooq.DSLContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

internal class OrganizationServiceJooqImplTest : BaseConfigDatabaseTest() {
  private lateinit var organizationService: OrganizationServiceJooqImpl

  @BeforeEach
  fun setUp() {
    truncateAllTables()
    organizationService = OrganizationServiceJooqImpl(database)
  }

  @Test
  fun `getActiveOrganization returns an active organization`() {
    val organizationId = insertOrganization(tombstone = false)

    val result = organizationService.getActiveOrganization(organizationId)

    assertTrue(result.isPresent)
    assertEquals(organizationId, result.get().organizationId)
  }

  @Test
  fun `getActiveOrganization returns empty for a tombstoned organization without changing getOrganization`() {
    val organizationId = insertOrganization(tombstone = true)

    val activeResult = organizationService.getActiveOrganization(organizationId)

    assertTrue(activeResult.isEmpty)
    assertTrue(organizationService.getOrganization(organizationId).isPresent)
  }

  @Test
  fun `getActiveOrganization returns empty when the organization does not exist`() {
    val result = organizationService.getActiveOrganization(UUID.randomUUID())

    assertTrue(result.isEmpty)
  }

  private fun insertOrganization(tombstone: Boolean): UUID {
    val organizationId = UUID.randomUUID()
    database!!.query<Int> { ctx: DSLContext ->
      ctx
        .insertInto(Tables.ORGANIZATION)
        .set(Tables.ORGANIZATION.ID, organizationId)
        .set(Tables.ORGANIZATION.NAME, "organization-$organizationId")
        .set(Tables.ORGANIZATION.EMAIL, "$organizationId@example.com")
        .set(Tables.ORGANIZATION.TOMBSTONE, tombstone)
        .execute()
    }
    return organizationId
  }
}
