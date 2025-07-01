/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.airbyte.db.instance.configs.AbstractConfigsDatabaseTest
import io.airbyte.db.instance.configs.migrations.V0_50_5_001__CreateOrganizationTable.Companion.createOrganization
import org.jooq.exception.DataAccessException
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.io.IOException
import java.sql.SQLException
import java.util.UUID

@Suppress("ktlint:standard:class-naming")
@Disabled
internal class V0_50_5_001__CreateOrganizationTableTest : AbstractConfigsDatabaseTest() {
  @Test
  @Throws(SQLException::class, IOException::class)
  fun test() {
    val context = dslContext!!
    createOrganization(context)

    val organizationId = UUID(0L, 1L)
    val userId = UUID(0L, 1L)

    // assert can insert
    Assertions.assertDoesNotThrow {
      context
        .insertInto(DSL.table(ORGANIZATION))
        .columns(
          DSL.field(ID),
          DSL.field(NAME),
          DSL.field(USER_ID),
          DSL.field(EMAIL),
        ).values(
          organizationId,
          NAME,
          userId,
          EMAIL,
        ).execute()
    }

    // assert primary key is unique
    val e: Exception =
      Assertions.assertThrows(
        DataAccessException::class.java,
      ) {
        context
          .insertInto(DSL.table(ORGANIZATION))
          .columns(
            DSL.field(ID),
            DSL.field(NAME),
            DSL.field(USER_ID),
            DSL.field(EMAIL),
          ).values(
            organizationId,
            NAME,
            userId,
            EMAIL,
          ).execute()
      }
    Assertions.assertTrue(e.message!!.contains("duplicate key value violates unique constraint \"organization_pkey\""))
  }

  companion object {
    private const val ORGANIZATION = "organization"
    private const val ID = "id"
    private const val USER_ID = "user_id"
    private const val NAME = "name"
    private const val EMAIL = "email"
  }
}
