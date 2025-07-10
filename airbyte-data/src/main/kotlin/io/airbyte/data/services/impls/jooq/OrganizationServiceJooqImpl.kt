/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq

import io.airbyte.config.Organization
import io.airbyte.data.services.OrganizationService
import io.airbyte.db.Database
import io.airbyte.db.ExceptionWrappingDatabase
import io.airbyte.db.instance.configs.jooq.generated.Tables
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.Result
import org.jooq.impl.DSL
import java.io.IOException
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID

/**
 * Deprecated - use OrganizationServiceDataImpl instead. This class is not being deleted right now
 * because it is used in test database setup code that currently requires jooq. Once that code is
 * refactored to use Micronaut Data configuration instead, this class can be deleted.
 */
@Deprecated("")
@Singleton
class OrganizationServiceJooqImpl(
  @Named("configDatabase") database: Database?,
) : OrganizationService {
  private val database = ExceptionWrappingDatabase(database)

  /**
   * Get organization.
   *
   * @param organizationId id to use to find the organization
   * @return organization, if present.
   * @throws IOException - you never know when you IO
   */
  @Throws(IOException::class)
  override fun getOrganization(organizationId: UUID): Optional<Organization> {
    val result: Result<Record> =
      database
        .query<org.jooq.SelectConditionStep<Record>>({ ctx: DSLContext ->
          ctx
            .select(Tables.ORGANIZATION.asterisk())
            .from(Tables.ORGANIZATION)
            .where(Tables.ORGANIZATION.ID.eq(organizationId))
        })
        .fetch()

    return result.stream().findFirst().map { record: Record -> DbConverter.buildOrganization(record) }
  }

  @Throws(IOException::class)
  override fun getOrganizationForWorkspaceId(workspaceId: UUID): Optional<Organization> {
    val result =
      database
        .query { ctx: DSLContext ->
          ctx
            .select(Tables.ORGANIZATION.asterisk())
            .from(Tables.ORGANIZATION)
            .innerJoin(Tables.WORKSPACE)
            .on(Tables.ORGANIZATION.ID.eq(Tables.WORKSPACE.ORGANIZATION_ID))
            .where(Tables.WORKSPACE.ID.eq(workspaceId))
        }.fetch()
    return result.stream().findFirst().map { record: Record -> DbConverter.buildOrganization(record) }
  }

  /**
   * Write an Organization to the database.
   *
   * @param organization - The configuration of the organization
   * @throws IOException - you never know when you IO
   */
  @Throws(IOException::class)
  override fun writeOrganization(organization: Organization) {
    database.transaction<Any?> { ctx: DSLContext ->
      val timestamp = OffsetDateTime.now()
      val isExistingConfig =
        ctx.fetchExists(
          DSL
            .select()
            .from(Tables.ORGANIZATION)
            .where(Tables.ORGANIZATION.ID.eq(organization.organizationId)),
        )

      if (isExistingConfig) {
        ctx
          .update(Tables.ORGANIZATION)
          .set(Tables.ORGANIZATION.ID, organization.organizationId)
          .set(Tables.ORGANIZATION.NAME, organization.name)
          .set(Tables.ORGANIZATION.EMAIL, organization.email)
          .set(Tables.ORGANIZATION.USER_ID, organization.userId)
          .set(Tables.ORGANIZATION.UPDATED_AT, timestamp)
          .where(Tables.ORGANIZATION.ID.eq(organization.organizationId))
          .execute()
      } else {
        ctx
          .insertInto(Tables.ORGANIZATION)
          .set(Tables.ORGANIZATION.ID, organization.organizationId)
          .set(Tables.ORGANIZATION.NAME, organization.name)
          .set(Tables.ORGANIZATION.EMAIL, organization.email)
          .set(Tables.ORGANIZATION.USER_ID, organization.userId)
          .set(Tables.WORKSPACE.CREATED_AT, timestamp)
          .set(Tables.WORKSPACE.UPDATED_AT, timestamp)
          .execute()
      }
      null
    }
  }
}
