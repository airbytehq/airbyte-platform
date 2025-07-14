/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import io.airbyte.commons.DEFAULT_ORGANIZATION_ID
import io.airbyte.config.Organization
import io.airbyte.config.SsoConfig
import io.airbyte.data.services.shared.ResourcesByUserQueryPaginated
import io.airbyte.db.Database
import io.airbyte.db.ExceptionWrappingDatabase
import io.airbyte.db.instance.configs.jooq.generated.Tables
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.impl.DSL
import java.io.IOException
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID

/**
 * Permission Persistence.
 *
 *
 * Handle persisting Permission to the Config Database and perform all SQL queries.
 */
class OrganizationPersistence(
  database: Database?,
) {
  private val database = ExceptionWrappingDatabase(database)

  /**
   * Retrieve an organization.
   *
   * @param organizationId to fetch
   * @return fetched organization
   * @throws IOException when interaction with DB failed
   */
  @Throws(IOException::class)
  fun getOrganization(organizationId: UUID?): Optional<Organization> {
    val result =
      database.query { ctx: DSLContext ->
        ctx
          .select(DSL.asterisk())
          .from(Tables.ORGANIZATION)
          .leftJoin(Tables.SSO_CONFIG)
          .on(Tables.ORGANIZATION.ID.eq(Tables.SSO_CONFIG.ORGANIZATION_ID))
          .where(Tables.ORGANIZATION.ID.eq(organizationId))
          .fetch()
      }

    if (result.isEmpty()) {
      return Optional.empty()
    }

    return Optional.of(createOrganizationFromRecord(result[0]))
  }

  @Throws(IOException::class)
  fun getOrganizationByWorkspaceId(workspaceId: UUID?): Optional<Organization> {
    val result =
      database.query { ctx: DSLContext ->
        ctx
          .select(DSL.asterisk())
          .from(Tables.ORGANIZATION)
          .leftJoin(Tables.SSO_CONFIG)
          .on(Tables.ORGANIZATION.ID.eq(Tables.SSO_CONFIG.ORGANIZATION_ID))
          .join(Tables.WORKSPACE)
          .on(Tables.ORGANIZATION.ID.eq(Tables.WORKSPACE.ORGANIZATION_ID))
          .where(Tables.WORKSPACE.ID.eq(workspaceId))
          .fetch()
      }

    if (result.isEmpty()) {
      return Optional.empty()
    }

    return Optional.of(createOrganizationFromRecord(result[0]))
  }

  @Throws(IOException::class)
  fun getOrganizationByConnectionId(connectionId: UUID?): Optional<Organization> {
    val result =
      database.query { ctx: DSLContext ->
        ctx
          .select(DSL.asterisk())
          .from(Tables.ORGANIZATION)
          .leftJoin(Tables.SSO_CONFIG)
          .on(Tables.ORGANIZATION.ID.eq(Tables.SSO_CONFIG.ORGANIZATION_ID))
          .join(Tables.WORKSPACE)
          .on(Tables.ORGANIZATION.ID.eq(Tables.WORKSPACE.ORGANIZATION_ID))
          .join(Tables.ACTOR)
          .on(Tables.ACTOR.WORKSPACE_ID.eq(Tables.WORKSPACE.ID))
          .join(Tables.CONNECTION)
          .on(Tables.CONNECTION.SOURCE_ID.eq(Tables.ACTOR.ID))
          .where(Tables.CONNECTION.ID.eq(connectionId))
          .fetch()
      }

    if (result.isEmpty()) {
      return Optional.empty()
    }

    return Optional.of(createOrganizationFromRecord(result[0]))
  }

  /**
   * Create a new organization and insert into the database.
   *
   * @param organization to be created
   * @return created organization
   * @throws IOException when interaction with DB failed
   */
  @Throws(IOException::class)
  fun createOrganization(organization: Organization): Organization {
    database.transaction<Any?> { ctx: DSLContext ->
      try {
        insertOrganizationIntoDB(ctx, organization)
      } catch (e: IOException) {
        throw RuntimeException(e)
      }
      null
    }
    return organization
  }

  /**
   * Update an existing organization.
   *
   * @param organization - proposed organization to update
   * @return updated organization
   * @throws IOException - when interaction with DB failed
   */
  @Throws(IOException::class)
  fun updateOrganization(organization: Organization): Organization {
    database.transaction<Any?> { ctx: DSLContext ->
      try {
        updateOrganizationInDB(ctx, organization)
      } catch (e: IOException) {
        throw RuntimeException(e)
      }
      null
    }
    return organization
  }

  @get:Throws(IOException::class)
  val defaultOrganization: Optional<Organization>
    /**
     * Get the default organization if it exists by looking up the hardcoded default organization id.
     */
    get() = getOrganization(DEFAULT_ORGANIZATION_ID)

  /**
   * List all organizations by user id, returning result ordered by org name. Supports keyword search.
   */
  @Throws(IOException::class)
  fun listOrganizationsByUserId(
    userId: UUID?,
    keyword: Optional<String>,
  ): List<Organization> =
    database
      .query { ctx: DSLContext ->
        ctx
          .select(
            Tables.ORGANIZATION.asterisk(),
            Tables.SSO_CONFIG.asterisk(),
          ).from(Tables.ORGANIZATION)
          .join(Tables.PERMISSION)
          .on(Tables.ORGANIZATION.ID.eq(Tables.PERMISSION.ORGANIZATION_ID))
          .leftJoin(Tables.SSO_CONFIG)
          .on(Tables.ORGANIZATION.ID.eq(Tables.SSO_CONFIG.ORGANIZATION_ID))
          .where(Tables.PERMISSION.USER_ID.eq(userId))
          .and(Tables.PERMISSION.ORGANIZATION_ID.isNotNull())
          .and(if (keyword.isPresent) Tables.ORGANIZATION.NAME.containsIgnoreCase(keyword.get()) else DSL.noCondition())
          .orderBy(Tables.ORGANIZATION.NAME.asc())
          .fetch()
      }.stream()
      .map { record: Record -> createOrganizationFromRecord(record) }
      .toList()

  /**
   * List all organizations by user id, returning result ordered by org name. Supports pagination and
   * keyword search.
   */
  @Throws(IOException::class)
  fun listOrganizationsByUserIdPaginated(
    query: ResourcesByUserQueryPaginated,
    keyword: Optional<String>,
  ): List<Organization> =
    database
      .query { ctx: DSLContext ->
        ctx
          .select(
            Tables.ORGANIZATION.asterisk(),
            Tables.SSO_CONFIG.asterisk(),
          ).from(Tables.ORGANIZATION)
          .join(Tables.PERMISSION)
          .on(Tables.ORGANIZATION.ID.eq(Tables.PERMISSION.ORGANIZATION_ID))
          .leftJoin(Tables.SSO_CONFIG)
          .on(Tables.ORGANIZATION.ID.eq(Tables.SSO_CONFIG.ORGANIZATION_ID))
          .where(Tables.PERMISSION.USER_ID.eq(query.userId))
          .and(Tables.PERMISSION.ORGANIZATION_ID.isNotNull())
          .and(if (keyword.isPresent) Tables.ORGANIZATION.NAME.containsIgnoreCase(keyword.get()) else DSL.noCondition())
          .orderBy(Tables.ORGANIZATION.NAME.asc())
          .limit(query.pageSize)
          .offset(query.rowOffset)
          .fetch()
      }.stream()
      .map { record: Record -> createOrganizationFromRecord(record) }
      .toList()

  /**
   * Get the matching organization that has the given sso config realm. If not exists, returns empty
   * optional obejct.
   */
  @Throws(IOException::class)
  fun getOrganizationBySsoConfigRealm(ssoConfigRealm: String?): Optional<Organization> {
    val result =
      database.query { ctx: DSLContext ->
        ctx
          .select(DSL.asterisk())
          .from(Tables.ORGANIZATION)
          .join(Tables.SSO_CONFIG)
          .on(Tables.ORGANIZATION.ID.eq(Tables.SSO_CONFIG.ORGANIZATION_ID))
          .where(Tables.SSO_CONFIG.KEYCLOAK_REALM.eq(ssoConfigRealm))
          .fetch()
      }

    if (result.isEmpty()) {
      return Optional.empty()
    }

    return Optional.of(createOrganizationFromRecord(result[0]))
  }

  @Throws(IOException::class)
  fun createSsoConfig(ssoConfig: SsoConfig): SsoConfig {
    database.transaction<Any?> { ctx: DSLContext ->
      try {
        insertSsoConfigIntoDB(ctx, ssoConfig)
      } catch (e: IOException) {
        throw RuntimeException(e)
      }
      null
    }
    return ssoConfig
  }

  @Throws(IOException::class)
  fun updateSsoConfig(ssoConfig: SsoConfig): SsoConfig {
    database.transaction<Any?> { ctx: DSLContext ->
      try {
        updateSsoConfigInDB(ctx, ssoConfig)
      } catch (e: IOException) {
        throw RuntimeException(e)
      }
      null
    }
    return ssoConfig
  }

  @Throws(IOException::class)
  fun getSsoConfigForOrganization(organizationId: UUID?): Optional<SsoConfig> {
    val result =
      database.query { ctx: DSLContext ->
        ctx
          .select(DSL.asterisk())
          .from(Tables.SSO_CONFIG)
          .where(Tables.SSO_CONFIG.ORGANIZATION_ID.eq(organizationId))
          .fetch()
      }

    if (result.isEmpty()) {
      return Optional.empty()
    }

    return Optional.of(createSsoConfigFromRecord(result[0]))
  }

  @Throws(IOException::class)
  fun getSsoConfigByRealmName(realmName: String?): Optional<SsoConfig> {
    val result =
      database.query { ctx: DSLContext ->
        ctx
          .select(DSL.asterisk())
          .from(Tables.SSO_CONFIG)
          .where(Tables.SSO_CONFIG.KEYCLOAK_REALM.eq(realmName))
          .fetch()
      }

    if (result.isEmpty()) {
      return Optional.empty()
    }

    return Optional.of(createSsoConfigFromRecord(result[0]))
  }

  @Throws(IOException::class)
  private fun updateOrganizationInDB(
    ctx: DSLContext,
    organization: Organization,
  ) {
    val timestamp = OffsetDateTime.now()

    val isExistingConfig =
      ctx.fetchExists(
        DSL
          .select()
          .from(Tables.ORGANIZATION)
          .where(Tables.ORGANIZATION.ID.eq(organization.organizationId)),
      )

    if (!isExistingConfig) {
      throw IOException("Organization with id " + organization.organizationId + " does not exist.")
    }
    ctx
      .update(Tables.ORGANIZATION)
      .set(Tables.ORGANIZATION.NAME, organization.name)
      .set(Tables.ORGANIZATION.EMAIL, organization.email)
      .set(Tables.ORGANIZATION.USER_ID, organization.userId)
      .set(Tables.ORGANIZATION.UPDATED_AT, timestamp)
      .where(Tables.ORGANIZATION.ID.eq(organization.organizationId))
      .execute()
  }

  @Throws(IOException::class)
  private fun insertOrganizationIntoDB(
    ctx: DSLContext,
    organization: Organization,
  ) {
    val timestamp = OffsetDateTime.now()

    val isExistingConfig =
      ctx.fetchExists(
        DSL
          .select()
          .from(Tables.ORGANIZATION)
          .where(Tables.ORGANIZATION.ID.eq(organization.organizationId)),
      )

    if (isExistingConfig) {
      throw IOException("Organization with id " + organization.organizationId + " already exists.")
    }
    ctx
      .insertInto(Tables.ORGANIZATION)
      .set(Tables.ORGANIZATION.ID, organization.organizationId)
      .set(Tables.ORGANIZATION.USER_ID, organization.userId)
      .set(Tables.ORGANIZATION.NAME, organization.name)
      .set(Tables.ORGANIZATION.EMAIL, organization.email)
      .set(Tables.ORGANIZATION.CREATED_AT, timestamp)
      .set(Tables.ORGANIZATION.UPDATED_AT, timestamp)
      .execute()
  }

  @Throws(IOException::class)
  private fun insertSsoConfigIntoDB(
    ctx: DSLContext,
    ssoConfig: SsoConfig,
  ) {
    val timestamp = OffsetDateTime.now()

    val isExistingConfig =
      ctx.fetchExists(
        DSL
          .select()
          .from(Tables.SSO_CONFIG)
          .where(Tables.SSO_CONFIG.ORGANIZATION_ID.eq(ssoConfig.organizationId)),
      )

    if (isExistingConfig) {
      throw IOException("SsoConfig with organization id " + ssoConfig.organizationId + " already exists.")
    }
    ctx
      .insertInto(Tables.SSO_CONFIG)
      .set(Tables.SSO_CONFIG.ID, ssoConfig.ssoConfigId)
      .set(Tables.SSO_CONFIG.ORGANIZATION_ID, ssoConfig.organizationId)
      .set(Tables.SSO_CONFIG.KEYCLOAK_REALM, ssoConfig.keycloakRealm)
      .set(Tables.SSO_CONFIG.CREATED_AT, timestamp)
      .set(Tables.SSO_CONFIG.UPDATED_AT, timestamp)
      .execute()
  }

  @Throws(IOException::class)
  private fun updateSsoConfigInDB(
    ctx: DSLContext,
    ssoConfig: SsoConfig,
  ) {
    val timestamp = OffsetDateTime.now()
    ctx
      .update(Tables.SSO_CONFIG)
      .set(Tables.SSO_CONFIG.ORGANIZATION_ID, ssoConfig.organizationId)
      .set(Tables.SSO_CONFIG.KEYCLOAK_REALM, ssoConfig.keycloakRealm)
      .set(Tables.SSO_CONFIG.UPDATED_AT, timestamp)
      .where(Tables.SSO_CONFIG.ID.eq(ssoConfig.ssoConfigId))
      .execute()
  }

  companion object {
    private fun createOrganizationFromRecord(record: Record): Organization =
      Organization()
        .withOrganizationId(record.get(Tables.ORGANIZATION.ID))
        .withName(record.get(Tables.ORGANIZATION.NAME))
        .withEmail(record.get(Tables.ORGANIZATION.EMAIL))
        .withUserId(record.get(Tables.ORGANIZATION.USER_ID))
        .withSsoRealm(record.get(Tables.SSO_CONFIG.KEYCLOAK_REALM))

    private fun createSsoConfigFromRecord(record: Record): SsoConfig =
      SsoConfig()
        .withSsoConfigId(record.get(Tables.SSO_CONFIG.ID))
        .withOrganizationId(record.get(Tables.SSO_CONFIG.ORGANIZATION_ID))
        .withKeycloakRealm(record.get(Tables.SSO_CONFIG.KEYCLOAK_REALM))
  }
}
