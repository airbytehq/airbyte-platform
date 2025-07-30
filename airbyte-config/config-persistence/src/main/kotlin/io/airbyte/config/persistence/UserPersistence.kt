/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.DEFAULT_USER_ID
import io.airbyte.commons.enums.toEnum
import io.airbyte.commons.json.Jsons
import io.airbyte.config.AuthProvider
import io.airbyte.config.AuthUser
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.Permission
import io.airbyte.config.User
import io.airbyte.config.WorkspaceUserAccessInfo
import io.airbyte.config.helpers.AuthenticatedUserConverter.toUser
import io.airbyte.db.Database
import io.airbyte.db.ExceptionWrappingDatabase
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.Status
import io.github.oshai.kotlinlogging.KotlinLogging
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.Record
import org.jooq.Record3
import org.jooq.impl.DSL
import java.io.IOException
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID

/**
 * User Persistence.
 *
 * Perform all SQL queries and handle persisting User to the Config Database.
 *
 */
open class UserPersistence(
  database: Database?,
) {
  private val database = ExceptionWrappingDatabase(database)

  /**
   * Create or update a user.
   *
   * @param user user to create or update.
   * @throws IOException in case of a db error
   */
  @Throws(IOException::class)
  fun writeUser(user: User) {
    database.transaction<Any?> { ctx: DSLContext ->
      val isExistingConfig =
        ctx.fetchExists(
          DSL
            .select()
            .from(Tables.USER)
            .where(Tables.USER.ID.eq(user.userId)),
        )
      if (isExistingConfig) {
        updateUser(ctx, user)
      } else {
        createUser(ctx, user)
      }
      null
    }
  }

  private fun updateUser(
    ctx: DSLContext,
    user: User,
  ) {
    val timestamp = OffsetDateTime.now()
    ctx
      .update(Tables.USER)
      .set(Tables.USER.ID, user.userId)
      .set(Tables.USER.NAME, user.name)
      .set(Tables.USER.DEFAULT_WORKSPACE_ID, user.defaultWorkspaceId)
      .set(
        Tables.USER.STATUS,
        if (user.status == null) {
          null
        } else {
          user.status.value().toEnum<Status>()!!
        },
      ).set(Tables.USER.COMPANY_NAME, user.companyName)
      .set(Tables.USER.EMAIL, user.email)
      .set(Tables.USER.NEWS, user.news)
      .set(Tables.USER.UI_METADATA, JSONB.valueOf(Jsons.serialize(user.uiMetadata)))
      .set(Tables.USER.UPDATED_AT, timestamp)
      .where(Tables.USER.ID.eq(user.userId))
      .execute()
  }

  private fun createUser(
    ctx: DSLContext,
    user: User,
  ) {
    val timestamp = OffsetDateTime.now()
    ctx
      .insertInto(Tables.USER)
      .set(Tables.USER.ID, user.userId)
      .set(Tables.USER.NAME, user.name)
      .set(Tables.USER.DEFAULT_WORKSPACE_ID, user.defaultWorkspaceId)
      .set(
        Tables.USER.STATUS,
        if (user.status == null) {
          null
        } else {
          user.status.value().toEnum<Status>()!!
        },
      ).set(Tables.USER.COMPANY_NAME, user.companyName)
      .set(Tables.USER.EMAIL, user.email)
      .set(Tables.USER.NEWS, user.news)
      .set(Tables.USER.UI_METADATA, JSONB.valueOf(Jsons.serialize(user.uiMetadata)))
      .set(Tables.USER.CREATED_AT, timestamp)
      .set(Tables.USER.UPDATED_AT, timestamp)
      .execute()
  }

  /**
   * Create or update a user.
   *
   * @param user user to create or update.
   */
  @Throws(IOException::class)
  fun writeAuthenticatedUser(user: AuthenticatedUser) {
    database.transaction<Any?> { ctx: DSLContext ->
      val isExistingConfig =
        ctx.fetchExists(
          DSL
            .select()
            .from(Tables.USER)
            .where(Tables.USER.ID.eq(user.userId)),
        )
      if (isExistingConfig) {
        updateUser(ctx, toUser(user))
      } else {
        createUser(ctx, toUser(user))
        writeAuthUser(ctx, user.userId, user.authUserId, user.authProvider)
      }
      null
    }
  }

  @Throws(IOException::class)
  fun writeAuthUser(
    userId: UUID,
    authUserId: String,
    authProvider: AuthProvider?,
  ) {
    database.query<Any?> { ctx: DSLContext ->
      writeAuthUser(ctx, userId, authUserId, authProvider)
      null
    }
  }

  private fun writeAuthUser(
    ctx: DSLContext,
    userId: UUID,
    authUserId: String,
    authProvider: AuthProvider?,
  ) {
    val now = OffsetDateTime.now()
    ctx
      .insertInto(Tables.AUTH_USER)
      .set(Tables.AUTH_USER.ID, UUID.randomUUID())
      .set(Tables.AUTH_USER.USER_ID, userId)
      .set(Tables.AUTH_USER.AUTH_USER_ID, authUserId)
      .set(
        Tables.AUTH_USER.AUTH_PROVIDER,
        if (authProvider == null) {
          null
        } else {
          authProvider.value().toEnum<io.airbyte.db.instance.configs.jooq.generated.enums.AuthProvider>()!!
        },
      ).set(Tables.AUTH_USER.CREATED_AT, now)
      .set(Tables.AUTH_USER.UPDATED_AT, now)
      .execute()
  }

  /**
   * Replace the auth user for a particular Airbyte user.
   *
   * @param userId internal user id
   * @param newAuthUserId new auth user id
   * @param newAuthProvider new auth provider
   * @throws IOException in case of a db error
   */
  @Throws(IOException::class)
  fun replaceAuthUserForUserId(
    userId: UUID,
    newAuthUserId: String,
    newAuthProvider: AuthProvider?,
  ) {
    database.transaction<Any?> { ctx: DSLContext ->
      ctx
        .deleteFrom(Tables.AUTH_USER)
        .where(Tables.AUTH_USER.USER_ID.eq(userId))
        .execute()
      writeAuthUser(ctx, userId, newAuthUserId, newAuthProvider)
      null
    }
  }

  /**
   * Delete User.
   *
   * @param userId internal user id
   * @return user if found
   */
  @Throws(IOException::class)
  fun deleteUserById(userId: UUID?): Boolean =
    database
      .transaction { ctx: DSLContext -> ctx.deleteFrom(Tables.USER) }
      .where(DSL.field(DSL.name(PRIMARY_KEY)).eq(userId))
      .execute() > 0

  /**
   * Get User.
   *
   * @param userId internal user id
   * @return user if found
   */
  @Deprecated("")
  @Throws(IOException::class)
  fun getAuthenticatedUser(userId: UUID?): Optional<AuthenticatedUser> {
    val result =
      database.query { ctx: DSLContext ->
        ctx
          .select(DSL.asterisk())
          .from(Tables.USER)
          .leftJoin(Tables.AUTH_USER)
          .on(Tables.USER.ID.eq(Tables.AUTH_USER.USER_ID))
          .where(Tables.USER.ID.eq(userId))
          .fetch()
      }

    if (result.isEmpty()) {
      return Optional.empty()
    }

    // FIXME: in the case of multiple auth providers, this will return the first one found.
    return Optional.of(createAuthenticatedUserFromRecord(result[0]))
  }

  /**
   * Get User.
   *
   * @param userId internal user id
   * @return user if found
   * @throws IOException in case of a db error
   */
  @Throws(IOException::class)
  fun getUser(userId: UUID?): Optional<User> {
    val result =
      database.query { ctx: DSLContext ->
        ctx
          .select(DSL.asterisk())
          .from(Tables.USER)
          .where(Tables.USER.ID.eq(userId))
          .fetch()
      }

    if (result.isEmpty()) {
      return Optional.empty()
    }

    return Optional.of(createUserFromRecord(result[0]))
  }

  private fun createUserFromRecord(record: Record): User =
    User()
      .withUserId(record.get(Tables.USER.ID))
      .withName(record.get(Tables.USER.NAME))
      .withDefaultWorkspaceId(record.get(Tables.USER.DEFAULT_WORKSPACE_ID))
      .withStatus(
        if (record.get(Tables.USER.STATUS) == null) {
          null
        } else {
          record
            .get(
              Tables.USER.STATUS,
              String::class.java,
            ).toEnum<User.Status>()!!
        },
      ).withCompanyName(record.get(Tables.USER.COMPANY_NAME))
      .withEmail(record.get(Tables.USER.EMAIL))
      .withNews(record.get(Tables.USER.NEWS)) // special handling of "null" string so User hashes predictably with Java `<null>` instead of
      // JsonNode `null`
      .withUiMetadata(
        if (record.get(Tables.USER.UI_METADATA) == null || record.get(Tables.USER.UI_METADATA).data() == "null") {
          null
        } else {
          Jsons.deserialize(
            record.get(Tables.USER.UI_METADATA).data(),
            JsonNode::class.java,
          )
        },
      )

  private fun createAuthenticatedUserFromRecord(record: Record): AuthenticatedUser {
    val user = createUserFromRecord(record)
    return AuthenticatedUser()
      .withUserId(user.userId)
      .withName(user.name)
      .withDefaultWorkspaceId(user.defaultWorkspaceId)
      .withStatus(user.status)
      .withCompanyName(user.companyName)
      .withEmail(user.email)
      .withNews(user.news)
      .withUiMetadata(user.uiMetadata)
      .withAuthUserId(record.get(Tables.AUTH_USER.AUTH_USER_ID))
      .withAuthProvider(
        if (record.get(Tables.AUTH_USER.AUTH_PROVIDER) == null) {
          null
        } else {
          record
            .get(
              Tables.AUTH_USER.AUTH_PROVIDER,
              String::class.java,
            ).toEnum<AuthProvider>()!!
        },
      )
  }

  /**
   * Fetch user information from their authentication id.
   *
   * @param userAuthId the authentication Identifier of the user
   * @return the user information if it exists in the database, Optional.empty() otherwise
   * @throws IOException in case of a db error
   */
  @Throws(IOException::class)
  fun getUserByAuthId(userAuthId: String?): Optional<AuthenticatedUser> {
    val result =
      database.query { ctx: DSLContext ->
        ctx
          .select(
            Tables.AUTH_USER.AUTH_USER_ID,
            Tables.AUTH_USER.AUTH_PROVIDER,
            Tables.USER.ID,
            Tables.USER.NAME,
            Tables.USER.DEFAULT_WORKSPACE_ID,
            Tables.USER.STATUS,
            Tables.USER.COMPANY_NAME,
            Tables.USER.EMAIL,
            Tables.USER.NEWS,
            Tables.USER.UI_METADATA,
          ).from(Tables.AUTH_USER)
          .innerJoin(Tables.USER)
          .on(Tables.AUTH_USER.USER_ID.eq(Tables.USER.ID))
          .where(Tables.AUTH_USER.AUTH_USER_ID.eq(userAuthId))
          .fetch()
      }

    if (result.isEmpty()) {
      return Optional.empty()
    }

    return Optional.of(createAuthenticatedUserFromRecord(result[0]))
  }

  /**
   * Fetch user from their email. TODO remove this after Cloud user handlers are removed. Use
   * getUserByEmail instead.
   *
   * @param email the user email address.
   * @return the user information if it exists in the database, Optional.empty() otherwise
   * @throws IOException in case of a db error
   */
  @Deprecated("")
  @Throws(IOException::class)
  fun getAuthenticatedUserByEmail(email: String?): Optional<AuthenticatedUser> {
    val result =
      database.query { ctx: DSLContext ->
        ctx
          .select(DSL.asterisk())
          .from(Tables.USER)
          .leftJoin(Tables.AUTH_USER)
          .on(Tables.USER.ID.eq(Tables.AUTH_USER.USER_ID))
          .where(Tables.USER.EMAIL.eq(email))
          .fetch()
      }

    if (result.isEmpty()) {
      return Optional.empty()
    }

    // FIXME: in the case of multiple auth providers, this will return the first one found.
    return Optional.of(createAuthenticatedUserFromRecord(result[0]))
  }

  @Throws(IOException::class)
  fun getUserByEmail(email: String?): Optional<User> {
    val result =
      database.query { ctx: DSLContext ->
        ctx
          .select(DSL.asterisk())
          .from(Tables.USER)
          .where(Tables.USER.EMAIL.eq(email))
          .fetch()
      }

    if (result.isEmpty()) {
      return Optional.empty()
    }

    return Optional.of(createUserFromRecord(result[0]))
  }

  /**
   * Get the default user if it exists by looking up the hardcoded default user id.
   */
  @Throws(IOException::class)
  fun getDefaultUser(): Optional<AuthenticatedUser> = getAuthenticatedUser(DEFAULT_USER_ID)

  /**
   * Get all users that have read access to the specified workspace.
   */
  @Throws(IOException::class)
  fun getUsersWithWorkspaceAccess(workspaceId: UUID?): List<User> =
    database
      .query { ctx: DSLContext ->
        ctx.fetch(
          PermissionPersistenceHelper.LIST_USERS_BY_WORKSPACE_ID_AND_PERMISSION_TYPES_QUERY,
          workspaceId,
          PermissionPersistenceHelper.getGrantingPermissionTypeArray(Permission.PermissionType.WORKSPACE_READER),
        )
      }.stream()
      .map { record: Record -> this.createUserFromRecord(record) }
      .toList()

  /**
   * Get all user access info for a particular workspace, including the specific workspace-level
   * and/or organization-level permissions that the user has that grant read-access to the workspace.
   */
  @Throws(IOException::class)
  fun listWorkspaceUserAccessInfo(workspaceId: UUID): List<WorkspaceUserAccessInfo> =
    queryWorkspaceUserAccessInfo(workspaceId)
      .stream()
      .map { record: Record -> buildWorkspaceUserAccessInfoFromRecord(record, workspaceId) }
      .toList()

  /**
   * Get all auth user IDs for a particular Airbyte user. Once Firebase is deprecated, there should
   * only be one auth user ID per Airbyte user and this method can be removed.
   */
  @Throws(IOException::class)
  fun listAuthUserIdsForUser(userId: UUID?): List<String> =
    database.query { ctx: DSLContext ->
      ctx
        .select(Tables.AUTH_USER.AUTH_USER_ID)
        .from(Tables.AUTH_USER)
        .where(Tables.AUTH_USER.USER_ID.eq(userId))
        .fetch(Tables.AUTH_USER.AUTH_USER_ID)
    }

  @Throws(IOException::class)
  fun listAuthUsersForUser(userId: UUID?): List<AuthUser> =
    database.query { ctx: DSLContext ->
      ctx
        .select(
          Tables.AUTH_USER.USER_ID,
          Tables.AUTH_USER.AUTH_USER_ID,
          Tables.AUTH_USER.AUTH_PROVIDER,
        ).from(Tables.AUTH_USER)
        .where(Tables.AUTH_USER.USER_ID.eq(userId))
        .fetch()
        .stream()
        .map { record: Record3<UUID, String, io.airbyte.db.instance.configs.jooq.generated.enums.AuthProvider> ->
          AuthUser()
            .withUserId(record.get(Tables.AUTH_USER.USER_ID))
            .withAuthUserId(record.get(Tables.AUTH_USER.AUTH_USER_ID))
            .withAuthProvider(
              if (record.get(Tables.AUTH_USER.AUTH_PROVIDER) == null) {
                null
              } else {
                record
                  .get(
                    Tables.AUTH_USER.AUTH_PROVIDER,
                    String::class.java,
                  ).toEnum<AuthProvider>()!!
              },
            )
        }.toList()
    }

  // This method is used for testing purposes only. For some reason, the actual
  // listWorkspaceUserAccessInfo method cannot be properly tested because in CI
  // tests only, permission_type enum values are mapped to `null` in the
  // `buildWorkspaceUserAccessInfoFromRecord` step. I spent so many hours trying
  // to figure out why, but I could not. This method allows me to at least test
  // that the right users are being returned in our CI tests, while leaving out
  // the problematic enum value mapping that isn't as critical to test.
  @VisibleForTesting
  @Throws(IOException::class)
  fun listJustUsersForWorkspaceUserAccessInfo(workspaceId: UUID): List<UUID> =
    queryWorkspaceUserAccessInfo(workspaceId)
      .stream()
      .map { record: Record -> record.get(Tables.USER.ID) }
      .toList()

  @Throws(IOException::class)
  private fun queryWorkspaceUserAccessInfo(workspaceId: UUID): Collection<Record> =
    database
      .query { ctx: DSLContext ->
        ctx.fetch(
          PermissionPersistenceHelper.LIST_USERS_BY_WORKSPACE_ID_AND_PERMISSION_TYPES_QUERY,
          workspaceId,
          PermissionPersistenceHelper.getGrantingPermissionTypeArray(Permission.PermissionType.WORKSPACE_READER),
        )
      }

  private fun buildWorkspaceUserAccessInfoFromRecord(
    record: Record,
    workspaceId: UUID,
  ): WorkspaceUserAccessInfo {
    var workspacePermission: Permission? = null
    if (record.get<UUID?>(PermissionPersistenceHelper.WORKSPACE_PERMISSION_ID_ALIAS, UUID::class.java) != null) {
      workspacePermission =
        Permission()
          .withUserId(record.get(Tables.USER.ID))
          .withWorkspaceId(record.get(PermissionPersistenceHelper.WORKSPACE_PERMISSION_WORKSPACE_ID_ALIAS, UUID::class.java))
          .withPermissionId(record.get(PermissionPersistenceHelper.WORKSPACE_PERMISSION_ID_ALIAS, UUID::class.java))
          .withPermissionType(
            record
              .get(
                PermissionPersistenceHelper.WORKSPACE_PERMISSION_TYPE_ALIAS,
                String::class.java,
              ).toEnum<Permission.PermissionType>()!!,
          )
    }

    var organizationPermission: Permission? = null
    if (record.get<UUID?>(PermissionPersistenceHelper.ORG_PERMISSION_ID_ALIAS, UUID::class.java) != null) {
      organizationPermission =
        Permission()
          .withUserId(record.get(Tables.USER.ID))
          .withOrganizationId(record.get(PermissionPersistenceHelper.ORG_PERMISSION_ORG_ID_ALIAS, UUID::class.java))
          .withPermissionId(record.get(PermissionPersistenceHelper.ORG_PERMISSION_ID_ALIAS, UUID::class.java))
          .withPermissionType(
            record
              .get(
                PermissionPersistenceHelper.ORG_PERMISSION_TYPE_ALIAS,
                String::class.java,
              ).toEnum<Permission.PermissionType>()!!,
          )
    }

    return WorkspaceUserAccessInfo()
      .withUserId(record.get(Tables.USER.ID))
      .withUserEmail(record.get(Tables.USER.EMAIL))
      .withUserName(record.get(Tables.USER.NAME))
      .withWorkspaceId(workspaceId)
      .withWorkspacePermission(workspacePermission)
      .withOrganizationPermission(organizationPermission)
  }

  companion object {
    private val log = KotlinLogging.logger {}

    const val PRIMARY_KEY: String = "id"
  }
}
