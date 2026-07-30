/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.DEFAULT_USER_ID
import io.airbyte.commons.annotation.InternalForTesting
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
import java.util.Locale
import java.util.Optional
import java.util.UUID
import io.airbyte.db.instance.configs.jooq.generated.enums.AuthProvider as DbAuthProvider

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
  fun writeUser(user: User) {
    writeUser(user, null, false)
  }

  fun writeUser(
    ctx: DSLContext,
    user: User,
  ) {
    check(writeUser(ctx, user, null, false))
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
      .set(Tables.USER.AGENTIC_ENABLED_AT, user.agenticEnabledAt)
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
      .set(Tables.USER.AGENTIC_ENABLED_AT, user.agenticEnabledAt)
      .set(Tables.USER.CREATED_AT, timestamp)
      .set(Tables.USER.UPDATED_AT, timestamp)
      .execute()
  }

  /**
   * Create or update a user.
   *
   * @param user user to create or update.
   */
  fun writeAuthenticatedUser(user: AuthenticatedUser) {
    check(writeUser(toUser(user), user, false)) {
      "Authentication identity is already attached to another user."
    }
  }

  fun writeAuthenticatedUser(
    ctx: DSLContext,
    user: AuthenticatedUser,
  ) {
    check(writeUser(ctx, toUser(user), user, false)) {
      "Authentication identity is already attached to another user."
    }
  }

  fun enableAgenticUser(
    userId: UUID,
    agenticEnabledAt: OffsetDateTime,
  ): OffsetDateTime = database.transaction { ctx -> enableAgenticUser(ctx, userId, agenticEnabledAt) }

  fun enableAgenticUser(
    ctx: DSLContext,
    userId: UUID,
    agenticEnabledAt: OffsetDateTime,
  ): OffsetDateTime {
    val updatedAgenticEnabledAt =
      ctx
        .update(Tables.USER)
        .set(Tables.USER.AGENTIC_ENABLED_AT, agenticEnabledAt)
        .where(Tables.USER.ID.eq(userId))
        .and(Tables.USER.AGENTIC_ENABLED_AT.isNull)
        .returning(Tables.USER.AGENTIC_ENABLED_AT)
        .fetchOne()
        ?.get(Tables.USER.AGENTIC_ENABLED_AT)
    if (updatedAgenticEnabledAt != null) {
      return updatedAgenticEnabledAt
    }

    val storedUser =
      ctx
        .select(Tables.USER.AGENTIC_ENABLED_AT)
        .from(Tables.USER)
        .where(Tables.USER.ID.eq(userId))
        .fetchOne()
    checkNotNull(storedUser) { "User $userId no longer exists." }
    return checkNotNull(storedUser.value1()) { "User $userId was not enabled for agentic use." }
  }

  /**
   * Creates an authenticated user unless a SCIM mapping claimed the email while login was in
   * progress.
   *
   * The email advisory lock makes the mapping check and user creation atomic with SCIM email
   * transitions, which acquire the same lock.
   */
  fun createAuthenticatedUserIfNoScimMapping(user: AuthenticatedUser): Boolean = writeUser(toUser(user), user, true)

  fun createAuthenticatedUserIfNoScimMapping(
    ctx: DSLContext,
    user: AuthenticatedUser,
  ): Boolean = writeUser(ctx, toUser(user), user, true)

  private fun writeUser(
    user: User,
    authenticatedUser: AuthenticatedUser?,
    rejectScimManagedEmail: Boolean,
  ): Boolean =
    database.transaction { ctx ->
      writeUser(ctx, user, authenticatedUser, rejectScimManagedEmail)
    }

  private fun writeUser(
    ctx: DSLContext,
    user: User,
    authenticatedUser: AuthenticatedUser?,
    rejectScimManagedEmail: Boolean,
  ): Boolean {
    while (true) {
      try {
        val observedUser =
          ctx
            .select(Tables.USER.EMAIL)
            .from(Tables.USER)
            .where(Tables.USER.ID.eq(user.userId))
            .fetchOne()
        val emailsToLock =
          listOfNotNull(observedUser?.value1(), user.email)
            .distinct()
            .sortedWith(compareBy<String> { it.lowercase(Locale.ROOT) }.thenBy { it })
        // Dual-lock for this one release's rolling-deploy bridge: old pods (pre-rename) only ever
        // take the legacy (unprefixed) key, so a new pod must also take it to serialize against
        // them. All legacy keys are acquired (sorted) before any current key (sorted) so that two
        // new pods locking overlapping email sets in different orders can never deadlock against
        // each other. TODO: drop the legacy pass next release.
        emailsToLock.forEach { email ->
          ctx.fetch("SELECT pg_advisory_xact_lock(hashtextextended(lower(?), 0))", email)
        }
        emailsToLock.forEach { email ->
          ctx.fetch("SELECT pg_advisory_xact_lock(hashtextextended('email:' || lower(?), 0))", email)
        }
        val lockedUser =
          ctx
            .select(Tables.USER.EMAIL)
            .from(Tables.USER)
            .where(Tables.USER.ID.eq(user.userId))
            .forUpdate()
            .fetchOne()
        if (
          (observedUser == null) != (lockedUser == null) ||
          observedUser?.value1() != lockedUser?.value1()
        ) {
          throw RetryUserWriteException()
        }
        if (lockedUser == null &&
          rejectScimManagedEmail &&
          ctx.fetchExists(
            ctx
              .selectOne()
              .from(Tables.SCIM_RESOURCE_MAPPING)
              .where(Tables.SCIM_RESOURCE_MAPPING.RESOURCE_TYPE.eq(io.airbyte.db.instance.configs.jooq.generated.enums.ScimResourceType.USER))
              .and(Tables.SCIM_RESOURCE_MAPPING.PRIMARY_EMAIL.equalIgnoreCase(user.email)),
          )
        ) {
          return false
        }
        if (lockedUser == null) {
          val dbAuthProvider = authenticatedUser?.authProvider?.value()?.toEnum<DbAuthProvider>()
          val existingAuthUsers =
            authenticatedUser?.let { lockAuthUsers(ctx, it.authUserId) }.orEmpty()
          if (existingAuthUsers.any { it.userId != user.userId }) {
            return false
          }
          createUser(ctx, user)
          if (authenticatedUser != null && existingAuthUsers.isEmpty()) {
            insertAuthUser(ctx, authenticatedUser.userId, authenticatedUser.authUserId, dbAuthProvider)
          }
        } else {
          updateUser(ctx, user)
        }
        return true
      } catch (_: RetryUserWriteException) {
        // Retry with the current stored email so every identity key remains locked until commit.
      }
    }
  }

  private class RetryUserWriteException : RuntimeException(null, null, false, false)

  fun writeAuthUser(
    userId: UUID,
    authUserId: String,
    authProvider: AuthProvider?,
  ): Boolean = database.transaction { ctx -> attachAuthUser(ctx, userId, authUserId, authProvider) }

  fun writeAuthUser(
    ctx: DSLContext,
    userId: UUID,
    authUserId: String,
    authProvider: AuthProvider?,
  ): Boolean = attachAuthUser(ctx, userId, authUserId, authProvider)

  private fun attachAuthUser(
    ctx: DSLContext,
    userId: UUID,
    authUserId: String,
    authProvider: AuthProvider?,
  ): Boolean {
    val dbAuthProvider = authProvider?.value()?.toEnum<DbAuthProvider>()
    val existingAuthUsers = lockAuthUsers(ctx, authUserId)
    if (existingAuthUsers.any { it.userId != userId }) {
      return false
    }
    if (existingAuthUsers.isNotEmpty()) {
      return true
    }

    insertAuthUser(ctx, userId, authUserId, dbAuthProvider)
    return true
  }

  /**
   * Locks authentication subjects in a stable order and verifies that each has exactly one owner,
   * which is the expected Airbyte user.
   */
  fun requireAuthUsersOwnedBy(
    ctx: DSLContext,
    userId: UUID,
    authUserIds: Collection<String>,
  ) {
    authUserIds
      .distinct()
      .sorted()
      .forEach { authUserId ->
        val owners = lockAuthUsers(ctx, authUserId).map { it.userId }.distinct()
        check(owners == listOf(userId)) {
          "Authentication identity $authUserId is not uniquely owned by user $userId."
        }
      }
  }

  /**
   * Locks an authentication subject and verifies that it is either unowned or owned only by the
   * expected Airbyte user.
   */
  fun requireAuthUserAvailableTo(
    ctx: DSLContext,
    userId: UUID,
    authUserId: String,
  ) {
    val owners = lockAuthUsers(ctx, authUserId).map { it.userId }.distinct()
    check(owners.isEmpty() || owners == listOf(userId)) {
      "Authentication identity $authUserId is owned by another user."
    }
  }

  /**
   * Locks every authentication subject participating in a replacement in one stable order.
   *
   * Existing subjects must remain uniquely owned by the expected Airbyte user. The incoming
   * subject may be unowned or already owned by that same user.
   *
   * @return whether the incoming subject is available to the expected Airbyte user
   */
  fun lockAuthUsersForReplacement(
    ctx: DSLContext,
    userId: UUID,
    existingAuthUserIds: Collection<String>,
    incomingAuthUserId: String,
  ): Boolean {
    val existingSubjects = existingAuthUserIds.toSet()
    var incomingSubjectAvailable = true
    (existingSubjects + incomingAuthUserId)
      .sorted()
      .forEach { authUserId ->
        val owners = lockAuthUsers(ctx, authUserId).map { it.userId }.distinct()
        if (authUserId in existingSubjects) {
          check(owners == listOf(userId)) {
            "Authentication identity $authUserId is not uniquely owned by user $userId."
          }
        } else {
          incomingSubjectAvailable = owners.isEmpty() || owners == listOf(userId)
        }
      }
    return incomingSubjectAvailable
  }

  /**
   * Serializes the staged database and external phases of an authentication identity replacement
   * for one Airbyte user.
   */
  fun lockAuthUserReplacement(
    ctx: DSLContext,
    userId: UUID,
  ) {
    ctx.fetch("SELECT pg_advisory_xact_lock(hashtextextended('auth-user-replacement:' || ?, 0))", userId.toString())
  }

  /**
   * Replace the auth user for a particular Airbyte user.
   *
   * @param userId internal user id
   * @param newAuthUserId new auth user id
   * @param newAuthProvider new auth provider
   * @return whether the identity was replaced or was already attached to this user
   * @throws IOException in case of a db error
   */
  fun replaceAuthUserForUserId(
    userId: UUID,
    newAuthUserId: String,
    newAuthProvider: AuthProvider?,
  ): Boolean =
    database.transaction { ctx: DSLContext ->
      replaceAuthUserForUserId(ctx, userId, newAuthUserId, newAuthProvider)
    }

  fun replaceAuthUserForUserId(
    ctx: DSLContext,
    userId: UUID,
    newAuthUserId: String,
    newAuthProvider: AuthProvider?,
  ): Boolean {
    lockAuthUserReplacement(ctx, userId)
    if (
      !lockAuthUsersForReplacement(
        ctx,
        userId,
        listAuthUsersForUser(ctx, userId).map { it.authUserId },
        newAuthUserId,
      )
    ) {
      return false
    }
    val dbAuthProvider = newAuthProvider?.value()?.toEnum<DbAuthProvider>()
    val existingAuthUsers = lockAuthUsers(ctx, newAuthUserId)
    if (existingAuthUsers.any { it.userId != userId }) {
      return false
    }
    val retainedAuthUser =
      existingAuthUsers.firstOrNull {
        it.userId == userId && it.authProvider == dbAuthProvider
      }
    ctx
      .deleteFrom(Tables.AUTH_USER)
      .where(Tables.AUTH_USER.USER_ID.eq(userId))
      .and(retainedAuthUser?.let { Tables.AUTH_USER.ID.ne(it.id) } ?: DSL.noCondition())
      .execute()
    if (retainedAuthUser == null) {
      insertAuthUser(ctx, userId, newAuthUserId, dbAuthProvider)
    }
    return true
  }

  private fun lockAuthUsers(
    ctx: DSLContext,
    authUserId: String,
  ): List<AuthUserOwnership> {
    ctx.fetch("SELECT pg_advisory_xact_lock(hashtextextended('auth-user:' || ?, 0))", authUserId)
    return ctx
      .select(Tables.AUTH_USER.ID, Tables.AUTH_USER.USER_ID, Tables.AUTH_USER.AUTH_PROVIDER)
      .from(Tables.AUTH_USER)
      .where(Tables.AUTH_USER.AUTH_USER_ID.eq(authUserId))
      .orderBy(Tables.AUTH_USER.USER_ID, Tables.AUTH_USER.AUTH_PROVIDER)
      .forUpdate()
      .fetch {
        AuthUserOwnership(
          id = it.value1(),
          userId = it.value2(),
          authProvider = it.value3(),
        )
      }
  }

  private fun insertAuthUser(
    ctx: DSLContext,
    userId: UUID,
    authUserId: String,
    authProvider: DbAuthProvider?,
  ) {
    val now = OffsetDateTime.now()
    ctx
      .insertInto(Tables.AUTH_USER)
      .set(Tables.AUTH_USER.ID, UUID.randomUUID())
      .set(Tables.AUTH_USER.USER_ID, userId)
      .set(Tables.AUTH_USER.AUTH_USER_ID, authUserId)
      .set(Tables.AUTH_USER.AUTH_PROVIDER, authProvider)
      .set(Tables.AUTH_USER.CREATED_AT, now)
      .set(Tables.AUTH_USER.UPDATED_AT, now)
      .execute()
  }

  private data class AuthUserOwnership(
    val id: UUID,
    val userId: UUID,
    val authProvider: DbAuthProvider,
  )

  /**
   * Delete User.
   *
   * @param userId internal user id
   * @return user if found
   */
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
  fun getUser(userId: UUID?): Optional<User> = database.query { ctx -> getUser(ctx, userId) }

  fun getUser(
    ctx: DSLContext,
    userId: UUID?,
  ): Optional<User> {
    val result =
      ctx
        .select(DSL.asterisk())
        .from(Tables.USER)
        .where(Tables.USER.ID.eq(userId))
        .fetch()

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
      ).withAgenticEnabledAt(record.get(Tables.USER.AGENTIC_ENABLED_AT))

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
      .withAgenticEnabledAt(user.agenticEnabledAt)
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
  fun getUserByAuthId(userAuthId: String?): Optional<AuthenticatedUser> = database.query { ctx -> getUserByAuthId(ctx, userAuthId) }

  fun getUserByAuthId(
    ctx: DSLContext,
    userAuthId: String?,
  ): Optional<AuthenticatedUser> {
    val owners =
      ctx
        .selectDistinct(Tables.AUTH_USER.USER_ID)
        .from(Tables.AUTH_USER)
        .where(Tables.AUTH_USER.AUTH_USER_ID.eq(userAuthId))
        .asTable("auth_user_owners")
    val resolvedUserId = owners.field(Tables.AUTH_USER.USER_ID)!!
    val result =
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
          Tables.USER.AGENTIC_ENABLED_AT,
        ).from(owners)
        .innerJoin(Tables.USER)
        .on(resolvedUserId.eq(Tables.USER.ID))
        .innerJoin(Tables.AUTH_USER)
        .on(Tables.AUTH_USER.USER_ID.eq(resolvedUserId))
        .and(Tables.AUTH_USER.AUTH_USER_ID.eq(userAuthId))
        .whereNotExists(
          ctx
            .selectOne()
            .from(Tables.AUTH_USER)
            .where(Tables.AUTH_USER.AUTH_USER_ID.eq(userAuthId))
            .and(Tables.AUTH_USER.USER_ID.ne(resolvedUserId)),
        ).orderBy(Tables.AUTH_USER.AUTH_PROVIDER)
        .limit(1)
        .fetch()

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

  fun getUserByEmail(email: String?): Optional<User> = database.query { ctx -> getUserByEmail(ctx, email) }

  fun getUserByEmail(
    ctx: DSLContext,
    email: String?,
  ): Optional<User> {
    val result =
      ctx
        .select(DSL.asterisk())
        .from(Tables.USER)
        .where(Tables.USER.EMAIL.eq(email))
        .fetch()

    if (result.isEmpty()) {
      return Optional.empty()
    }

    return Optional.of(createUserFromRecord(result[0]))
  }

  /**
   * Get the default user if it exists by looking up the hardcoded default user id.
   */
  fun getDefaultUser(): Optional<AuthenticatedUser> = getAuthenticatedUser(DEFAULT_USER_ID)

  /**
   * Get all users that have read access to the specified workspace.
   */
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
  fun listWorkspaceUserAccessInfo(workspaceId: UUID): List<WorkspaceUserAccessInfo> =
    queryWorkspaceUserAccessInfo(workspaceId)
      .stream()
      .map { record: Record -> buildWorkspaceUserAccessInfoFromRecord(record, workspaceId) }
      .toList()

  /**
   * Get all auth user IDs for a particular Airbyte user. Once Firebase is deprecated, there should
   * only be one auth user ID per Airbyte user and this method can be removed.
   */
  fun listAuthUserIdsForUser(userId: UUID?): List<String> =
    database.query { ctx: DSLContext ->
      val otherOwner = Tables.AUTH_USER.`as`("other_owner")
      ctx
        .selectDistinct(Tables.AUTH_USER.AUTH_USER_ID)
        .from(Tables.AUTH_USER)
        .where(Tables.AUTH_USER.USER_ID.eq(userId))
        .andNotExists(
          ctx
            .selectOne()
            .from(otherOwner)
            .where(otherOwner.AUTH_USER_ID.eq(Tables.AUTH_USER.AUTH_USER_ID))
            .and(otherOwner.USER_ID.ne(Tables.AUTH_USER.USER_ID)),
        ).fetch(Tables.AUTH_USER.AUTH_USER_ID)
    }

  fun listAuthUsersForUser(userId: UUID?): List<AuthUser> = database.query { ctx -> listAuthUsersForUser(ctx, userId) }

  fun listAuthUsersForUser(
    ctx: DSLContext,
    userId: UUID?,
  ): List<AuthUser> =
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

  // This method is used for testing purposes only. For some reason, the actual
  // listWorkspaceUserAccessInfo method cannot be properly tested because in CI
  // tests only, permission_type enum values are mapped to `null` in the
  // `buildWorkspaceUserAccessInfoFromRecord` step. I spent so many hours trying
  // to figure out why, but I could not. This method allows me to at least test
  // that the right users are being returned in our CI tests, while leaving out
  // the problematic enum value mapping that isn't as critical to test.
  @InternalForTesting
  fun listJustUsersForWorkspaceUserAccessInfo(workspaceId: UUID): List<UUID> =
    queryWorkspaceUserAccessInfo(workspaceId)
      .stream()
      .map { record: Record -> record.get(Tables.USER.ID) }
      .toList()

  private fun queryWorkspaceUserAccessInfo(workspaceId: UUID): Collection<Record> =
    database
      .query { ctx: DSLContext ->
        ctx.fetch(
          PermissionPersistenceHelper.LIST_USERS_BY_WORKSPACE_ID_AND_PERMISSION_TYPES_QUERY,
          workspaceId,
          PermissionPersistenceHelper.getGrantingPermissionTypeArray(Permission.PermissionType.WORKSPACE_READER),
        )
      }

  /**
   * Check if any users with emails matching the given domain exist outside of the specified organization.
   *
   * @param emailDomain the email domain to check (e.g., "example.com")
   * @param organizationId the organization ID to exclude from the check
   * @return list of user IDs with the email domain that belong to different organizations
   */
  fun findUsersWithEmailDomainOutsideOrganization(
    emailDomain: String,
    organizationId: UUID,
  ): List<UUID> =
    database.query { ctx: DSLContext ->
      ctx
        .select(Tables.USER.ID)
        .from(Tables.USER)
        .leftJoin(Tables.PERMISSION)
        .on(Tables.USER.ID.eq(Tables.PERMISSION.USER_ID))
        .where(Tables.USER.EMAIL.likeIgnoreCase("%@$emailDomain"))
        .and(
          Tables.PERMISSION.ORGANIZATION_ID.isNull
            .or(Tables.PERMISSION.ORGANIZATION_ID.ne(organizationId)),
        ).groupBy(Tables.USER.ID)
        .fetch(Tables.USER.ID)
    }

  /**
   * Find users with emails matching the given domain who do NOT have any permission to the specified organization.
   * This is used during SSO activation to find users who need to be granted organization membership.
   *
   * @param emailDomain the email domain to check (e.g., "example.com")
   * @param organizationId the organization ID to check permissions against
   * @return list of user IDs with the email domain who do not have permission to the organization
   */
  fun findUsersWithEmailDomainWithoutOrgPermission(
    emailDomain: String,
    organizationId: UUID,
  ): List<UUID> =
    database.query { ctx: DSLContext ->
      // Subquery to find users who DO have permission to this org
      val usersWithOrgPermission =
        ctx
          .select(Tables.PERMISSION.USER_ID)
          .from(Tables.PERMISSION)
          .where(Tables.PERMISSION.ORGANIZATION_ID.eq(organizationId))
          .and(Tables.PERMISSION.USER_ID.isNotNull)

      // Find users with matching email domain who are NOT in the subquery
      ctx
        .select(Tables.USER.ID)
        .from(Tables.USER)
        .where(Tables.USER.EMAIL.likeIgnoreCase("%@$emailDomain"))
        .and(Tables.USER.ID.notIn(usersWithOrgPermission))
        .fetch(Tables.USER.ID)
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
