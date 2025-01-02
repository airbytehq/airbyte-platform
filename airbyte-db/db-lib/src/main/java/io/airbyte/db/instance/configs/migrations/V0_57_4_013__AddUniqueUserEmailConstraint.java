/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations;

import com.google.common.annotations.VisibleForTesting;
import java.time.OffsetDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
public class V0_57_4_013__AddUniqueUserEmailConstraint extends BaseJavaMigration {

  private static final Logger LOGGER = LoggerFactory.getLogger(V0_57_4_013__AddUniqueUserEmailConstraint.class);

  private static final UUID DEFAULT_USER_ID = UUID.fromString("00000000-0000-0000-0000-000000000000");
  private static final Table<Record> USER = DSL.table("\"user\"");
  private static final Table<Record> USER_INVITATION = DSL.table("user_invitation");
  private static final Table<Record> CONNECTION_TIMELINE_EVENT = DSL.table("connection_timeline_event");
  private static final Table<Record> SSO_CONFIG = DSL.table("sso_config");
  private static final Table<Record> PERMISSION = DSL.table("permission");
  private static final Field<UUID> ID = DSL.field("id", SQLDataType.UUID);
  private static final Field<UUID> USER_ID = DSL.field("user_id", SQLDataType.UUID);
  private static final Field<UUID> INVITER_USER_ID = DSL.field("inviter_user_id", SQLDataType.UUID);
  private static final Field<UUID> ORGANIZATION_ID = DSL.field("organization_id", SQLDataType.UUID);
  private static final Field<OffsetDateTime> CREATED_AT = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE);
  private static final Field<String> EMAIL = DSL.field("email", SQLDataType.VARCHAR);

  @Override
  public void migrate(final Context context) throws Exception {
    LOGGER.info("Running migration: {}", this.getClass().getSimpleName());

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    final DSLContext ctx = DSL.using(context.getConnection());
    deleteDuplicateUsers(ctx);
    addUniqueUserEmailConstraint(ctx);
  }

  @VisibleForTesting
  static void deleteDuplicateUsers(final DSLContext ctx) {
    final List<String> duplicateEmails = getDuplicateEmails(ctx);
    final List<UUID> ssoOrganizationIds = getSsoOrganizationIds(ctx);

    for (final String email : duplicateEmails) {
      LOGGER.info("Found duplicate users with email {}", email);
      List<User> users = getUsersByEmail(ctx, email);

      final Optional<User> defaultUser = users.stream()
          .filter(u -> u.id().equals(DEFAULT_USER_ID))
          .findFirst();

      if (defaultUser.isPresent()) {
        LOGGER.info("Clearing email for default user");
        clearUserEmail(ctx, DEFAULT_USER_ID);
        users = getUsersByEmail(ctx, email);
      }

      if (users.size() > 1) {
        final UUID userToKeep = getUserToKeep(ctx, users, ssoOrganizationIds);
        users.stream()
            .filter(u -> !u.id().equals(userToKeep))
            .forEach(u -> deleteUserById(ctx, u.id(), userToKeep));
      }
    }
  }

  private static UUID getUserToKeep(final DSLContext ctx, final List<User> users, final List<UUID> ssoOrganizationIds) {
    // Prefer to keep user with permissions to an SSO organization
    final List<User> ssoUsers = users.stream()
        .filter(u -> hasSsoOrgPermissions(ctx, u.id(), ssoOrganizationIds))
        .toList();

    if (ssoUsers.size() == 1) {
      LOGGER.info("Keeping user with SSO permissions {}", ssoUsers.getFirst().id());
      return ssoUsers.getFirst().id();
    }

    // Otherwise, keep the oldest one
    final UUID oldestUserId = users.stream()
        .min(Comparator.comparing(User::createdAt))
        .orElseThrow()
        .id();
    LOGGER.info("Keeping oldest user {}", oldestUserId);
    return oldestUserId;
  }

  private static void deleteUserById(final DSLContext ctx, final UUID userId, final UUID replacementUserId) {
    // update sent invitations
    final int inviteUpdateCount = ctx.update(USER_INVITATION)
        .set(INVITER_USER_ID, replacementUserId)
        .where(INVITER_USER_ID.eq(userId))
        .execute();
    if (inviteUpdateCount > 0) {
      LOGGER.info("Updated {} user invitations from {} with inviter_user_id {}", inviteUpdateCount, userId, replacementUserId);
    }

    // update timeline events
    final int eventUpdateCount = ctx.update(CONNECTION_TIMELINE_EVENT)
        .set(USER_ID, replacementUserId)
        .where(USER_ID.eq(userId))
        .execute();
    if (eventUpdateCount > 0) {
      LOGGER.info("Updated {} connection timeline events from {} with user_id {}", eventUpdateCount, userId, replacementUserId);
    }

    // delete user
    ctx.deleteFrom(USER)
        .where(ID.eq(userId))
        .execute();
    LOGGER.info("Deleted user with id {}", userId);
  }

  private static List<UUID> getSsoOrganizationIds(final DSLContext ctx) {
    return ctx.select(ORGANIZATION_ID)
        .from(SSO_CONFIG)
        .fetch()
        .map(r -> r.get(ORGANIZATION_ID));
  }

  private static boolean hasSsoOrgPermissions(final DSLContext ctx, final UUID userId, final List<UUID> ssoOrganizationIds) {
    final Result<Record> permissions = ctx.select(DSL.asterisk())
        .from(PERMISSION)
        .where(USER_ID.eq(userId).and(ORGANIZATION_ID.in(ssoOrganizationIds)))
        .fetch();
    return permissions.isNotEmpty();
  }

  private static void clearUserEmail(final DSLContext ctx, final UUID userId) {
    ctx.update(USER)
        .set(EMAIL, "")
        .where(ID.eq(userId))
        .execute();
  }

  private static List<String> getDuplicateEmails(final DSLContext ctx) {
    return ctx.select(EMAIL)
        .from(USER)
        .groupBy(EMAIL)
        .having(DSL.count().greaterThan(1))
        .fetch()
        .map(r -> r.get(EMAIL));
  }

  private static List<User> getUsersByEmail(final DSLContext ctx, final String email) {
    return ctx.select(ID, EMAIL, CREATED_AT)
        .from(USER)
        .where(EMAIL.eq(email))
        .fetch()
        .map(r -> new User(r.get(ID), r.get(EMAIL), r.get(CREATED_AT)));
  }

  @VisibleForTesting
  static void addUniqueUserEmailConstraint(final DSLContext ctx) {
    ctx.createUniqueIndex("user_email_unique_key")
        .on(USER, DSL.lower(EMAIL))
        .execute();
  }

  record User(UUID id, String email, OffsetDateTime createdAt) {}

}
