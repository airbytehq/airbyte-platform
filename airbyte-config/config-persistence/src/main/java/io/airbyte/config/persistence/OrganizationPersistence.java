/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static io.airbyte.commons.ConstantsKt.DEFAULT_ORGANIZATION_ID;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.CONNECTION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.ORGANIZATION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.PERMISSION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.SSO_CONFIG;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.WORKSPACE;
import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.noCondition;
import static org.jooq.impl.DSL.select;

import io.airbyte.config.Organization;
import io.airbyte.config.SsoConfig;
import io.airbyte.data.services.shared.ResourcesByUserQueryPaginated;
import io.airbyte.db.Database;
import io.airbyte.db.ExceptionWrappingDatabase;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;

/**
 * Permission Persistence.
 * <p>
 * Handle persisting Permission to the Config Database and perform all SQL queries.
 */
public class OrganizationPersistence {

  private final ExceptionWrappingDatabase database;

  public OrganizationPersistence(final Database database) {
    this.database = new ExceptionWrappingDatabase(database);
  }

  /**
   * Retrieve an organization.
   *
   * @param organizationId to fetch
   * @return fetched organization
   * @throws IOException when interaction with DB failed
   */
  public Optional<Organization> getOrganization(final UUID organizationId) throws IOException {
    final Result<Record> result = database.query(ctx -> ctx
        .select(asterisk())
        .from(ORGANIZATION)
        .leftJoin(SSO_CONFIG).on(ORGANIZATION.ID.eq(SSO_CONFIG.ORGANIZATION_ID))
        .where(ORGANIZATION.ID.eq(organizationId)).fetch());

    if (result.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(createOrganizationFromRecord(result.get(0)));
  }

  public Optional<Organization> getOrganizationByWorkspaceId(final UUID workspaceId) throws IOException {
    final Result<Record> result = database.query(ctx -> ctx
        .select(asterisk())
        .from(ORGANIZATION)
        .leftJoin(SSO_CONFIG).on(ORGANIZATION.ID.eq(SSO_CONFIG.ORGANIZATION_ID))
        .join(WORKSPACE)
        .on(ORGANIZATION.ID.eq(WORKSPACE.ORGANIZATION_ID))
        .where(WORKSPACE.ID.eq(workspaceId)).fetch());

    if (result.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(createOrganizationFromRecord(result.get(0)));
  }

  public Optional<Organization> getOrganizationByConnectionId(final UUID connectionId) throws IOException {
    final Result<Record> result = database.query(ctx -> ctx
        .select(asterisk())
        .from(ORGANIZATION)
        .leftJoin(SSO_CONFIG)
        .on(ORGANIZATION.ID.eq(SSO_CONFIG.ORGANIZATION_ID))
        .join(WORKSPACE)
        .on(ORGANIZATION.ID.eq(WORKSPACE.ORGANIZATION_ID))
        .join(ACTOR)
        .on(ACTOR.WORKSPACE_ID.eq(WORKSPACE.ID))
        .join(CONNECTION)
        .on(CONNECTION.SOURCE_ID.eq(ACTOR.ID))
        .where(CONNECTION.ID.eq(connectionId)).fetch());

    if (result.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(createOrganizationFromRecord(result.get(0)));
  }

  /**
   * Create a new organization and insert into the database.
   *
   * @param organization to be created
   * @return created organization
   * @throws IOException when interaction with DB failed
   */
  public Organization createOrganization(final Organization organization) throws IOException {
    database.transaction(ctx -> {
      try {
        insertOrganizationIntoDB(ctx, organization);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
      return null;
    });
    return organization;
  }

  /**
   * Update an existing organization.
   *
   * @param organization - proposed organization to update
   * @return updated organization
   * @throws IOException - when interaction with DB failed
   */
  public Organization updateOrganization(final Organization organization) throws IOException {
    database.transaction(ctx -> {
      try {
        updateOrganizationInDB(ctx, organization);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
      return null;
    });
    return organization;
  }

  /**
   * Get the default organization if it exists by looking up the hardcoded default organization id.
   */
  public Optional<Organization> getDefaultOrganization() throws IOException {
    return getOrganization(DEFAULT_ORGANIZATION_ID);
  }

  /**
   * List all organizations by user id, returning result ordered by org name. Supports keyword search.
   */
  public List<Organization> listOrganizationsByUserId(final UUID userId, final Optional<String> keyword)
      throws IOException {
    return database.query(ctx -> ctx.select(ORGANIZATION.asterisk(), SSO_CONFIG.asterisk())
        .from(ORGANIZATION)
        .join(PERMISSION)
        .on(ORGANIZATION.ID.eq(PERMISSION.ORGANIZATION_ID))
        .leftJoin(SSO_CONFIG).on(ORGANIZATION.ID.eq(SSO_CONFIG.ORGANIZATION_ID))
        .where(PERMISSION.USER_ID.eq(userId))
        .and(PERMISSION.ORGANIZATION_ID.isNotNull())
        .and(keyword.isPresent() ? ORGANIZATION.NAME.containsIgnoreCase(keyword.get()) : noCondition())
        .orderBy(ORGANIZATION.NAME.asc())
        .fetch())
        .stream()
        .map(OrganizationPersistence::createOrganizationFromRecord)
        .toList();
  }

  /**
   * List all organizations by user id, returning result ordered by org name. Supports pagination and
   * keyword search.
   */
  public List<Organization> listOrganizationsByUserIdPaginated(final ResourcesByUserQueryPaginated query, final Optional<String> keyword)
      throws IOException {
    return database.query(ctx -> ctx.select(ORGANIZATION.asterisk(), SSO_CONFIG.asterisk())
        .from(ORGANIZATION)
        .join(PERMISSION)
        .on(ORGANIZATION.ID.eq(PERMISSION.ORGANIZATION_ID))
        .leftJoin(SSO_CONFIG).on(ORGANIZATION.ID.eq(SSO_CONFIG.ORGANIZATION_ID))
        .where(PERMISSION.USER_ID.eq(query.userId()))
        .and(PERMISSION.ORGANIZATION_ID.isNotNull())
        .and(keyword.isPresent() ? ORGANIZATION.NAME.containsIgnoreCase(keyword.get()) : noCondition())
        .orderBy(ORGANIZATION.NAME.asc())
        .limit(query.pageSize())
        .offset(query.rowOffset())
        .fetch())
        .stream()
        .map(OrganizationPersistence::createOrganizationFromRecord)
        .toList();
  }

  /**
   * Get the matching organization that has the given sso config realm. If not exists, returns empty
   * optional obejct.
   */

  public Optional<Organization> getOrganizationBySsoConfigRealm(final String ssoConfigRealm) throws IOException {
    final Result<Record> result = database.query(ctx -> ctx
        .select(asterisk())
        .from(ORGANIZATION)
        .join(SSO_CONFIG)
        .on(ORGANIZATION.ID.eq(SSO_CONFIG.ORGANIZATION_ID))
        .where(SSO_CONFIG.KEYCLOAK_REALM.eq(ssoConfigRealm)).fetch());

    if (result.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(createOrganizationFromRecord(result.get(0)));
  }

  public SsoConfig createSsoConfig(final SsoConfig ssoConfig) throws IOException {
    database.transaction(ctx -> {
      try {
        insertSsoConfigIntoDB(ctx, ssoConfig);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
      return null;
    });
    return ssoConfig;
  }

  public SsoConfig updateSsoConfig(final SsoConfig ssoConfig) throws IOException {
    database.transaction(ctx -> {
      try {
        updateSsoConfigInDB(ctx, ssoConfig);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
      return null;
    });
    return ssoConfig;
  }

  public Optional<SsoConfig> getSsoConfigForOrganization(final UUID organizationId) throws IOException {
    final Result<Record> result = database.query(ctx -> ctx
        .select(asterisk())
        .from(SSO_CONFIG)
        .where(SSO_CONFIG.ORGANIZATION_ID.eq(organizationId)).fetch());

    if (result.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(createSsoConfigFromRecord(result.get(0)));
  }

  public Optional<SsoConfig> getSsoConfigByRealmName(final String realmName) throws IOException {
    final Result<Record> result = database.query(ctx -> ctx
        .select(asterisk())
        .from(SSO_CONFIG)
        .where(SSO_CONFIG.KEYCLOAK_REALM.eq(realmName)).fetch());

    if (result.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(createSsoConfigFromRecord(result.get(0)));
  }

  private void updateOrganizationInDB(final DSLContext ctx, final Organization organization) throws IOException {
    final OffsetDateTime timestamp = OffsetDateTime.now();

    final boolean isExistingConfig = ctx.fetchExists(select()
        .from(ORGANIZATION)
        .where(ORGANIZATION.ID.eq(organization.getOrganizationId())));

    if (!isExistingConfig) {
      throw new IOException("Organization with id " + organization.getOrganizationId() + " does not exist.");
    }
    ctx.update(ORGANIZATION)
        .set(ORGANIZATION.NAME, organization.getName())
        .set(ORGANIZATION.EMAIL, organization.getEmail())
        .set(ORGANIZATION.USER_ID, organization.getUserId())
        .set(ORGANIZATION.UPDATED_AT, timestamp)
        .where(ORGANIZATION.ID.eq(organization.getOrganizationId()))
        .execute();
  }

  private void insertOrganizationIntoDB(final DSLContext ctx, final Organization organization) throws IOException {
    final OffsetDateTime timestamp = OffsetDateTime.now();

    final boolean isExistingConfig = ctx.fetchExists(select()
        .from(ORGANIZATION)
        .where(ORGANIZATION.ID.eq(organization.getOrganizationId())));

    if (isExistingConfig) {
      throw new IOException("Organization with id " + organization.getOrganizationId() + " already exists.");
    }
    ctx.insertInto(ORGANIZATION)
        .set(ORGANIZATION.ID, organization.getOrganizationId())
        .set(ORGANIZATION.USER_ID, organization.getUserId())
        .set(ORGANIZATION.NAME, organization.getName())
        .set(ORGANIZATION.EMAIL, organization.getEmail())
        .set(ORGANIZATION.CREATED_AT, timestamp)
        .set(ORGANIZATION.UPDATED_AT, timestamp)
        .execute();

  }

  private void insertSsoConfigIntoDB(final DSLContext ctx, final SsoConfig ssoConfig) throws IOException {
    final OffsetDateTime timestamp = OffsetDateTime.now();

    final boolean isExistingConfig = ctx.fetchExists(select()
        .from(SSO_CONFIG)
        .where(SSO_CONFIG.ORGANIZATION_ID.eq(ssoConfig.getOrganizationId())));

    if (isExistingConfig) {
      throw new IOException("SsoConfig with organization id " + ssoConfig.getOrganizationId() + " already exists.");
    }
    ctx.insertInto(SSO_CONFIG)
        .set(SSO_CONFIG.ID, ssoConfig.getSsoConfigId())
        .set(SSO_CONFIG.ORGANIZATION_ID, ssoConfig.getOrganizationId())
        .set(SSO_CONFIG.KEYCLOAK_REALM, ssoConfig.getKeycloakRealm())
        .set(SSO_CONFIG.CREATED_AT, timestamp)
        .set(SSO_CONFIG.UPDATED_AT, timestamp)
        .execute();
  }

  private void updateSsoConfigInDB(final DSLContext ctx, final SsoConfig ssoConfig) throws IOException {
    final OffsetDateTime timestamp = OffsetDateTime.now();
    ctx.update(SSO_CONFIG)
        .set(SSO_CONFIG.ORGANIZATION_ID, ssoConfig.getOrganizationId())
        .set(SSO_CONFIG.KEYCLOAK_REALM, ssoConfig.getKeycloakRealm())
        .set(SSO_CONFIG.UPDATED_AT, timestamp)
        .where(SSO_CONFIG.ID.eq(ssoConfig.getSsoConfigId()))
        .execute();
  }

  private static Organization createOrganizationFromRecord(final Record record) {
    return new Organization()
        .withOrganizationId(record.get(ORGANIZATION.ID))
        .withName(record.get(ORGANIZATION.NAME))
        .withEmail(record.get(ORGANIZATION.EMAIL))
        .withUserId(record.get(ORGANIZATION.USER_ID))
        .withSsoRealm(record.get(SSO_CONFIG.KEYCLOAK_REALM));
  }

  private static SsoConfig createSsoConfigFromRecord(final Record record) {
    return new SsoConfig()
        .withSsoConfigId(record.get(SSO_CONFIG.ID))
        .withOrganizationId(record.get(SSO_CONFIG.ORGANIZATION_ID))
        .withKeycloakRealm(record.get(SSO_CONFIG.KEYCLOAK_REALM));
  }

}
