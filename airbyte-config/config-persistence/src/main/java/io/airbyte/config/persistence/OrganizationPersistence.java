/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.ORGANIZATION;
import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.select;

import io.airbyte.config.Organization;
import io.airbyte.db.Database;
import io.airbyte.db.ExceptionWrappingDatabase;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;

/**
 * Permission Persistence.
 *
 * Handle persisting Permission to the Config Database and perform all SQL queries.
 *
 */
public class OrganizationPersistence {

  private final ExceptionWrappingDatabase database;

  public static final String PRIMARY_KEY = "id";
  public static final String USER_KEY = "user_id";
  public static final String WORKSPACE_KEY = "workspace_id";

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
        .where(ORGANIZATION.ID.eq(organizationId)).fetch());

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
  public Organization createOrganization(Organization organization) throws IOException {
    database.transaction(ctx -> {
      try {
        insertOrganizationIntoDB(ctx, organization);
      } catch (IOException e) {
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
  public Organization updateOrganization(Organization organization) throws IOException {
    database.transaction(ctx -> {
      try {
        updateOrganizationInDB(ctx, organization);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return null;
    });
    return organization;
  }

  private void updateOrganizationInDB(final DSLContext ctx, Organization organization) throws IOException {
    final OffsetDateTime timestamp = OffsetDateTime.now();

    final boolean isExistingConfig = ctx.fetchExists(select()
        .from(ORGANIZATION)
        .where(ORGANIZATION.ID.eq(organization.getOrganizationId())));

    if (!isExistingConfig) {
      throw new IOException("Organization with id " + organization.getOrganizationId() + " does not exist.");
    }
    ctx.update(ORGANIZATION)
        .set(ORGANIZATION.NAME, organization.getName())
        .set(ORGANIZATION.UPDATED_AT, timestamp)
        .where(ORGANIZATION.ID.eq(organization.getOrganizationId()))
        .execute();
  }

  private void insertOrganizationIntoDB(final DSLContext ctx, Organization organization) throws IOException {
    final OffsetDateTime timestamp = OffsetDateTime.now();

    final boolean isExistingConfig = ctx.fetchExists(select()
        .from(ORGANIZATION)
        .where(ORGANIZATION.ID.eq(organization.getOrganizationId())));

    if (isExistingConfig) {
      throw new IOException("Organization with id " + organization.getOrganizationId() + " already exists.");
    }
    ctx.insertInto(ORGANIZATION)
        .set(ORGANIZATION.ID, organization.getOrganizationId())
        .set(ORGANIZATION.NAME, organization.getName())
        .set(ORGANIZATION.EMAIL, organization.getEmail())
        .set(ORGANIZATION.CREATED_AT, timestamp)
        .set(ORGANIZATION.UPDATED_AT, timestamp)
        .execute();

  }

  private Organization createOrganizationFromRecord(final Record record) {
    return new Organization().withOrganizationId(record.get(ORGANIZATION.ID)).withName(record.get(ORGANIZATION.NAME))
        .withEmail(record.get(ORGANIZATION.EMAIL))
        .withUserId(record.get(ORGANIZATION.USER_ID));
  }

}
