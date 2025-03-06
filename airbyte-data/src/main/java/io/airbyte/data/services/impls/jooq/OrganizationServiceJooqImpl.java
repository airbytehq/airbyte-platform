/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.ORGANIZATION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.WORKSPACE;
import static org.jooq.impl.DSL.select;

import io.airbyte.config.Organization;
import io.airbyte.data.services.OrganizationService;
import io.airbyte.db.Database;
import io.airbyte.db.ExceptionWrappingDatabase;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;
import org.jooq.Record;
import org.jooq.Result;

/**
 * Deprecated - use OrganizationServiceDataImpl instead. This class is not being deleted right now
 * because it is used in test database setup code that currently requires jooq. Once that code is
 * refactored to use Micronaut Data configuration instead, this class can be deleted.
 */
@Deprecated(forRemoval = true)
@Singleton
public class OrganizationServiceJooqImpl implements OrganizationService {

  private final ExceptionWrappingDatabase database;

  public OrganizationServiceJooqImpl(@Named("configDatabase") Database database) {
    this.database = new ExceptionWrappingDatabase(database);
  }

  /**
   * Get organization.
   *
   * @param organizationId id to use to find the organization
   * @return organization, if present.
   * @throws IOException - you never know when you IO
   */
  @Override
  public Optional<Organization> getOrganization(final UUID organizationId) throws IOException {
    final Result<Record> result;
    result = database.query(ctx -> ctx.select(ORGANIZATION.asterisk())
        .from(ORGANIZATION)
        .where(ORGANIZATION.ID.eq(organizationId))).fetch();

    return result.stream().findFirst().map(DbConverter::buildOrganization);
  }

  @Override
  public Optional<Organization> getOrganizationForWorkspaceId(UUID workspaceId) throws IOException {
    final Result<Record> result = database
        .query(ctx -> ctx.select(ORGANIZATION.asterisk()).from(ORGANIZATION).innerJoin(WORKSPACE).on(ORGANIZATION.ID.eq(WORKSPACE.ORGANIZATION_ID))
            .where(WORKSPACE.ID.eq(workspaceId)))
        .fetch();
    return result.stream().findFirst().map(DbConverter::buildOrganization);
  }

  /**
   * Write an Organization to the database.
   *
   * @param organization - The configuration of the organization
   * @throws IOException - you never know when you IO
   */
  @Override
  public void writeOrganization(final Organization organization) throws IOException {
    database.transaction(ctx -> {
      final OffsetDateTime timestamp = OffsetDateTime.now();
      final boolean isExistingConfig = ctx.fetchExists(select()
          .from(ORGANIZATION)
          .where(ORGANIZATION.ID.eq(organization.getOrganizationId())));

      if (isExistingConfig) {
        ctx.update(ORGANIZATION)
            .set(ORGANIZATION.ID, organization.getOrganizationId())
            .set(ORGANIZATION.NAME, organization.getName())
            .set(ORGANIZATION.EMAIL, organization.getEmail())
            .set(ORGANIZATION.USER_ID, organization.getUserId())
            .set(ORGANIZATION.UPDATED_AT, timestamp)
            .where(ORGANIZATION.ID.eq(organization.getOrganizationId()))
            .execute();
      } else {
        ctx.insertInto(ORGANIZATION)
            .set(ORGANIZATION.ID, organization.getOrganizationId())
            .set(ORGANIZATION.NAME, organization.getName())
            .set(ORGANIZATION.EMAIL, organization.getEmail())
            .set(ORGANIZATION.USER_ID, organization.getUserId())
            .set(WORKSPACE.CREATED_AT, timestamp)
            .set(WORKSPACE.UPDATED_AT, timestamp)
            .execute();
      }
      return null;
    });
  }

}
