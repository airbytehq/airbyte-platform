/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.ORGANIZATION;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.WORKSPACE;
import static org.jooq.impl.DSL.noCondition;
import static org.jooq.impl.DSL.select;

import io.airbyte.config.Organization;
import io.airbyte.data.services.OrganizationService;
import io.airbyte.data.services.shared.ResourcesByOrganizationQueryPaginated;
import io.airbyte.db.Database;
import io.airbyte.db.ExceptionWrappingDatabase;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.jooq.Record;
import org.jooq.Result;

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
            .set(ORGANIZATION.PBA, organization.getPba())
            .set(ORGANIZATION.ORG_LEVEL_BILLING, organization.getOrgLevelBilling())
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
            .set(ORGANIZATION.PBA, organization.getPba())
            .set(ORGANIZATION.ORG_LEVEL_BILLING, organization.getOrgLevelBilling())
            .execute();
      }
      return null;
    });
  }

  /**
   * List organizations.
   *
   * @return organizations
   * @throws IOException - you never know when you IO
   */
  @Override
  public List<Organization> listOrganizations() throws IOException {
    return listOrganizationQuery(Optional.empty()).toList();
  }

  /**
   * List organizations (paginated).
   *
   * @param resourcesByOrganizationQueryPaginated - contains all the information we need to paginate
   * @return A List of organizations objects
   * @throws IOException you never know when you IO
   */
  @Override
  public List<Organization> listOrganizationsPaginated(final ResourcesByOrganizationQueryPaginated resourcesByOrganizationQueryPaginated)
      throws IOException {
    return database.query(ctx -> ctx.select(ORGANIZATION.asterisk())
        .from(ORGANIZATION)
        .where(ORGANIZATION.ID.in(resourcesByOrganizationQueryPaginated.organizationId()))
        .limit(resourcesByOrganizationQueryPaginated.pageSize())
        .offset(resourcesByOrganizationQueryPaginated.rowOffset())
        .fetch())
        .stream()
        .map(DbConverter::buildOrganization)
        .toList();
  }

  private Stream<Organization> listOrganizationQuery(final Optional<UUID> organizationId) throws IOException {
    return database.query(ctx -> ctx.select(ORGANIZATION.asterisk())
        .from(ORGANIZATION)
        .where(organizationId.map(ORGANIZATION.ID::eq).orElse(noCondition()))
        .fetch())
        .stream()
        .map(DbConverter::buildOrganization);
  }

}
