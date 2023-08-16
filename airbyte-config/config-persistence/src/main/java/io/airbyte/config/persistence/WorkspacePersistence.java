/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.WORKSPACE;
import static org.jooq.impl.DSL.noCondition;

import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.persistence.ConfigRepository.ResourcesByOrganizationQueryPaginated;
import io.airbyte.db.Database;
import io.airbyte.db.ExceptionWrappingDatabase;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Persistence Interface for Workspace table.
 */
public class WorkspacePersistence {

  private final ExceptionWrappingDatabase database;

  public WorkspacePersistence(final Database database) {
    this.database = new ExceptionWrappingDatabase(database);
  }

  /**
   * List all workspaces owned by org id, returning result ordered by workspace name. Supports
   * pagination and keyword search.
   */
  public List<StandardWorkspace> listWorkspacesByOrganizationId(final ResourcesByOrganizationQueryPaginated query, Optional<String> keyword)
      throws IOException {
    return database.query(ctx -> ctx.select(WORKSPACE.asterisk())
        .from(WORKSPACE)
        .where(WORKSPACE.ORGANIZATION_ID.eq(query.organizationId()))
        .and(keyword.isPresent() ? WORKSPACE.NAME.containsIgnoreCase(keyword.get()) : noCondition())
        .orderBy(WORKSPACE.NAME.asc())
        .limit(query.pageSize())
        .offset(query.rowOffset())
        .fetch())
        .stream()
        .map(DbConverter::buildStandardWorkspace)
        .toList();
  }

}
