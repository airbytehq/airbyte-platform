/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import io.airbyte.api.model.generated.Pagination;

/**
 * Helper to get data out of pagination objects.
 */
public class PaginationHelper {

  private static final int DEFAULT_PAGE_SIZE = 20;
  private static final int DEFAULT_ROW_OFFSET = 0;

  public static int rowOffset(Pagination pagination) {
    return (pagination != null && pagination.getRowOffset() != null) ? pagination.getRowOffset() : DEFAULT_ROW_OFFSET;
  }

  public static int pageSize(Pagination pagination) {
    return (pagination != null && pagination.getPageSize() != null) ? pagination.getPageSize() : DEFAULT_PAGE_SIZE;
  }

}
