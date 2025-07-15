/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import io.airbyte.api.model.generated.Pagination

/**
 * Helper to get data out of pagination objects.
 */
object PaginationHelper {
  private const val DEFAULT_PAGE_SIZE = 20
  private const val DEFAULT_ROW_OFFSET = 0

  @JvmStatic
  fun rowOffset(pagination: Pagination?): Int = if (pagination != null && pagination.rowOffset != null) pagination.rowOffset else DEFAULT_ROW_OFFSET

  @JvmStatic
  fun pageSize(pagination: Pagination?): Int = if (pagination != null && pagination.pageSize != null) pagination.pageSize else DEFAULT_PAGE_SIZE
}
