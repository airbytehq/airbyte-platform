/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import io.airbyte.api.server.generated.models.Pagination

/**
 * Helper to get data out of pagination objects.
 */
object PaginationHelper {
  private const val DEFAULT_PAGE_SIZE = 20
  private const val DEFAULT_ROW_OFFSET = 0

  @JvmStatic
  fun rowOffset(pagination: Pagination?): Int = pagination?.rowOffset ?: DEFAULT_ROW_OFFSET

  @JvmStatic
  fun pageSize(pagination: Pagination?): Int = pagination?.pageSize ?: DEFAULT_PAGE_SIZE
}
