/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

data class ScimPage<T>(
  val resources: List<T>,
  val totalResults: Int,
  val startIndex: Int,
  val itemsPerPage: Int,
)

object ScimPagination {
  const val DEFAULT_COUNT = 100
  const val MAX_COUNT = 200

  fun <T, C : Comparable<C>> page(
    resources: Iterable<T>,
    startIndex: Int?,
    count: Int?,
    createdAt: (T) -> C,
    id: (T) -> String,
  ): ScimPage<T> = page(resources, { true }, startIndex, count, createdAt, id)

  fun <T, C : Comparable<C>> page(
    resources: Iterable<T>,
    filter: (T) -> Boolean,
    startIndex: Int?,
    count: Int?,
    createdAt: (T) -> C,
    id: (T) -> String,
  ): ScimPage<T> {
    val normalizedStartIndex = (startIndex ?: 1).coerceAtLeast(1)
    val normalizedCount = (count ?: DEFAULT_COUNT).coerceIn(0, MAX_COUNT)
    val sorted = resources.filter(filter).sortedWith(compareBy(createdAt, id))
    val offset = normalizedStartIndex.toLong() - 1
    val pageResources =
      if (normalizedCount == 0 || offset >= sorted.size) {
        emptyList()
      } else {
        sorted.drop(offset.toInt()).take(normalizedCount)
      }
    return ScimPage(
      resources = pageResources,
      totalResults = sorted.size,
      startIndex = normalizedStartIndex,
      itemsPerPage = pageResources.size,
    )
  }
}
