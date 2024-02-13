/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server.mappers

import io.micronaut.core.util.CollectionUtils
import io.micronaut.http.uri.UriBuilder
import java.util.Optional
import java.util.UUID

/**
 * Pagination mapper for easily creating pagination previous/next strings.
 */
object PaginationMapper {
  const val LIMIT = "limit"
  const val OFFSET = "offset"

  /**
   * Base URI builder we need to create.
   *
   * @param publicApiHost public API host
   * @param path path of the endpoint for which you want to build pagination URLs
   * @return URL builder for the base URL
   */
  fun getBuilder(
    publicApiHost: String,
    path: String,
  ): UriBuilder {
    return UriBuilder.of(publicApiHost).path(path)
  }

  /**
   * Get next offset.
   *
   * @param collection collection of items we just got
   * @param limit current limit
   * @param offset current offset
   * @return offset or null which indicates we have no next offset to provide
   */
  fun getNextOffset(
    collection: Collection<*>,
    limit: Int,
    offset: Int,
  ): Optional<Int> {
    // If we have no more entries or we had no entries this page, just return empty - no next URL
    return if (CollectionUtils.isEmpty(collection) || collection.size < limit) {
      Optional.empty()
    } else {
      Optional.of(offset + limit)
    }
  }

  /**
   * Gets previous offset based on passed in limit and offset.
   *
   * @param limit current limit
   * @param offset current offset
   * @return new "previous" offset
   */
  fun getPreviousOffset(
    limit: Int,
    offset: Int,
  ): Int {
    val previousOffset = offset - limit
    return Math.max(previousOffset, 0)
  }

  /**
   * Get the full next URL.
   *
   * @param collection list of things we just got from the endpoint.
   * @param limit current limit
   * @param offset current offset
   * @param uriBuilder the URL builder created from getBuilder
   * @return a String URL that can be put into the response.
   */
  fun getNextUrl(
    collection: Collection<*>,
    limit: Int,
    offset: Int,
    uriBuilder: UriBuilder,
  ): String {
    val nextOffset = getNextOffset(collection, limit, offset)
    return if (nextOffset.isPresent) {
      uriBuilder.queryParam(LIMIT, limit)
        .replaceQueryParam(OFFSET, nextOffset.get()).toString()
    } else {
      ""
    }
  }

  /**
   * Get the full previous URL.
   *
   * @param limit current limit
   * @param offset current offset
   * @param uriBuilder the URL builder created from getBuilder
   * @return a String URL that can be put into the response.
   */
  fun getPreviousUrl(
    limit: Int,
    offset: Int,
    uriBuilder: UriBuilder,
  ): String {
    return if (offset != 0) {
      uriBuilder.queryParam(LIMIT, limit).replaceQueryParam(
        OFFSET,
        getPreviousOffset(limit, offset),
      ).toString()
    } else {
      ""
    }
  }

  /**
   * Helper to turn a list of UUIDs into a pagination acceptable string.
   *
   * @param uuids uuids to stringify
   */
  fun uuidListToQueryString(uuids: List<UUID>): String {
    return java.lang.String.join(",", uuids.stream().map { obj: UUID -> obj.toString() }.toList())
  }
}
