/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime

class ScimPaginationTest {
  @Test
  fun `normalizes defaults bounds zero and negative values`() {
    val resources = (1..250).map { resource(it) }.reversed()

    val defaultPage = ScimPagination.page(resources, null, null, Resource::createdAt, Resource::id)
    assertThat(defaultPage.resources).hasSize(100)
    assertThat(defaultPage.startIndex).isEqualTo(1)
    assertThat(defaultPage.itemsPerPage).isEqualTo(100)
    assertThat(defaultPage.totalResults).isEqualTo(250)

    assertThat(ScimPagination.page(resources, 0, 500, Resource::createdAt, Resource::id).resources).hasSize(200)
    assertThat(ScimPagination.page(resources, 1, 0, Resource::createdAt, Resource::id).resources).isEmpty()
    assertThat(ScimPagination.page(resources, 1, -5, Resource::createdAt, Resource::id).resources).isEmpty()
    assertThat(ScimPagination.page(resources, 1, -5, Resource::createdAt, Resource::id).totalResults).isEqualTo(250)
  }

  @Test
  fun `uses stable created-at then id order and one-based offsets`() {
    val timestamp = OffsetDateTime.parse("2026-07-16T00:00:00Z")
    val resources =
      listOf(
        Resource("c", timestamp.plusSeconds(1)),
        Resource("b", timestamp),
        Resource("a", timestamp),
      )

    val page = ScimPagination.page(resources, 2, 2, Resource::createdAt, Resource::id)

    assertThat(page.resources.map(Resource::id)).containsExactly("b", "c")
    assertThat(page.startIndex).isEqualTo(2)
    assertThat(page.itemsPerPage).isEqualTo(2)
  }

  @Test
  fun `beyond-end page preserves normalized request metadata`() {
    val page = ScimPagination.page(listOf(resource(1)), 99, 10, Resource::createdAt, Resource::id)

    assertThat(page.resources).isEmpty()
    assertThat(page.totalResults).isEqualTo(1)
    assertThat(page.startIndex).isEqualTo(99)
    assertThat(page.itemsPerPage).isZero()
  }

  @Test
  fun `filters before applying one-based pagination`() {
    val resources = (1..6).map { resource(it) }

    val page =
      ScimPagination.page(
        resources = resources,
        filter = { it.id.toInt() % 2 == 0 },
        startIndex = 2,
        count = 1,
        createdAt = Resource::createdAt,
        id = Resource::id,
      )

    assertThat(page.resources.map(Resource::id)).containsExactly("004")
    assertThat(page.totalResults).isEqualTo(3)
    assertThat(page.startIndex).isEqualTo(2)
    assertThat(page.itemsPerPage).isEqualTo(1)
  }

  private fun resource(index: Int): Resource =
    Resource(index.toString().padStart(3, '0'), OffsetDateTime.parse("2026-07-16T00:00:00Z").plusSeconds(index.toLong()))

  private data class Resource(
    val id: String,
    val createdAt: OffsetDateTime,
  )
}
