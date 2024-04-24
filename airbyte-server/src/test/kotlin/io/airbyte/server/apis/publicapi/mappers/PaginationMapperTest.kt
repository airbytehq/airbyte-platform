package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.public_api.model.generated.SourceDefinitionRead.SourceTypeEnum
import io.airbyte.server.apis.publicapi.constants.SOURCES_PATH
import io.airbyte.server.apis.publicapi.helpers.removePublicApiPathPrefix
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class PaginationMapperTest {
  private val publicApiHost = "https://api.airbyte.com"

  @Test
  fun `test that it outputs a correct URL`() {
    val paginationMapper =
      PaginationMapper.getBuilder(publicApiHost, removePublicApiPathPrefix(SOURCES_PATH))
        .queryParam("string", "string")
        .queryParam("int", 1)
        .queryParam("enum", SourceTypeEnum.API)

    assertEquals("$publicApiHost/v1/sources?string=string&int=1&enum=api", paginationMapper.build().toString())
  }

  @Test
  fun `test that it can generate next URLs`() {
    val lessResultsBuilder = PaginationMapper.getBuilder(publicApiHost, removePublicApiPathPrefix(SOURCES_PATH))
    PaginationMapper.getNextUrl(listOf("a", "b", "c"), 4, 0, lessResultsBuilder)
    assertEquals(
      "$publicApiHost/v1/sources?limit=4&offset=4",
      lessResultsBuilder.build().toString(),
    )

    val noResultsBuilder = PaginationMapper.getBuilder(publicApiHost, removePublicApiPathPrefix(SOURCES_PATH))
    PaginationMapper.getNextUrl(emptyList<Any>(), 4, 0, noResultsBuilder)
    assertEquals(
      "$publicApiHost/v1/sources",
      noResultsBuilder.build().toString(),
    )

    val offsetLimitBuilder = PaginationMapper.getBuilder(publicApiHost, removePublicApiPathPrefix(SOURCES_PATH))
    PaginationMapper.getNextUrl(listOf("a", "b", "c"), 2, 0, offsetLimitBuilder)
    assertEquals(
      "$publicApiHost/v1/sources?limit=2&offset=2",
      offsetLimitBuilder.build().toString(),
    )
  }

  @Test
  fun `test that it can generate prev URLs`() {
    var prevPageBuilder = PaginationMapper.getBuilder(publicApiHost, removePublicApiPathPrefix(SOURCES_PATH))
    PaginationMapper.getPreviousUrl(4, 8, prevPageBuilder)
    assertEquals(
      "$publicApiHost/v1/sources?limit=4&offset=4",
      prevPageBuilder.build().toString(),
    )

    var noPrevPageBuilder = PaginationMapper.getBuilder(publicApiHost, removePublicApiPathPrefix(SOURCES_PATH))
    PaginationMapper.getPreviousUrl(2, 0, noPrevPageBuilder)
    assertEquals(
      "$publicApiHost/v1/sources",
      noPrevPageBuilder.build().toString(),
    )
  }

  @Test
  fun `uuid list to qs`() {
    val uuids = listOf(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID())
    val (first, second, third) = uuids
    assertEquals("$first,$second,$third", PaginationMapper.uuidListToQueryString(uuids))

    assertEquals("", PaginationMapper.uuidListToQueryString(emptyList()))
  }
}
