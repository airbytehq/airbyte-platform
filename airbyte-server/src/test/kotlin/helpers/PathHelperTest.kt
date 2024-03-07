package helpers

import io.airbyte.server.apis.publicapi.helpers.removePublicApiPathPrefix
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class PathHelperTest {
  @Test
  fun testRemovePublicApiPathPrefix() {
    assertEquals(
      "/v1/health",
      removePublicApiPathPrefix("/api/public/v1/health"),
    )
  }
}
