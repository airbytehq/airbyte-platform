package io.airbyte.server.apis.publicapi.helpers

import io.airbyte.commons.server.errors.problems.InvalidRedirectUrlProblem
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows

class OAuthHelperTest {
  @Test
  fun `it should reject invalid redirect URLs for our initiateOAuth endpoint`() {
    assertThrows<InvalidRedirectUrlProblem> { OAuthHelper.validateRedirectUrl(null) }
    assertThrows<InvalidRedirectUrlProblem> { OAuthHelper.validateRedirectUrl("http://example.com") }
    assertThrows<InvalidRedirectUrlProblem> { OAuthHelper.validateRedirectUrl("this isn't a URL") }
    assertDoesNotThrow { OAuthHelper.validateRedirectUrl("https://test-site.com/path/and/stuff?query=params") }
  }
}
