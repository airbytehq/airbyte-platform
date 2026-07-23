/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.helpers

import io.airbyte.api.problems.throwable.generated.InvalidRedirectUrlProblem
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

  @Test
  fun `it should require exact allowlisted URLs for webapp mode`() {
    assertThrows<InvalidRedirectUrlProblem> { OAuthHelper.validateWebappRedirectUrlAllowlisted(null) }
    assertThrows<InvalidRedirectUrlProblem> { OAuthHelper.validateWebappRedirectUrlAllowlisted("https://example.com/auth_flow") }
    assertThrows<InvalidRedirectUrlProblem> { OAuthHelper.validateWebappRedirectUrlAllowlisted("https://app.airbyte.ai/auth_flow/") }
    assertDoesNotThrow { OAuthHelper.validateWebappRedirectUrlAllowlisted("https://app.airbyte.ai/auth_flow") }
    assertDoesNotThrow { OAuthHelper.validateWebappRedirectUrlAllowlisted("http://localhost:3000/auth_flow") }
    assertDoesNotThrow {
      OAuthHelper.validateWebappRedirectUrlAllowlisted("https://api.airbyte.ai/api/v1/integrations/connectors/oauth/callback")
    }
    assertDoesNotThrow {
      OAuthHelper.validateWebappRedirectUrlAllowlisted("http://localhost:8000/api/v1/integrations/connectors/oauth/callback")
    }
  }
}
