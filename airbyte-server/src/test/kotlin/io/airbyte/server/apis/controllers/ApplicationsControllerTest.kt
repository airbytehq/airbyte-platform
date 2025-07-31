/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.ApplicationTokenRequest
import io.airbyte.api.problems.throwable.generated.RequestTimeoutExceededProblem
import io.airbyte.data.services.ApplicationService
import io.micronaut.context.ApplicationContext
import io.micronaut.http.HttpStatus
import io.mockk.Awaits
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows

internal class ApplicationsControllerTest {
  @Test
  fun testTokenRequestTimeout() {
    val applicationService: ApplicationService =
      mockk {
        every { getToken(any(), any()) } just Awaits
      }
    val applicationTokenRequest =
      mockk<ApplicationTokenRequest> {
        every { clientId } returns "clientId"
        every { clientSecret } returns "clientSecret"
      }
    val applicationContext = ApplicationContext.run()
    applicationContext.registerSingleton(applicationService)
    val applicationsController = applicationContext.getBean(ApplicationsController::class.java)
    val e =
      assertThrows<RequestTimeoutExceededProblem> {
        applicationsController.applicationTokenRequest(applicationTokenRequest)
      }
    assertEquals(mapOf("timeout" to "PT1S"), e.problem.getData())
    assertEquals(HttpStatus.REQUEST_TIMEOUT.code, e.problem.getStatus())
    assertEquals("The request has exceeded the timeout associated with this endpoint.", e.problem.getDetail())
    assertEquals("error:request-timeout-exceeded", e.problem.getType())
    assertEquals("Request timeout exceeded", e.problem.getTitle())
    applicationContext.close()
  }

  @Test
  fun testTokenRequestNoTimeout() {
    val token = "an-access-token"
    val applicationService: ApplicationService =
      mockk {
        every { getToken(any(), any()) } returns token
      }
    val applicationTokenRequest =
      mockk<ApplicationTokenRequest> {
        every { clientId } returns "clientId"
        every { clientSecret } returns "clientSecret"
      }
    val applicationContext = ApplicationContext.run()
    applicationContext.registerSingleton(applicationService)
    val applicationsController = applicationContext.getBean(ApplicationsController::class.java)
    assertDoesNotThrow {
      val accessToken = applicationsController.applicationTokenRequest(applicationTokenRequest)
      assertEquals(token, accessToken.accessToken)
    }
    applicationContext.close()
  }
}
