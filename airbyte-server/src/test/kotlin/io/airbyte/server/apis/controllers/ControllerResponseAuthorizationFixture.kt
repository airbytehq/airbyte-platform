/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.server.auth.AuthorizedRequest
import io.micronaut.aop.MethodInvocationContext
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Requires
import io.mockk.every
import io.mockk.mockk
import jakarta.inject.Singleton

/** Controller response tests leave authorization policy to the HTTP contract tests. */
@Factory
@Requires(property = "controller.response.authorization.fixture", value = "true")
internal class ControllerResponseAuthorizationFixture {
  @Singleton
  @Primary
  fun authorizedRequest(): AuthorizedRequest =
    mockk(relaxed = true) {
      every { execute(any()) } answers { firstArg<MethodInvocationContext<Any, Any>>().proceed() }
    }
}
