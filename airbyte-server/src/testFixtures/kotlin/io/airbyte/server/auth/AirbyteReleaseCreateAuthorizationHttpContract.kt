/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.auth

import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.config.Permission
import io.airbyte.data.services.PermissionService
import io.airbyte.domain.models.release.AirbyteRelease
import io.airbyte.domain.models.release.AirbyteReleaseConflictException
import io.airbyte.domain.models.release.InvalidAirbyteReleaseException
import io.airbyte.domain.services.release.AirbyteReleaseService
import io.airbyte.server.apis.controllers.AirbyteReleaseApiController
import io.micronaut.runtime.server.EmbeddedServer
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

/** The route is served by `AirbyteReleaseWrappedController` in Cloud and answers 501 elsewhere, behind the same rule. */
abstract class AirbyteReleaseCreateAuthorizationHttpContract {
  protected open val cloud = false

  @Test
  fun `instance admins create releases in Cloud and get 501 in other editions`() {
    run(callerGrant = Permission.PermissionType.INSTANCE_ADMIN) { server, services ->
      if (cloud) createsInCloud(server, services.single()) else answersNotImplemented(server, services)
    }
  }

  @Test
  fun `non admin users, workers, data planes, embedded and scoped tokens are rejected before recording`() {
    run(callerGrant = Permission.PermissionType.ORGANIZATION_ADMIN) { server, services ->
      for (token in listOf("user", "internal", "dataplane", "workload", "embedded", "scoped:${UUID(0, 1)}")) {
        val response = post(server, body(), token)
        assertEquals(403, response.statusCode(), "$token: ${response.body()}")
      }
      assertEquals(401, post(server, body(), "").statusCode())

      services.forEach { verify(exactly = 0) { it.record(any()) } }
    }
  }

  private fun createsInCloud(
    server: EmbeddedServer,
    service: AirbyteReleaseService,
  ) {
    every { service.record(RELEASE) } returns RELEASE
    every { service.record(match { it.version == "bad" }) } throws InvalidAirbyteReleaseException("version must be non-blank")
    every { service.record(match { it.sourceCommit == OTHER_COMMIT }) } throws AirbyteReleaseConflictException(RELEASE.version)

    val recorded = post(server, body(), "user")
    assertEquals(200, recorded.statusCode(), recorded.body())
    assertTrue(recorded.body().contains("\"version\":\"2.1.0\""), recorded.body())
    assertTrue(recorded.body().contains("\"sourceCommit\":\"$COMMIT\""), recorded.body())

    val invalid = post(server, body(version = "bad"), "user")
    assertEquals(400, invalid.statusCode(), invalid.body())
    assertTrue(invalid.body().contains("version must be non-blank"), invalid.body())

    val conflict = post(server, body(sourceCommit = OTHER_COMMIT), "user")
    assertEquals(409, conflict.statusCode(), conflict.body())

    assertEquals(200, post(server, body(), "service").statusCode())
  }

  /** Outside Cloud the controller has no release service, so nothing can be recorded. */
  private fun answersNotImplemented(
    server: EmbeddedServer,
    services: List<AirbyteReleaseService>,
  ) {
    for (token in listOf("user", "service")) {
      val response = post(server, body(), token)
      assertEquals(501, response.statusCode(), "$token: ${response.body()}")
    }
    assertEquals(emptyList<AirbyteReleaseService>(), services)
  }

  private fun run(
    callerGrant: Permission.PermissionType,
    check: (EmbeddedServer, List<AirbyteReleaseService>) -> Unit,
  ) {
    val grants = mockk<PermissionService>()
    val permission = Permission().withPermissionType(callerGrant)
    if (callerGrant == Permission.PermissionType.ORGANIZATION_ADMIN) permission.organizationId = UUID(0, 2)
    every { grants.getPermissionsByAuthUserId("caller") } returns listOf(permission)
    every { grants.getPermissionsByServiceAccountId(UUID(0, 9)) } returns listOf(permission)

    RemainingOperationHttpSupport(
      cloud = cloud,
      controllers = listOf(controller()),
      realRules = setOf(AirbyteReleaseCreateRule::class.java.name),
      earlierAllow = false,
    ).run(
      configure = { context, _ ->
        context.registerSingleton(
          OperationPermissions::class.java,
          OperationPermissions(grants, RoleResolver(mockk(), mockk(), mockk(), mockk()), true),
        )
      },
      check = { context, server, business ->
        RemainingOperationHttpSupport.requireRule(context, "POST", PATH, AirbyteReleaseCreateRule::class.java.name)
        check(server, business.filterIsInstance<AirbyteReleaseService>())
      },
    )
  }

  private fun controller(): Class<*> =
    if (cloud) {
      Class.forName("io.airbyte.server.wrapped.apis.controllers.AirbyteReleaseWrappedController")
    } else {
      AirbyteReleaseApiController::class.java
    }

  private fun post(
    server: EmbeddedServer,
    body: String,
    token: String,
  ) = RemainingOperationHttpSupport.request(server, "POST", PATH, body, token)

  private fun body(
    version: String = RELEASE.version,
    sourceCommit: String = RELEASE.sourceCommit,
  ) = """{"version":"$version","sourceCommit":"$sourceCommit","publishedAt":"2026-11-06T17:30:00Z"}"""

  companion object {
    private const val PATH = "/api/v1/airbyte_releases/create"
    private const val COMMIT = "0123456789abcdef0123456789abcdef01234567"
    private const val OTHER_COMMIT = "89abcdef0123456789abcdef0123456789abcdef"
    private val RELEASE =
      AirbyteRelease(
        version = "2.1.0",
        sourceCommit = COMMIT,
        publishedAt = OffsetDateTime.of(2026, 11, 6, 17, 30, 0, 0, ZoneOffset.UTC),
      )
  }
}
