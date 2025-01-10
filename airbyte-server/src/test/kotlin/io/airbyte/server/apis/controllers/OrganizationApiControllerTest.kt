/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.OrganizationCreateRequestBody
import io.airbyte.api.model.generated.OrganizationIdRequestBody
import io.airbyte.api.model.generated.OrganizationRead
import io.airbyte.api.model.generated.OrganizationUpdateRequestBody
import io.airbyte.commons.server.handlers.OrganizationsHandler
import io.airbyte.server.assertStatus
import io.airbyte.server.status
import io.micronaut.context.annotation.Requires
import io.micronaut.context.env.Environment
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.test.annotation.MockBean
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import jakarta.inject.Inject
import org.junit.jupiter.api.Test

@MicronautTest
@Requires(env = [Environment.TEST])
internal class OrganizationApiControllerTest {
  @Inject
  lateinit var organizationsHandler: OrganizationsHandler

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @MockBean(OrganizationsHandler::class)
  fun organizationsHandler(): OrganizationsHandler = mockk()

  @Test
  fun testGetOrganization() {
    every { organizationsHandler.getOrganization(any()) } returns OrganizationRead()

    val path = "/api/v1/organizations/get"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, OrganizationIdRequestBody())))
  }

  @Test
  @Throws(Exception::class)
  fun testUpdateOrganization() {
    every { organizationsHandler.updateOrganization(any()) } returns OrganizationRead()

    val path = "/api/v1/organizations/update"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, OrganizationUpdateRequestBody())))
  }

  @Test
  @Throws(Exception::class)
  fun testCreateOrganization() {
    every { organizationsHandler.createOrganization(any()) } returns OrganizationRead()

    val path = "/api/v1/organizations/create"
    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, OrganizationCreateRequestBody())))
  }
}
