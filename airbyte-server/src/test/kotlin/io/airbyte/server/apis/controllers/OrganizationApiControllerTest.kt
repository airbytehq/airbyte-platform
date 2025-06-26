/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.OrganizationCreateRequestBody
import io.airbyte.api.model.generated.OrganizationIdRequestBody
import io.airbyte.api.model.generated.OrganizationRead
import io.airbyte.api.model.generated.OrganizationUpdateRequestBody
import io.airbyte.commons.server.handlers.OrganizationsHandler
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

class OrganizationApiControllerTest {
  private lateinit var organizationsHandler: OrganizationsHandler
  private lateinit var organizationApiController: OrganizationApiController

  @BeforeEach
  fun setup() {
    organizationsHandler = mockk()
    organizationApiController = OrganizationApiController(organizationsHandler)
  }

  @Test
  fun testGetOrganization() {
    every { organizationsHandler.getOrganization(any()) } returns OrganizationRead()
    val body = OrganizationIdRequestBody().organizationId(UUID.randomUUID())
    assertNotNull(organizationApiController.getOrganization(body))
  }

  @Test
  fun testUpdateOrganization() {
    every { organizationsHandler.updateOrganization(any()) } returns OrganizationRead()
    assertNotNull(organizationApiController.updateOrganization(OrganizationUpdateRequestBody()))
  }

  @Test
  fun testCreateOrganization() {
    every { organizationsHandler.createOrganization(any()) } returns OrganizationRead()
    assertNotNull(organizationApiController.createOrganization(OrganizationCreateRequestBody()))
  }
}
