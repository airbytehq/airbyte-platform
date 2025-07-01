/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.config.Permission
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.OrganizationPaymentConfigService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.WorkspaceService
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.UUID
import java.util.function.Supplier

class ResourceBootstrapHandlerTest {
  private val orgId = UUID.randomUUID()
  private val uuidSupplier: Supplier<UUID> = Supplier { orgId }
  private val workspaceService: WorkspaceService = mockk()
  private val organizationService: OrganizationService = mockk()
  private val permissionHandler: PermissionHandler = mockk()
  private val currentUserService: CurrentUserService = mockk()
  private val organizationPaymentConfigService: OrganizationPaymentConfigService = mockk()
  private val dataplaneGroupService: DataplaneGroupService = mockk()
  private val roleResolver: RoleResolver = mockk()

  private val handler =
    ResourceBootstrapHandler(
      uuidSupplier,
      workspaceService,
      organizationService,
      permissionHandler,
      currentUserService,
      roleResolver,
      organizationPaymentConfigService,
      AirbyteEdition.COMMUNITY,
      dataplaneGroupService,
    )

  @Nested
  inner class `findOrCreateOrganizationAndPermission` {
    private val user: AuthenticatedUser = mockk()

    @BeforeEach
    fun setup() {
      user.let {
        every { it.userId } returns UUID.randomUUID()
        every { it.email } returns "test@airbyte.io"
        every { it.companyName } returns "Airbyte"
      }
      every { permissionHandler.createPermission(any()) } returns mockk<Permission>()
    }

    @Test
    fun `creates organization with organization payment config`() {
      val spy = spyk(handler)

      every { spy.findExistingOrganization(any()) } returns null
      every { organizationService.writeOrganization(any()) } returns Unit
      every { organizationPaymentConfigService.saveDefaultPaymentConfig(any()) } returns Unit

      spy.findOrCreateOrganizationAndPermission(user)

      verify { organizationService.writeOrganization(any()) }
      verify { organizationPaymentConfigService.saveDefaultPaymentConfig(any()) }

      // We no longer need to test the payment config saved because default config is always the same
    }
  }
}
