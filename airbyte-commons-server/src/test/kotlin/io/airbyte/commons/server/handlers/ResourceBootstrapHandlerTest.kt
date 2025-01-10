package io.airbyte.commons.server.handlers

import io.airbyte.commons.server.authorization.ApiAuthorizationHelper
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthenticatedUser
import io.airbyte.config.OrganizationPaymentConfig
import io.airbyte.config.Permission
import io.airbyte.data.services.OrganizationPaymentConfigService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.PermissionService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.featureflag.FeatureFlagClient
import io.kotest.assertions.asClue
import io.kotest.matchers.shouldBe
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
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
  private val permissionService: PermissionService = mockk()
  private val currentUserService: CurrentUserService = mockk()
  private val apiAuthorizationHelper: ApiAuthorizationHelper = mockk()
  private val featureFlagClient: FeatureFlagClient = mockk()
  private val organizationPaymentConfigService: OrganizationPaymentConfigService = mockk()

  private val handler =
    ResourceBootstrapHandler(
      uuidSupplier,
      workspaceService,
      organizationService,
      permissionService,
      currentUserService,
      apiAuthorizationHelper,
      featureFlagClient,
      organizationPaymentConfigService,
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
      every { permissionService.createPermission(any()) } returns mockk<Permission>()
    }

    @Test
    fun `creates organization with organization payment config`() {
      val spy = spyk(handler)
      val paymentConfigSlot = slot<OrganizationPaymentConfig>()

      every { spy.findExistingOrganization(any()) } returns null
      every { organizationService.writeOrganization(any()) } returns Unit
      every { organizationPaymentConfigService.savePaymentConfig(capture(paymentConfigSlot)) } returns Unit

      spy.findOrCreateOrganizationAndPermission(user)

      verify { organizationService.writeOrganization(any()) }
      verify { organizationPaymentConfigService.savePaymentConfig(any()) }

      paymentConfigSlot.captured.asClue {
        it.paymentStatus shouldBe OrganizationPaymentConfig.PaymentStatus.UNINITIALIZED
        it.organizationId shouldBe orgId
      }
    }
  }
}
