package io.airbyte.commons.server.services

import io.airbyte.analytics.BillingTrackingHelper
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.api.problems.throwable.generated.StateConflictProblem
import io.airbyte.commons.server.ConnectionId
import io.airbyte.commons.server.OrganizationId
import io.airbyte.config.OrganizationPaymentConfig
import io.airbyte.config.OrganizationPaymentConfig.PaymentStatus
import io.airbyte.config.OrganizationPaymentConfig.SubscriptionStatus
import io.airbyte.data.services.shared.ConnectionAutoDisabledReason
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.UUID
import io.airbyte.data.services.ConnectionService as ConnectionRepository
import io.airbyte.data.services.OrganizationPaymentConfigService as OrganizationPaymentConfigRepository

class OrganizationServiceTest {
  private val connectionService: ConnectionService = mockk()
  private val connectionRepository: ConnectionRepository = mockk()
  private val organizationPaymentConfigRepository: OrganizationPaymentConfigRepository = mockk()
  private val billingTrackingHelper: BillingTrackingHelper = mockk()

  private val service =
    OrganizationServiceImpl(
      connectionService,
      connectionRepository,
      organizationPaymentConfigRepository,
      billingTrackingHelper,
    )

  private val organizationId = OrganizationId(UUID.randomUUID())
  private val connectionId1 = ConnectionId(UUID.randomUUID())
  private val connectionId2 = ConnectionId(UUID.randomUUID())
  private val connectionId3 = ConnectionId(UUID.randomUUID())

  @Nested
  inner class DisableAllConnections {
    @Test
    fun `should call connectionService to disable connections`() {
      every { connectionRepository.listConnectionIdsForOrganization(organizationId.value) } returns
        listOf(connectionId1.value, connectionId2.value, connectionId3.value)
      every { connectionService.disableConnections(any(), any()) } returns mockk()

      service.disableAllConnections(organizationId, null)

      verify { connectionService.disableConnections(setOf(connectionId1, connectionId2, connectionId3), null) }
    }
  }

  @Nested
  inner class HandlePaymentGracePeriodEnded {
    @Test
    fun `should throw if orgPaymentConfig is not found`() {
      every { organizationPaymentConfigRepository.findByOrganizationId(organizationId.value) } returns null
      shouldThrow<ResourceNotFoundProblem> { service.handlePaymentGracePeriodEnded(organizationId) }
    }

    @Test
    fun `should throw if organization is not in a grace period`() {
      every { organizationPaymentConfigRepository.findByOrganizationId(organizationId.value) } returns
        OrganizationPaymentConfig().apply { paymentStatus = PaymentStatus.OKAY }
      shouldThrow<StateConflictProblem> { service.handlePaymentGracePeriodEnded(organizationId) }
    }

    @Test
    fun `should update orgPaymentConfig status to disabled`() {
      val orgPaymentConfig =
        OrganizationPaymentConfig().apply {
          paymentStatus = PaymentStatus.GRACE_PERIOD
          paymentProviderId = "provider-id-1"
        }
      val orgPaymentConfigSlot = slot<OrganizationPaymentConfig>()

      every { organizationPaymentConfigRepository.findByOrganizationId(organizationId.value) } returns orgPaymentConfig
      every { organizationPaymentConfigRepository.savePaymentConfig(capture(orgPaymentConfigSlot)) } just Runs
      every { connectionRepository.listConnectionIdsForOrganization(organizationId.value) } returns emptyList()
      every { connectionService.disableConnections(any(), any()) } returns mockk()
      every { billingTrackingHelper.trackGracePeriodEnded(any(), any()) } returns Unit

      service.handlePaymentGracePeriodEnded(organizationId)

      verify { organizationPaymentConfigRepository.savePaymentConfig(orgPaymentConfig) }
      orgPaymentConfigSlot.captured.paymentStatus shouldBe PaymentStatus.DISABLED
    }

    @Test
    fun `should call disableAllConnections with invalid payment method reason`() {
      val orgPaymentConfig =
        OrganizationPaymentConfig().apply {
          paymentStatus = PaymentStatus.GRACE_PERIOD
          paymentProviderId = "provider-id-1"
        }
      every { organizationPaymentConfigRepository.findByOrganizationId(organizationId.value) } returns orgPaymentConfig
      every { organizationPaymentConfigRepository.savePaymentConfig(orgPaymentConfig) } just Runs
      every { connectionRepository.listConnectionIdsForOrganization(organizationId.value) } returns
        listOf(connectionId1.value, connectionId2.value, connectionId3.value)
      every { connectionService.disableConnections(any(), any()) } returns mockk()
      every { billingTrackingHelper.trackGracePeriodEnded(any(), any()) } returns Unit

      service.handlePaymentGracePeriodEnded(organizationId)

      verify {
        connectionService.disableConnections(
          setOf(connectionId1, connectionId2, connectionId3),
          ConnectionAutoDisabledReason.INVALID_PAYMENT_METHOD,
        )
      }
    }

    @Test
    fun `should track grace period ended`() {
      val orgPaymentConfig =
        OrganizationPaymentConfig().apply {
          paymentStatus = PaymentStatus.GRACE_PERIOD
          paymentProviderId = "provider-id-1"
        }
      every { organizationPaymentConfigRepository.findByOrganizationId(organizationId.value) } returns orgPaymentConfig
      every { organizationPaymentConfigRepository.savePaymentConfig(orgPaymentConfig) } just Runs
      every { connectionRepository.listConnectionIdsForOrganization(organizationId.value) } returns emptyList()
      every { connectionService.disableConnections(any(), any()) } returns mockk()
      every { billingTrackingHelper.trackGracePeriodEnded(any(), any()) } returns Unit

      service.handlePaymentGracePeriodEnded(organizationId)

      verify { billingTrackingHelper.trackGracePeriodEnded(organizationId.value, orgPaymentConfig.paymentProviderId) }
    }
  }

  @Nested
  inner class HandleUncollectibleInvoice {
    @Test
    fun `should throw if orgPaymentConfig is not found`() {
      every { organizationPaymentConfigRepository.findByOrganizationId(organizationId.value) } returns null
      shouldThrow<ResourceNotFoundProblem> { service.handleUncollectibleInvoice(organizationId) }
    }

    @Test
    fun `should update orgPaymentConfig status to locked`() {
      val orgPaymentConfig = OrganizationPaymentConfig().apply { paymentStatus = PaymentStatus.OKAY }
      val orgPaymentConfigSlot = slot<OrganizationPaymentConfig>()

      every { organizationPaymentConfigRepository.findByOrganizationId(organizationId.value) } returns orgPaymentConfig
      every { organizationPaymentConfigRepository.savePaymentConfig(capture(orgPaymentConfigSlot)) } just Runs
      every { connectionRepository.listConnectionIdsForOrganization(organizationId.value) } returns emptyList()
      every { connectionService.disableConnections(any(), any()) } returns mockk()

      service.handleUncollectibleInvoice(organizationId)

      verify { organizationPaymentConfigRepository.savePaymentConfig(orgPaymentConfig) }
      orgPaymentConfigSlot.captured.paymentStatus shouldBe PaymentStatus.LOCKED
    }

    @Test
    fun `should call disableAllConnections with uncollectible invoice reason`() {
      val orgPaymentConfig = OrganizationPaymentConfig().apply { paymentStatus = PaymentStatus.GRACE_PERIOD }
      every { organizationPaymentConfigRepository.findByOrganizationId(organizationId.value) } returns orgPaymentConfig
      every { organizationPaymentConfigRepository.savePaymentConfig(orgPaymentConfig) } just Runs
      every { connectionRepository.listConnectionIdsForOrganization(organizationId.value) } returns
        listOf(connectionId1.value, connectionId2.value, connectionId3.value)
      every { connectionService.disableConnections(any(), any()) } returns mockk()

      service.handleUncollectibleInvoice(organizationId)

      verify {
        connectionService.disableConnections(
          setOf(connectionId1, connectionId2, connectionId3),
          ConnectionAutoDisabledReason.INVOICE_MARKED_UNCOLLECTIBLE,
        )
      }
    }
  }

  @Nested
  inner class HandleSubscriptionStarted {
    @Test
    fun `should throw if orgPaymentConfig is not found`() {
      every { organizationPaymentConfigRepository.findByOrganizationId(organizationId.value) } returns null
      shouldThrow<ResourceNotFoundProblem> { service.handleSubscriptionStarted(organizationId) }
    }

    @Test
    fun `should no-op if already subscribed`() {
      val orgPaymentConfig =
        OrganizationPaymentConfig().apply {
          subscriptionStatus = SubscriptionStatus.SUBSCRIBED
        }

      every { organizationPaymentConfigRepository.findByOrganizationId(organizationId.value) } returns orgPaymentConfig

      service.handleSubscriptionStarted(organizationId)

      verify(exactly = 0) { organizationPaymentConfigRepository.savePaymentConfig(any()) }
      verify(exactly = 0) { connectionService.disableConnections(any(), any()) }
    }

    @Test
    fun `should set subscriptionStatus to SUBSCRIBED if not already subscribed`() {
      val orgPaymentConfig =
        OrganizationPaymentConfig().apply {
          subscriptionStatus = SubscriptionStatus.PRE_SUBSCRIPTION
        }
      val slotConfig = slot<OrganizationPaymentConfig>()

      every { organizationPaymentConfigRepository.findByOrganizationId(organizationId.value) } returns orgPaymentConfig
      every { organizationPaymentConfigRepository.savePaymentConfig(capture(slotConfig)) } just Runs

      service.handleSubscriptionStarted(organizationId)

      slotConfig.captured.subscriptionStatus shouldBe SubscriptionStatus.SUBSCRIBED
      verify { organizationPaymentConfigRepository.savePaymentConfig(orgPaymentConfig) }
      verify(exactly = 0) { connectionService.disableConnections(any(), any()) }
    }
  }

  @Nested
  inner class HandleSubscriptionEnded {
    @Test
    fun `should throw if orgPaymentConfig is not found`() {
      every { organizationPaymentConfigRepository.findByOrganizationId(organizationId.value) } returns null
      shouldThrow<ResourceNotFoundProblem> { service.handleSubscriptionEnded(organizationId) }
    }

    @Test
    fun `should no-op if already unsubscribed`() {
      val orgPaymentConfig =
        OrganizationPaymentConfig().apply {
          subscriptionStatus = SubscriptionStatus.UNSUBSCRIBED
        }

      every { organizationPaymentConfigRepository.findByOrganizationId(organizationId.value) } returns orgPaymentConfig

      service.handleSubscriptionEnded(organizationId)

      verify(exactly = 0) { organizationPaymentConfigRepository.savePaymentConfig(any()) }
      verify(exactly = 0) { connectionService.disableConnections(any(), any()) }
    }

    @Test
    fun `should no-op if pre-subscription`() {
      val orgPaymentConfig =
        OrganizationPaymentConfig().apply {
          subscriptionStatus = SubscriptionStatus.PRE_SUBSCRIPTION
        }

      every { organizationPaymentConfigRepository.findByOrganizationId(organizationId.value) } returns orgPaymentConfig

      service.handleSubscriptionEnded(organizationId)

      verify(exactly = 0) { organizationPaymentConfigRepository.savePaymentConfig(any()) }
      verify(exactly = 0) { connectionService.disableConnections(any(), any()) }
    }

    @Test
    fun `should set subscriptionStatus to UNSUBSCRIBED if currently SUBSCRIBED and disableAllConnections`() {
      val orgPaymentConfig =
        OrganizationPaymentConfig().apply {
          subscriptionStatus = SubscriptionStatus.SUBSCRIBED
        }
      val slotConfig = slot<OrganizationPaymentConfig>()

      every { organizationPaymentConfigRepository.findByOrganizationId(organizationId.value) } returns orgPaymentConfig
      every { organizationPaymentConfigRepository.savePaymentConfig(capture(slotConfig)) } just Runs

      every { connectionRepository.listConnectionIdsForOrganization(organizationId.value) } returns
        listOf(connectionId1.value, connectionId2.value)
      every { connectionService.disableConnections(any(), any()) } returns mockk()

      service.handleSubscriptionEnded(organizationId)

      slotConfig.captured.subscriptionStatus shouldBe SubscriptionStatus.UNSUBSCRIBED
      verify { organizationPaymentConfigRepository.savePaymentConfig(orgPaymentConfig) }

      verify {
        connectionService.disableConnections(
          setOf(connectionId1, connectionId2),
          ConnectionAutoDisabledReason.UNSUBSCRIBED,
        )
      }
    }
  }
}
