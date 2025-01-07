package io.airbyte.commons.server.services

import io.airbyte.analytics.BillingTrackingHelper
import io.airbyte.api.problems.ResourceType
import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.model.generated.ProblemResourceData
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.api.problems.throwable.generated.StateConflictProblem
import io.airbyte.commons.server.ConnectionId
import io.airbyte.commons.server.OrganizationId
import io.airbyte.config.OrganizationPaymentConfig
import io.airbyte.config.OrganizationPaymentConfig.PaymentStatus
import io.airbyte.data.services.shared.ConnectionAutoDisabledReason
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.transaction.annotation.Transactional
import jakarta.inject.Singleton
import io.airbyte.data.services.ConnectionService as ConnectionRepository
import io.airbyte.data.services.OrganizationPaymentConfigService as OrganizationPaymentConfigRepository

private val logger = KotlinLogging.logger {}

/**
 * Application service for performing business logic related to organizations.
 */
interface OrganizationService {
  /**
   * Disable all connections in an organization.
   *
   * @param organizationId the ID of the organization to disable connections for
   * @param autoDisableReason if set, the reason the connections were disabled through an automatic process
   * @return the set of connection IDs that were disabled
   */
  fun disableAllConnections(
    organizationId: OrganizationId,
    autoDisableReason: ConnectionAutoDisabledReason?,
  ): Set<ConnectionId>

  /**
   * Handle the end of a payment grace period for an organization.
   *
   * @param organizationId the ID of the organization that reached the end of a grace period.
   */
  fun handlePaymentGracePeriodEnded(organizationId: OrganizationId)

  /**
   * Handle an uncollectible invoice for an organization.
   *
   * @param organizationId the ID of the organization with the uncollectible invoice.
   */
  fun handleUncollectibleInvoice(organizationId: OrganizationId)

  /**
   * Handle the start of a subscription for an organization
   *
   * @param organizationId the ID of the organization that started a new subscription
   */
  fun handleSubscriptionStarted(organizationId: OrganizationId)

  /**
   * Handle the end of a subscription for an organization
   *
   * @param organizationId the ID of the organization that unsubscribed
   */
  fun handleSubscriptionEnded(organizationId: OrganizationId)
}

@Singleton
open class OrganizationServiceImpl(
  private val connectionService: ConnectionService,
  private val connectionRepository: ConnectionRepository,
  private val organizationPaymentConfigRepository: OrganizationPaymentConfigRepository,
  private val billingTrackingHelper: BillingTrackingHelper,
) : OrganizationService {
  @Transactional("config")
  override fun disableAllConnections(
    organizationId: OrganizationId,
    autoDisabledReason: ConnectionAutoDisabledReason?,
  ): Set<ConnectionId> {
    val connectionIds = connectionRepository.listConnectionIdsForOrganization(organizationId.value).map(::ConnectionId).toSet()
    return connectionService.disableConnections(connectionIds, autoDisabledReason)
  }

  @Transactional("config")
  override fun handlePaymentGracePeriodEnded(organizationId: OrganizationId) {
    val orgPaymentConfig =
      organizationPaymentConfigRepository.findByOrganizationId(organizationId.value)
        ?: throw ResourceNotFoundProblem(
          ProblemResourceData().resourceId(organizationId.toString()).resourceType(ResourceType.ORGANIZATION_PAYMENT_CONFIG),
        )

    if (orgPaymentConfig.paymentStatus != PaymentStatus.GRACE_PERIOD) {
      throw StateConflictProblem(
        ProblemMessageData().message(
          "OrganizationPaymentConfig paymentStatus is ${orgPaymentConfig.paymentStatus}, but expected ${PaymentStatus.GRACE_PERIOD}",
        ),
      )
    }

    orgPaymentConfig.paymentStatus = PaymentStatus.DISABLED
    organizationPaymentConfigRepository.savePaymentConfig(orgPaymentConfig)

    disableAllConnections(organizationId, ConnectionAutoDisabledReason.INVALID_PAYMENT_METHOD)
    billingTrackingHelper.trackGracePeriodEnded(organizationId.value, orgPaymentConfig.paymentProviderId)
  }

  override fun handleUncollectibleInvoice(organizationId: OrganizationId) {
    val orgPaymentConfig =
      organizationPaymentConfigRepository.findByOrganizationId(organizationId.value)
        ?: throw ResourceNotFoundProblem(
          ProblemResourceData().resourceId(organizationId.toString()).resourceType(ResourceType.ORGANIZATION_PAYMENT_CONFIG),
        )

    orgPaymentConfig.paymentStatus = PaymentStatus.LOCKED
    organizationPaymentConfigRepository.savePaymentConfig(orgPaymentConfig)

    disableAllConnections(organizationId, ConnectionAutoDisabledReason.INVOICE_MARKED_UNCOLLECTIBLE)
  }

  override fun handleSubscriptionStarted(organizationId: OrganizationId) {
    val orgPaymentConfig =
      organizationPaymentConfigRepository.findByOrganizationId(organizationId.value)
        ?: throw ResourceNotFoundProblem(
          ProblemResourceData().resourceId(organizationId.toString()).resourceType(ResourceType.ORGANIZATION_PAYMENT_CONFIG),
        )

    val currentSubscriptionStatus = orgPaymentConfig.subscriptionStatus

    if (currentSubscriptionStatus == OrganizationPaymentConfig.SubscriptionStatus.SUBSCRIBED) {
      logger.warn {
        "Received a subscription started event for organization ${orgPaymentConfig.organizationId} that is already subscribed. Ignoring..."
      }
      return
    }

    orgPaymentConfig.subscriptionStatus = OrganizationPaymentConfig.SubscriptionStatus.SUBSCRIBED
    organizationPaymentConfigRepository.savePaymentConfig(orgPaymentConfig)
    logger.info {
      "Organization ${orgPaymentConfig.organizationId} successfully updated from $currentSubscriptionStatus to ${orgPaymentConfig.subscriptionStatus}"
    }
  }

  @Transactional("config")
  override fun handleSubscriptionEnded(organizationId: OrganizationId) {
    val orgPaymentConfig =
      organizationPaymentConfigRepository.findByOrganizationId(organizationId.value)
        ?: throw ResourceNotFoundProblem(
          ProblemResourceData().resourceId(organizationId.toString()).resourceType(ResourceType.ORGANIZATION_PAYMENT_CONFIG),
        )

    when (val currentSubscriptionStatus = orgPaymentConfig.subscriptionStatus) {
      OrganizationPaymentConfig.SubscriptionStatus.UNSUBSCRIBED, OrganizationPaymentConfig.SubscriptionStatus.PRE_SUBSCRIPTION -> {
        logger.warn {
          "Received a subscription ended event for organization $organizationId that is not currently subscribed. Ignoring..."
        }
        return
      }
      OrganizationPaymentConfig.SubscriptionStatus.SUBSCRIBED -> {
        orgPaymentConfig.subscriptionStatus = OrganizationPaymentConfig.SubscriptionStatus.UNSUBSCRIBED
        organizationPaymentConfigRepository.savePaymentConfig(orgPaymentConfig)
        logger.info {
          "Organization $organizationId successfully updated from $currentSubscriptionStatus to ${orgPaymentConfig.subscriptionStatus}"
        }
        disableAllConnections(organizationId, ConnectionAutoDisabledReason.UNSUBSCRIBED)
        logger.info { "Successfully disabled all syncs for unsubscribed organization $organizationId" }
      }
    }
  }
}
