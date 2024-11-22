package io.airbyte.commons.server.services

import io.airbyte.api.problems.ResourceType
import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.model.generated.ProblemResourceData
import io.airbyte.api.problems.throwable.generated.ResourceNotFoundProblem
import io.airbyte.api.problems.throwable.generated.StateConflictProblem
import io.airbyte.commons.server.ConnectionId
import io.airbyte.commons.server.OrganizationId
import io.airbyte.config.OrganizationPaymentConfig.PaymentStatus
import io.airbyte.data.services.shared.ConnectionAutoDisabledReason
import io.micronaut.transaction.annotation.Transactional
import jakarta.inject.Singleton
import io.airbyte.data.services.ConnectionService as ConnectionRepository
import io.airbyte.data.services.OrganizationPaymentConfigService as OrganizationPaymentConfigRepository

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
}

@Singleton
open class OrganizationServiceImpl(
  private val connectionService: ConnectionService,
  private val connectionRepository: ConnectionRepository,
  private val organizationPaymentConfigRepository: OrganizationPaymentConfigRepository,
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
    // TODO send an email summarizing the disabled connections and payment method problem
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
    // TODO send an email summarizing the disabled connections and uncollectible invoice problem
  }
}
