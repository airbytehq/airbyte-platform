package io.airbyte.analytics

import io.airbyte.config.ScopeType
import jakarta.inject.Singleton
import java.util.UUID

private const val ACTION_GRACE_PERIOD_STARTED = "grace_period_started"
private const val ACTION_GRACE_PERIOD_ENDED = "grace_period_ended"
private const val ACTION_GRACE_PERIOD_UPDATED = "grace_period_updated"
private const val ACTION_GRACE_PERIOD_CANCELED = "grace_period_canceled"
private const val ACTION_PAYMENT_SETUP_COMPLETED = "payment_setup_completed"
private const val METADATA_GRACE_PERIOD_END_AT_SECONDS = "grace_period_end_at_seconds"
private const val METADATA_REASON = "reason"
private const val METADATA_PAYMENT_PROVIDER_ID = "payment_provider_id"

/**
 * This helper provides convenience wrappers around the tracking client for billing-related events.
 */
@Singleton
class BillingTrackingHelper(
  private val trackingClient: TrackingClient,
) {
  fun trackGracePeriodStarted(
    organizationId: UUID,
    paymentProviderId: String,
    gracePeriodEndAtSeconds: Long,
    reason: String,
  ) {
    trackingClient.track(
      organizationId,
      ScopeType.ORGANIZATION,
      ACTION_GRACE_PERIOD_STARTED,
      mapOf(
        METADATA_PAYMENT_PROVIDER_ID to paymentProviderId,
        METADATA_GRACE_PERIOD_END_AT_SECONDS to gracePeriodEndAtSeconds.toString(),
        METADATA_REASON to reason,
      ),
    )
  }

  fun trackGracePeriodEnded(
    organizationId: UUID,
    paymentProviderId: String,
  ) {
    trackingClient.track(
      organizationId,
      ScopeType.ORGANIZATION,
      ACTION_GRACE_PERIOD_ENDED,
      mapOf(
        METADATA_PAYMENT_PROVIDER_ID to paymentProviderId,
      ),
    )
  }

  fun trackGracePeriodUpdated(
    organizationId: UUID,
    paymentProviderId: String,
    gracePeriodEndAtSeconds: Long,
  ) {
    trackingClient.track(
      organizationId,
      ScopeType.ORGANIZATION,
      ACTION_GRACE_PERIOD_UPDATED,
      mapOf(
        METADATA_PAYMENT_PROVIDER_ID to paymentProviderId,
        METADATA_GRACE_PERIOD_END_AT_SECONDS to gracePeriodEndAtSeconds.toString(),
      ),
    )
  }

  fun trackGracePeriodCanceled(
    organizationId: UUID,
    paymentProviderId: String,
    reason: String,
  ) {
    trackingClient.track(
      organizationId,
      ScopeType.ORGANIZATION,
      ACTION_GRACE_PERIOD_CANCELED,
      mapOf(
        METADATA_PAYMENT_PROVIDER_ID to paymentProviderId,
        METADATA_REASON to reason,
      ),
    )
  }

  fun trackPaymentSetupCompleted(
    organizationId: UUID,
    paymentProviderId: String,
  ) {
    trackingClient.track(
      organizationId,
      ScopeType.ORGANIZATION,
      ACTION_PAYMENT_SETUP_COMPLETED,
      mapOf(
        METADATA_PAYMENT_PROVIDER_ID to paymentProviderId,
      ),
    )
  }
}
