/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.analytics

import io.airbyte.config.ScopeType
import jakarta.inject.Singleton
import java.time.OffsetDateTime
import java.util.UUID

private const val ACTION_GRACE_PERIOD_STARTED = "grace_period_started"
private const val ACTION_GRACE_PERIOD_ENDED = "grace_period_ended"
private const val ACTION_GRACE_PERIOD_UPDATED = "grace_period_updated"
private const val ACTION_GRACE_PERIOD_CANCELED = "grace_period_canceled"
private const val ACTION_PAYMENT_SETUP_COMPLETED = "payment_setup_completed"
private const val ACTION_SUBSCRIPTION_CANCELED = "subscription_canceled"
private const val ACTION_SUBSCRIPTION_CANCELLATION_UNSCHEDULED = "subscription_cancellation_unscheduled"
private const val ACTION_PLAN_PHASE_CHANGE = "plan_phase_change"

private const val METADATA_GRACE_PERIOD_END_AT_SECONDS = "grace_period_end_at_seconds"
private const val METADATA_REASON = "reason"
private const val METADATA_PAYMENT_PROVIDER_ID = "payment_provider_id"
private const val METADATA_SUBSCRIPTION_END_DATE = "subscription_end_date"
private const val METADATA_PLAN_NAME = "plan_name"
private const val METADATA_PLAN_ID = "plan_id"
private const val METADATA_ORIGINAL_PHASE = "original_phase"
private const val METADATA_NEW_PHASE = "new_phase"

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
    paymentProviderId: String?,
  ) {
    trackingClient.track(
      organizationId,
      ScopeType.ORGANIZATION,
      ACTION_GRACE_PERIOD_ENDED,
      paymentProviderId?.let { mapOf(METADATA_PAYMENT_PROVIDER_ID to it) } ?: emptyMap(),
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

  fun trackSubscriptionCanceled(
    organizationId: UUID,
    planName: String,
    planId: String,
    subscriptionEndDate: OffsetDateTime,
  ) {
    trackingClient.track(
      organizationId,
      ScopeType.ORGANIZATION,
      ACTION_SUBSCRIPTION_CANCELED,
      mapOf(
        METADATA_PLAN_NAME to planName,
        METADATA_PLAN_ID to planId,
        METADATA_SUBSCRIPTION_END_DATE to subscriptionEndDate.toString(),
      ),
    )
  }

  fun trackSubscriptionCancellationUnscheduled(
    organizationId: UUID,
    planName: String,
    planId: String,
    unscheduledEndDate: OffsetDateTime,
  ) {
    trackingClient.track(
      organizationId,
      ScopeType.ORGANIZATION,
      ACTION_SUBSCRIPTION_CANCELLATION_UNSCHEDULED,
      mapOf(
        METADATA_PLAN_NAME to planName,
        METADATA_PLAN_ID to planId,
        METADATA_SUBSCRIPTION_END_DATE to unscheduledEndDate.toString(),
      ),
    )
  }

  fun trackPlanPhaseChange(
    organizationId: UUID,
    planName: String,
    planId: String,
    originalPhase: Long,
    newPhase: Long,
  ) {
    trackingClient.track(
      organizationId,
      ScopeType.ORGANIZATION,
      ACTION_PLAN_PHASE_CHANGE,
      mapOf(
        METADATA_PLAN_NAME to planName,
        METADATA_PLAN_ID to planId,
        METADATA_ORIGINAL_PHASE to originalPhase.toString(),
        METADATA_NEW_PHASE to newPhase.toString(),
      ),
    )
  }
}
