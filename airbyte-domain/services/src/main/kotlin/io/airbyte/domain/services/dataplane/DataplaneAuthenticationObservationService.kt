/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.dataplane

import io.airbyte.data.repositories.DataplaneGroupRepository
import io.airbyte.data.repositories.DataplaneRepository
import io.airbyte.data.repositories.entities.Dataplane
import io.airbyte.data.services.DataplaneHealthService
import io.airbyte.domain.models.DataplaneAuthenticationAssociation
import io.airbyte.domain.models.DataplaneDiagnosticOutcome
import io.airbyte.domain.models.DataplaneTokenRejectionAssociation
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.time.OffsetDateTime
import java.util.UUID

private val logger = KotlinLogging.logger {}

internal const val CREDENTIALS_REJECTED_ERROR_CODE = "CREDENTIALS_REJECTED"
internal const val REGISTERED_CLIENT_ID_REJECTED_MESSAGE =
  "Airbyte rejected a token request that used this data plane's client ID. Check the client secret configured for the data plane."
internal const val CLAIMED_DIAGNOSTIC_ID_REJECTED_MESSAGE =
  "Airbyte rejected a token request with an unknown client ID that named this data plane. Check the client ID configured for the data plane."

/**
 * Records authentication failures from rejected token exchanges, and their recovery at the next accepted check-in.
 */
@Singleton
class DataplaneAuthenticationObservationService(
  private val dataplaneRepository: DataplaneRepository,
  private val dataplaneGroupRepository: DataplaneGroupRepository,
  private val dataplaneHealthService: DataplaneHealthService,
) {
  /**
   * Saves a rejected token exchange as an authentication failure and returns which data plane it was tied to.
   * [diagnosticsDataplaneId] comes from an unauthenticated header, so it is only read when [clientId] matches nothing.
   * A deleted data plane counts as no data plane.
   *
   * ```
   * rejected exchange
   *   |
   *   +-- clientId belongs to a data plane? ----- yes --> FAILED, REGISTERED_CLIENT_ID, on that data plane
   *   |   no
   *   +-- header names a data plane? ------------ yes --> FAILED, CLAIMED_DIAGNOSTIC_ID, on that data plane
   *   |   no
   *   +-- nothing saved, only logged and counted (Unassociated)
   * ```
   *
   * The organization always comes from the data plane's record, never from the request.
   */
  fun recordRejectedTokenExchange(
    clientId: String,
    diagnosticsDataplaneId: String?,
    rejectedAt: OffsetDateTime,
  ): DataplaneTokenRejectionAssociation {
    // A deleted data plane no longer has a card to show the rejection on, so it counts as unregistered.
    val clientIdDataplane = parseUuid(clientId)?.let(dataplaneRepository::findByServiceAccountId)?.takeUnless { it.tombstone }
    if (clientIdDataplane != null) {
      val dataplaneId = checkNotNull(clientIdDataplane.id) { "dataplane.id is the primary key" }
      if (diagnosticsDataplaneId != null && parseUuid(diagnosticsDataplaneId) != dataplaneId) {
        logger.info { "Ignoring the diagnostics header on a rejected token exchange for dataplane $dataplaneId: it does not name this data plane" }
      }
      val organizationId = saveRejection(clientIdDataplane, rejectedAt, DataplaneAuthenticationAssociation.REGISTERED_CLIENT_ID)
      return DataplaneTokenRejectionAssociation.RegisteredClientId(dataplaneId, organizationId)
    }

    val claimedDataplane =
      diagnosticsDataplaneId
        ?.let(::parseUuid)
        ?.let { dataplaneRepository.findById(it).orElse(null) }
        ?.takeUnless { it.tombstone }
    if (claimedDataplane == null) {
      logger.warn {
        "Rejected a dataplane token exchange for an unregistered client ID (diagnostics header present: ${diagnosticsDataplaneId != null})"
      }
      return DataplaneTokenRejectionAssociation.Unassociated
    }

    val organizationId = saveRejection(claimedDataplane, rejectedAt, DataplaneAuthenticationAssociation.CLAIMED_DIAGNOSTIC_ID)
    return DataplaneTokenRejectionAssociation.ClaimedDiagnosticId(checkNotNull(claimedDataplane.id), organizationId)
  }

  /**
   * Marks the data plane's authentication failure as passed at [checkedInAt]. Only the first check-in after a failure
   * changes it.
   */
  fun recordAcceptedCheckIn(
    dataplaneId: UUID,
    organizationId: UUID,
    checkedInAt: OffsetDateTime,
  ) = dataplaneHealthService.recordAuthenticationRecovery(dataplaneId, organizationId, checkedInAt)

  /**
   * Returns the organization the failure was saved under.
   */
  private fun saveRejection(
    dataplane: Dataplane,
    rejectedAt: OffsetDateTime,
    association: DataplaneAuthenticationAssociation,
  ): UUID {
    val organizationId = dataplaneGroupRepository.getOrganizationIdFromDataplaneGroup(dataplane.dataplaneGroupId)
    val message =
      when (association) {
        DataplaneAuthenticationAssociation.REGISTERED_CLIENT_ID -> REGISTERED_CLIENT_ID_REJECTED_MESSAGE
        DataplaneAuthenticationAssociation.CLAIMED_DIAGNOSTIC_ID -> CLAIMED_DIAGNOSTIC_ID_REJECTED_MESSAGE
      }
    dataplaneHealthService.recordAuthenticationRejection(
      dataplaneId = checkNotNull(dataplane.id) { "dataplane.id is the primary key" },
      organizationId = organizationId,
      rejectedAt = rejectedAt,
      failure = DataplaneDiagnosticOutcome.Failed(CREDENTIALS_REJECTED_ERROR_CODE, message),
      association = association,
    )
    return organizationId
  }

  private fun parseUuid(value: String): UUID? =
    try {
      UUID.fromString(value)
    } catch (e: IllegalArgumentException) {
      null
    }
}
