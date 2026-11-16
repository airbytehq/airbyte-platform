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
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.NullSource
import org.junit.jupiter.params.provider.ValueSource
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.Optional
import java.util.UUID

class DataplaneAuthenticationObservationServiceTest {
  private val dataplaneRepository = mockk<DataplaneRepository>()
  private val dataplaneGroupRepository = mockk<DataplaneGroupRepository>()
  private val dataplaneHealthService = mockk<DataplaneHealthService>(relaxed = true)
  private val service = DataplaneAuthenticationObservationService(dataplaneRepository, dataplaneGroupRepository, dataplaneHealthService)

  private val rejectedAt = OffsetDateTime.of(2026, 11, 11, 14, 3, 0, 0, ZoneOffset.UTC)
  private val organizationId = UUID.randomUUID()
  private val clientId = UUID.randomUUID()
  private val dataplane = registeredDataplane(clientId)

  init {
    every { dataplaneRepository.findByServiceAccountId(any()) } returns null
    every { dataplaneRepository.findById(any()) } returns Optional.empty()
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = ["", "not-a-uuid"])
  fun `a rejection with a registered client ID is saved for that data plane whatever the diagnostics header holds`(header: String?) {
    givenRegistered(dataplane, organizationId)

    val association = service.recordRejectedTokenExchange(clientId.toString(), header, rejectedAt)

    assertEquals(DataplaneTokenRejectionAssociation.RegisteredClientId(dataplane.id!!, organizationId), association)
    verifyRejectionSaved(dataplane.id!!, organizationId)
  }

  @Test
  fun `a header naming another organization's data plane does not move a registered client's rejection`() {
    givenRegistered(dataplane, organizationId)
    val otherOrganizationDataplane = registeredDataplane(UUID.randomUUID())
    givenRegistered(otherOrganizationDataplane, UUID.randomUUID())

    val association = service.recordRejectedTokenExchange(clientId.toString(), otherOrganizationDataplane.id.toString(), rejectedAt)

    assertEquals(DataplaneTokenRejectionAssociation.RegisteredClientId(dataplane.id!!, organizationId), association)
    verifyRejectionSaved(dataplane.id!!, organizationId)
    verify(exactly = 0) { dataplaneHealthService.recordAuthenticationRejection(otherOrganizationDataplane.id!!, any(), any(), any(), any()) }
    verify(exactly = 0) { dataplaneRepository.findById(any()) }
  }

  @Test
  fun `an unregistered client ID without a diagnostics header stays in telemetry`() {
    val association = service.recordRejectedTokenExchange(clientId.toString(), null, rejectedAt)

    assertEquals(DataplaneTokenRejectionAssociation.Unassociated, association)
    verify(exactly = 0) { dataplaneRepository.findById(any()) }
    verify(exactly = 0) { dataplaneHealthService.recordAuthenticationRejection(any(), any(), any(), any(), any()) }
  }

  @ParameterizedTest
  @ValueSource(strings = ["9b7a1c52-3f0e-4d8a-9c41-2e6b5f1d7a30", "not-a-uuid"])
  fun `an unregistered client ID with a header naming a data plane is saved as claimed under that data plane's own organization`(
    unregisteredClientId: String,
  ) {
    val claimedOrganizationId = UUID.randomUUID()
    val claimed = registeredDataplane(UUID.randomUUID())
    givenRegistered(claimed, claimedOrganizationId)
    val association = service.recordRejectedTokenExchange(unregisteredClientId, claimed.id.toString().uppercase(), rejectedAt)

    assertEquals(DataplaneTokenRejectionAssociation.ClaimedDiagnosticId(claimed.id!!, claimedOrganizationId), association)
    verifyRejectionSaved(claimed.id!!, claimedOrganizationId, DataplaneAuthenticationAssociation.CLAIMED_DIAGNOSTIC_ID)
  }

  @ParameterizedTest
  @ValueSource(strings = ["", "not-a-uuid"])
  fun `an unregistered client ID with a malformed header stays in telemetry without a lookup`(header: String) {
    val association = service.recordRejectedTokenExchange(clientId.toString(), header, rejectedAt)

    assertEquals(DataplaneTokenRejectionAssociation.Unassociated, association)
    verify(exactly = 0) { dataplaneRepository.findById(any()) }
    verify(exactly = 0) { dataplaneHealthService.recordAuthenticationRejection(any(), any(), any(), any(), any()) }
  }

  @Test
  fun `an unregistered client ID with a header naming no data plane stays in telemetry`() {
    val association = service.recordRejectedTokenExchange(clientId.toString(), UUID.randomUUID().toString(), rejectedAt)

    assertEquals(DataplaneTokenRejectionAssociation.Unassociated, association)
    verify(exactly = 0) { dataplaneHealthService.recordAuthenticationRejection(any(), any(), any(), any(), any()) }
  }

  @Test
  fun `an unregistered client ID with a header naming a deleted data plane stays in telemetry`() {
    val deleted = registeredDataplane(UUID.randomUUID()).copy(tombstone = true)
    every { dataplaneRepository.findById(deleted.id!!) } returns Optional.of(deleted)

    val association = service.recordRejectedTokenExchange(clientId.toString(), deleted.id.toString(), rejectedAt)

    assertEquals(DataplaneTokenRejectionAssociation.Unassociated, association)
    verify(exactly = 0) { dataplaneHealthService.recordAuthenticationRejection(any(), any(), any(), any(), any()) }
  }

  @ParameterizedTest
  @ValueSource(strings = ["", "not-a-uuid", "client id with spaces"])
  fun `a malformed client ID without a diagnostics header stays in telemetry without a lookup`(malformedClientId: String) {
    val association = service.recordRejectedTokenExchange(malformedClientId, null, rejectedAt)

    assertEquals(DataplaneTokenRejectionAssociation.Unassociated, association)
    verify(exactly = 0) { dataplaneRepository.findByServiceAccountId(any()) }
    verify(exactly = 0) { dataplaneHealthService.recordAuthenticationRejection(any(), any(), any(), any(), any()) }
  }

  @Test
  fun `a deleted data plane's client ID stays in telemetry even when the header names it`() {
    val deleted = dataplane.copy(tombstone = true)
    every { dataplaneRepository.findByServiceAccountId(clientId) } returns deleted
    every { dataplaneRepository.findById(deleted.id!!) } returns Optional.of(deleted)

    val association = service.recordRejectedTokenExchange(clientId.toString(), deleted.id.toString(), rejectedAt)

    assertEquals(DataplaneTokenRejectionAssociation.Unassociated, association)
    verify(exactly = 0) { dataplaneHealthService.recordAuthenticationRejection(any(), any(), any(), any(), any()) }
  }

  @Test
  fun `an accepted check-in is passed on as a recovery for the data plane's organization`() {
    service.recordAcceptedCheckIn(dataplane.id!!, organizationId, rejectedAt)

    verify(exactly = 1) { dataplaneHealthService.recordAuthenticationRecovery(dataplane.id!!, organizationId, rejectedAt) }
  }

  private fun registeredDataplane(serviceAccountId: UUID) =
    Dataplane(
      id = UUID.randomUUID(),
      dataplaneGroupId = UUID.randomUUID(),
      name = "dataplane",
      enabled = true,
      tombstone = false,
      serviceAccountId = serviceAccountId,
    )

  private fun givenRegistered(
    registered: Dataplane,
    owner: UUID,
  ) {
    every { dataplaneRepository.findByServiceAccountId(registered.serviceAccountId!!) } returns registered
    every { dataplaneRepository.findById(registered.id!!) } returns Optional.of(registered)
    every { dataplaneGroupRepository.getOrganizationIdFromDataplaneGroup(registered.dataplaneGroupId) } returns owner
  }

  private fun verifyRejectionSaved(
    dataplaneId: UUID,
    owner: UUID,
    association: DataplaneAuthenticationAssociation = DataplaneAuthenticationAssociation.REGISTERED_CLIENT_ID,
  ) {
    val message =
      when (association) {
        DataplaneAuthenticationAssociation.REGISTERED_CLIENT_ID -> REGISTERED_CLIENT_ID_REJECTED_MESSAGE
        DataplaneAuthenticationAssociation.CLAIMED_DIAGNOSTIC_ID -> CLAIMED_DIAGNOSTIC_ID_REJECTED_MESSAGE
      }
    verify(exactly = 1) {
      dataplaneHealthService.recordAuthenticationRejection(
        dataplaneId = dataplaneId,
        organizationId = owner,
        rejectedAt = rejectedAt,
        failure = DataplaneDiagnosticOutcome.Failed(CREDENTIALS_REJECTED_ERROR_CODE, message),
        association = association,
      )
    }
  }
}
