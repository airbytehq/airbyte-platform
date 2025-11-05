/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.jobs

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.DomainVerificationsApi
import io.airbyte.api.client.model.generated.DomainVerificationIdRequestBody
import io.airbyte.api.client.model.generated.DomainVerificationListResponse
import io.airbyte.api.client.model.generated.DomainVerificationResponse
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifySequence
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.OffsetDateTime
import java.util.UUID

class DomainVerificationJobTest {
  private lateinit var airbyteApiClient: AirbyteApiClient
  private lateinit var domainVerificationsApi: DomainVerificationsApi
  private lateinit var domainVerificationJob: DomainVerificationJob

  @BeforeEach
  fun setup() {
    domainVerificationsApi = mockk()
    airbyteApiClient =
      mockk {
        every { domainVerificationsApi } returns this@DomainVerificationJobTest.domainVerificationsApi
      }
    domainVerificationJob = DomainVerificationJob(airbyteApiClient)
  }

  @Test
  fun `checks verification when lastCheckedAt is null`() {
    val verificationId = UUID.randomUUID()
    val verification =
      createVerification(
        id = verificationId,
        lastCheckedAt = null,
        attempts = 0,
      )

    every { domainVerificationsApi.listPendingDomainVerifications() } returns
      DomainVerificationListResponse(domainVerifications = listOf(verification))
    every { domainVerificationsApi.checkDomainVerification(any()) } returns
      createVerification(id = verificationId, domain = "example.com")

    domainVerificationJob.checkPendingDomainVerifications()

    verify(exactly = 1) {
      domainVerificationsApi.checkDomainVerification(
        match { it.domainVerificationId == verificationId },
      )
    }
  }

  @Test
  fun `checks verification every time during first hour (attempts 0-59)`() {
    val now = OffsetDateTime.now()
    val verifications =
      (0..5).map { attempt ->
        createVerification(
          id = UUID.randomUUID(),
          lastCheckedAt = now.minusSeconds(30), // Just checked 30 seconds ago
          attempts = attempt,
        )
      }

    every { domainVerificationsApi.listPendingDomainVerifications() } returns
      DomainVerificationListResponse(domainVerifications = verifications)
    every { domainVerificationsApi.checkDomainVerification(any()) } returns
      createVerification()

    domainVerificationJob.checkPendingDomainVerifications()

    // All verifications should be checked (no backoff in first hour)
    verify(exactly = 6) {
      domainVerificationsApi.checkDomainVerification(any())
    }
  }

  @Test
  fun `applies exponential backoff after first hour (attempt 60+)`() {
    val now = OffsetDateTime.now()

    // Create verifications with different backoff requirements
    val shouldCheck =
      listOf(
        // Attempt 60: needs 1 minute - checked 2 minutes ago (should check)
        createVerification(
          id = UUID.randomUUID(),
          lastCheckedAt = now.minusMinutes(2),
          attempts = 60,
        ),
        // Attempt 61: needs 2 minutes - checked 3 minutes ago (should check)
        createVerification(
          id = UUID.randomUUID(),
          lastCheckedAt = now.minusMinutes(3),
          attempts = 61,
        ),
        // Attempt 66: needs 60 minutes (capped) - checked 61 minutes ago (should check)
        createVerification(
          id = UUID.randomUUID(),
          lastCheckedAt = now.minusMinutes(61),
          attempts = 66,
        ),
      )

    val shouldSkip =
      listOf(
        // Attempt 60: needs 1 minute - checked 30 seconds ago (should skip)
        createVerification(
          id = UUID.randomUUID(),
          lastCheckedAt = now.minusSeconds(30),
          attempts = 60,
        ),
        // Attempt 62: needs 4 minutes - checked 3 minutes ago (should skip)
        createVerification(
          id = UUID.randomUUID(),
          lastCheckedAt = now.minusMinutes(3),
          attempts = 62,
        ),
        // Attempt 66: needs 60 minutes - checked 59 minutes ago (should skip)
        createVerification(
          id = UUID.randomUUID(),
          lastCheckedAt = now.minusMinutes(59),
          attempts = 66,
        ),
      )

    every { domainVerificationsApi.listPendingDomainVerifications() } returns
      DomainVerificationListResponse(domainVerifications = shouldCheck + shouldSkip)
    every { domainVerificationsApi.checkDomainVerification(any()) } returns
      createVerification()

    domainVerificationJob.checkPendingDomainVerifications()

    // Only 3 should be checked (the ones that passed backoff threshold)
    verify(exactly = 3) {
      domainVerificationsApi.checkDomainVerification(any())
    }

    // Verify the correct ones were checked
    shouldCheck.forEach { verification ->
      verify(exactly = 1) {
        domainVerificationsApi.checkDomainVerification(
          match { it.domainVerificationId == verification.id },
        )
      }
    }

    // Verify the skipped ones were NOT checked
    shouldSkip.forEach { verification ->
      verify(exactly = 0) {
        domainVerificationsApi.checkDomainVerification(
          match { it.domainVerificationId == verification.id },
        )
      }
    }
  }

  @Test
  fun `handles exceptions when checking individual verifications`() {
    val verification1 = createVerification(id = UUID.randomUUID())
    val verification2 = createVerification(id = UUID.randomUUID())
    val verification3 = createVerification(id = UUID.randomUUID())

    every { domainVerificationsApi.listPendingDomainVerifications() } returns
      DomainVerificationListResponse(domainVerifications = listOf(verification1, verification2, verification3))

    // First check succeeds, second fails, third succeeds
    every { domainVerificationsApi.checkDomainVerification(match { it.domainVerificationId == verification1.id }) } returns
      createVerification(id = verification1.id)
    every { domainVerificationsApi.checkDomainVerification(match { it.domainVerificationId == verification2.id }) } throws
      RuntimeException("DNS lookup failed")
    every { domainVerificationsApi.checkDomainVerification(match { it.domainVerificationId == verification3.id }) } returns
      createVerification(id = verification3.id)

    // Should not throw exception - should continue processing after error
    domainVerificationJob.checkPendingDomainVerifications()

    // All three should have been attempted
    verifySequence {
      domainVerificationsApi.listPendingDomainVerifications()
      domainVerificationsApi.checkDomainVerification(match { it.domainVerificationId == verification1.id })
      domainVerificationsApi.checkDomainVerification(match { it.domainVerificationId == verification2.id })
      domainVerificationsApi.checkDomainVerification(match { it.domainVerificationId == verification3.id })
    }
  }

  @Test
  fun `handles empty pending verifications list`() {
    every { domainVerificationsApi.listPendingDomainVerifications() } returns
      DomainVerificationListResponse(domainVerifications = emptyList())

    domainVerificationJob.checkPendingDomainVerifications()

    verify(exactly = 1) {
      domainVerificationsApi.listPendingDomainVerifications()
    }
    verify(exactly = 0) {
      domainVerificationsApi.checkDomainVerification(any())
    }
  }

  @Test
  fun `checks multiple verifications in single run`() {
    val verifications =
      (1..10).map { i ->
        createVerification(
          id = UUID.randomUUID(),
          domain = "domain$i.com",
          attempts = i - 1, // 0 to 9, all within first hour threshold
        )
      }

    every { domainVerificationsApi.listPendingDomainVerifications() } returns
      DomainVerificationListResponse(domainVerifications = verifications)
    every { domainVerificationsApi.checkDomainVerification(any()) } returns
      createVerification()

    domainVerificationJob.checkPendingDomainVerifications()

    // All 10 should be checked since they're all in first hour
    verify(exactly = 10) {
      domainVerificationsApi.checkDomainVerification(any())
    }
  }

  @Test
  fun `respects 60 minute backoff cap for high attempt counts`() {
    val now = OffsetDateTime.now()

    // Attempt 100: exponential would be huge, but should be capped at 60 minutes
    val highAttemptVerification =
      createVerification(
        id = UUID.randomUUID(),
        lastCheckedAt = now.minusMinutes(61), // More than 60 minutes ago
        attempts = 100,
      )

    every { domainVerificationsApi.listPendingDomainVerifications() } returns
      DomainVerificationListResponse(domainVerifications = listOf(highAttemptVerification))
    every { domainVerificationsApi.checkDomainVerification(any()) } returns
      createVerification(id = highAttemptVerification.id)

    domainVerificationJob.checkPendingDomainVerifications()

    // Should be checked because more than 60 minutes have passed (the cap)
    verify(exactly = 1) {
      domainVerificationsApi.checkDomainVerification(
        match { it.domainVerificationId == highAttemptVerification.id },
      )
    }
  }

  @Test
  fun `boundary test - exactly at threshold between phases`() {
    val now = OffsetDateTime.now()

    // Attempt 59: last in first hour phase (should check every time)
    val lastInFirstHour =
      createVerification(
        id = UUID.randomUUID(),
        lastCheckedAt = now.minusSeconds(30), // Just checked
        attempts = 59,
      )

    // Attempt 60: first in exponential backoff phase (needs 1 minute)
    val firstInSecondPhase =
      createVerification(
        id = UUID.randomUUID(),
        lastCheckedAt = now.minusSeconds(30), // Just checked, less than 1 minute
        attempts = 60,
      )

    every { domainVerificationsApi.listPendingDomainVerifications() } returns
      DomainVerificationListResponse(domainVerifications = listOf(lastInFirstHour, firstInSecondPhase))
    every { domainVerificationsApi.checkDomainVerification(any()) } returns
      createVerification()

    domainVerificationJob.checkPendingDomainVerifications()

    // Attempt 59 should be checked (first hour phase)
    verify(exactly = 1) {
      domainVerificationsApi.checkDomainVerification(
        match { it.domainVerificationId == lastInFirstHour.id },
      )
    }

    // Attempt 60 should NOT be checked (backoff not elapsed)
    verify(exactly = 0) {
      domainVerificationsApi.checkDomainVerification(
        match { it.domainVerificationId == firstInSecondPhase.id },
      )
    }
  }

  private fun createVerification(
    id: UUID = UUID.randomUUID(),
    domain: String = "example.com",
    lastCheckedAt: OffsetDateTime? = null,
    attempts: Int = 0,
  ): DomainVerificationResponse =
    DomainVerificationResponse(
      id = id,
      organizationId = UUID.randomUUID(),
      domain = domain,
      status = DomainVerificationResponse.Status.PENDING,
      verificationMethod = DomainVerificationResponse.VerificationMethod.DNS_TXT,
      dnsRecordName = "_airbyte-verification.$domain",
      dnsRecordValue = "airbyte-domain-verification=test-token-$id",
      createdAt = OffsetDateTime.now().minusHours(1).toEpochSecond(),
      lastCheckedAt = lastCheckedAt?.toEpochSecond(),
      attempts = attempts,
    )
}
