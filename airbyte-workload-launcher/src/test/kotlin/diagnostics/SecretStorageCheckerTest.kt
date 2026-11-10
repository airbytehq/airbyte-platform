/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.diagnostics

import io.airbyte.api.client.model.generated.DataplaneDiagnosticKind
import io.airbyte.api.client.model.generated.DataplaneDiagnosticResult
import io.airbyte.api.client.model.generated.DataplaneDiagnosticStatus
import io.airbyte.api.client.model.generated.ScopeType
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.airbyte.workers.hydration.ConnectorSecretsHydrator
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.util.UUID

internal class SecretStorageCheckerTest {
  private val organizationId = UUID.randomUUID()
  private val target = SecretStorageProbeTarget(secretStorageId = UUID.randomUUID(), scopeType = ScopeType.WORKSPACE, scopeId = UUID.randomUUID())
  private val now = Instant.parse("2026-11-09T10:00:00Z")
  private val testSecret = AirbyteManagedSecretCoordinate("airbyte_diagnostic_$organizationId", 1L)
  private val testSecretValue = "Organization $organizationId secret storage is reachable"

  private lateinit var hydrator: ConnectorSecretsHydrator
  private lateinit var persistence: SecretPersistence
  private lateinit var checker: SecretStorageChecker

  @BeforeEach
  fun setup() {
    hydrator = mockk()
    persistence = mockk()
    every { hydrator.getPersistenceByStorageId(target.secretStorageId) } returns persistence
    every { persistence.write(testSecret, testSecretValue) } just runs
    checker = SecretStorageChecker(hydrator, Clock.fixed(now, ZoneOffset.UTC))
  }

  @Test
  fun `the test secret is named after the organization in Airbyte's secret name format`() {
    assertEquals("airbyte_diagnostic_${organizationId}_v1", SecretStorageChecker.testSecret(organizationId).fullCoordinate)
  }

  @Test
  fun `an existing test secret passes without being written`() {
    every { persistence.read(testSecret) } returns testSecretValue

    assertEquals(expected(DataplaneDiagnosticStatus.PASSED), checker.check(target, organizationId))
    verify(exactly = 0) { persistence.write(any(), any()) }
  }

  @Test
  fun `a missing test secret is created and then passes`() {
    every { persistence.read(testSecret) } returnsMany listOf("", testSecretValue)

    assertEquals(expected(DataplaneDiagnosticStatus.PASSED), checker.check(target, organizationId))
    verify(exactly = 1) { persistence.write(testSecret, testSecretValue) }
  }

  @Test
  fun `a failed create is unable to complete`() {
    every { persistence.read(testSecret) } returns ""
    every { persistence.write(testSecret, testSecretValue) } throws RuntimeException("AccessDeniedException: arn:aws:secretsmanager:private")

    assertEquals(
      expected(
        DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE,
        SecretStorageChecker.TEST_SECRET_CREATE_FAILED,
        "Airbyte could not create the test secret airbyte_diagnostic_${organizationId}_v1 in this secret store.",
      ),
      checker.check(target, organizationId),
    )
  }

  @Test
  fun `an empty read after a create is unable to complete`() {
    every { persistence.read(testSecret) } returns ""

    assertEquals(
      expected(
        DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE,
        SecretStorageChecker.TEST_SECRET_UNVERIFIED,
        "Airbyte could not confirm that the data plane can read the test secret airbyte_diagnostic_${organizationId}_v1 from this secret store.",
      ),
      checker.check(target, organizationId),
    )
  }

  @Test
  fun `a read that throws fails without showing the error`() {
    every { persistence.read(testSecret) } throws RuntimeException("AccessDeniedException: arn:aws:secretsmanager:private")

    assertEquals(
      expected(DataplaneDiagnosticStatus.FAILED, SecretStorageChecker.READ_FAILED, "The data plane could not read from this secret store."),
      checker.check(target, organizationId),
    )
    verify(exactly = 0) { persistence.write(any(), any()) }
  }

  @Test
  fun `a read after the create that throws fails`() {
    every { persistence.read(testSecret) } returns "" andThenThrows RuntimeException("connection reset")

    assertEquals(
      expected(DataplaneDiagnosticStatus.FAILED, SecretStorageChecker.READ_FAILED, "The data plane could not read from this secret store."),
      checker.check(target, organizationId),
    )
  }

  @Test
  fun `a store whose settings can't be loaded is unable to complete`() {
    every { hydrator.getPersistenceByStorageId(target.secretStorageId) } throws RuntimeException("403 Forbidden")

    assertEquals(
      expected(
        DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE,
        SecretStorageChecker.STORE_UNAVAILABLE,
        "Airbyte could not load this secret store's settings.",
      ),
      checker.check(target, organizationId),
    )
  }

  @Test
  fun `a timed out store is unable to complete`() {
    assertEquals(
      expected(DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE, SecretStorageChecker.TIMED_OUT, "The secret store did not respond in time."),
      checker.timedOut(target),
    )
  }

  private fun expected(
    status: DataplaneDiagnosticStatus,
    errorCode: String? = null,
    errorMessage: String? = null,
  ) = DataplaneDiagnosticResult(
    kind = DataplaneDiagnosticKind.SECRET_STORAGE,
    targetId = target.secretStorageId,
    status = status,
    checkedAt = now.toEpochMilli(),
    errorCode = errorCode,
    errorMessage = errorMessage,
    observation = mapOf("scope_type" to "workspace", "scope_id" to target.scopeId.toString()),
  )
}
