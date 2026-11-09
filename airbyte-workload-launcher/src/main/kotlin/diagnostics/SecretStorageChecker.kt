/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.diagnostics

import io.airbyte.api.client.model.generated.DataplaneDiagnosticKind
import io.airbyte.api.client.model.generated.DataplaneDiagnosticResult
import io.airbyte.api.client.model.generated.DataplaneDiagnosticStatus
import io.airbyte.config.secrets.SecretCoordinate.ExternalSecretCoordinate
import io.airbyte.workers.hydration.ConnectorSecretsHydrator
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.time.Clock
import java.util.UUID

private val logger = KotlinLogging.logger {}

/**
 * Reads the organization's test secret from one secret store.
 */
@Singleton
class SecretStorageChecker(
  private val connectorSecretsHydrator: ConnectorSecretsHydrator,
  private val clock: Clock,
) {
  // Every failure is reported as that store's result instead of thrown, so one bad store doesn't hide the others.
  fun check(
    target: SecretStorageProbeTarget,
    organizationId: UUID,
  ): DataplaneDiagnosticResult {
    val persistence =
      try {
        connectorSecretsHydrator.getPersistenceByStorageId(target.secretStorageId)
      } catch (e: Exception) {
        logger.warn(e) { "Could not load secret store ${target.secretStorageId}" }
        return result(target, DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE, STORE_UNAVAILABLE, "Airbyte could not load this secret store's settings.")
      }

    val value =
      try {
        persistence.read(ExternalSecretCoordinate(testSecretName(organizationId)))
      } catch (e: Exception) {
        logger.warn(e) { "Could not read the test secret from secret store ${target.secretStorageId}" }
        return result(target, DataplaneDiagnosticStatus.FAILED, READ_FAILED, "The data plane could not read from this secret store.")
      }

    // Some stores return an empty value for both a missing secret and a denied read, so this can't count as a pass.
    if (value.isEmpty()) {
      return result(
        target,
        DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE,
        TEST_SECRET_UNVERIFIED,
        "Airbyte could not confirm that the data plane can read the test secret ${testSecretName(organizationId)} from this secret store.",
      )
    }
    return result(target, DataplaneDiagnosticStatus.PASSED)
  }

  fun timedOut(target: SecretStorageProbeTarget): DataplaneDiagnosticResult =
    result(target, DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE, TIMED_OUT, "The secret store did not respond in time.")

  private fun result(
    target: SecretStorageProbeTarget,
    status: DataplaneDiagnosticStatus,
    errorCode: String? = null,
    errorMessage: String? = null,
  ) = DataplaneDiagnosticResult(
    kind = DataplaneDiagnosticKind.SECRET_STORAGE,
    targetId = target.secretStorageId,
    status = status,
    checkedAt = clock.millis(),
    errorCode = errorCode,
    errorMessage = errorMessage,
    observation = mapOf("scope_type" to target.scopeType.value, "scope_id" to target.scopeId.toString()),
  )

  companion object {
    const val STORE_UNAVAILABLE = "STORE_UNAVAILABLE"
    const val READ_FAILED = "READ_FAILED"
    const val TEST_SECRET_UNVERIFIED = "TEST_SECRET_UNVERIFIED"
    const val TIMED_OUT = "TIMED_OUT"

    fun testSecretName(organizationId: UUID) = "airbyte-diagnostic-$organizationId"
  }
}
