/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.diagnostics

import io.airbyte.api.client.model.generated.DataplaneDiagnosticKind
import io.airbyte.api.client.model.generated.DataplaneDiagnosticResult
import io.airbyte.api.client.model.generated.DataplaneDiagnosticStatus
import io.airbyte.api.client.model.generated.ScopeType
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
import io.airbyte.workers.hydration.ConnectorSecretsHydrator
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.exceptions.BeanContextException
import jakarta.inject.Provider
import jakarta.inject.Singleton
import java.time.Clock
import java.util.UUID

private val logger = KotlinLogging.logger {}

/**
 * Reads the organization's test secret from one secret store, creating it first when it's missing.
 */
@Singleton
class SecretStorageChecker(
  // Built on first use, so a data plane with broken secrets settings still starts and reports it here.
  private val connectorSecretsHydrator: Provider<ConnectorSecretsHydrator>,
  private val clock: Clock,
) {
  // Every failure is reported as that store's result instead of thrown, so one bad store doesn't hide the others.
  fun check(
    target: SecretStorageProbeTarget,
    organizationId: UUID,
  ): DataplaneDiagnosticResult {
    val hydrator =
      try {
        connectorSecretsHydrator.get()
      } catch (e: BeanContextException) {
        logger.warn(e) { "Could not build the data plane's secrets client" }
        return result(
          target,
          DataplaneDiagnosticStatus.FAILED,
          DATAPLANE_SECRETS_NOT_CONFIGURED,
          "The data plane's secrets settings are missing or invalid, so it can't read secrets. Check the secrets manager in its Helm values.",
        )
      }

    val persistence =
      try {
        hydrator.getPersistenceByStorageId(target.secretStorageId)
      } catch (e: Exception) {
        logger.warn(e) { "Could not load secret store ${target.secretStorageId}" }
        return result(target, DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE, STORE_UNAVAILABLE, "Airbyte could not load this secret store's settings.")
      }

    val testSecret = testSecret(organizationId)
    val value =
      try {
        persistence.read(testSecret)
      } catch (e: Exception) {
        return readFailed(target, e)
      }
    if (value.isNotEmpty()) {
      return result(target, DataplaneDiagnosticStatus.PASSED)
    }

    try {
      persistence.write(testSecret, testSecretValue(organizationId))
    } catch (e: Exception) {
      logger.warn(e) { "Could not create the test secret in secret store ${target.secretStorageId}" }
      return result(
        target,
        DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE,
        TEST_SECRET_CREATE_FAILED,
        "Airbyte could not create the test secret ${testSecret.fullCoordinate} in this secret store.",
      )
    }

    val valueAfterCreate =
      try {
        persistence.read(testSecret)
      } catch (e: Exception) {
        return readFailed(target, e)
      }
    // Some stores return an empty value for a denied read, and some ignore a failed write, so this can't count as a pass.
    if (valueAfterCreate.isEmpty()) {
      return result(
        target,
        DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE,
        TEST_SECRET_UNVERIFIED,
        "Airbyte could not confirm that the data plane can read the test secret ${testSecret.fullCoordinate} from this secret store.",
      )
    }
    return result(target, DataplaneDiagnosticStatus.PASSED)
  }

  // There's no store to point at, so the result targets the data plane and is scoped to the organization without a store.
  fun notConfigured(
    dataplaneId: UUID,
    organizationId: UUID,
  ): DataplaneDiagnosticResult =
    DataplaneDiagnosticResult(
      kind = DataplaneDiagnosticKind.SECRET_STORAGE.value,
      targetId = dataplaneId,
      status = DataplaneDiagnosticStatus.NOT_CONFIGURED,
      checkedAt = clock.millis(),
      observation = mapOf("scope_type" to ScopeType.ORGANIZATION.value, "scope_id" to organizationId.toString()),
    )

  fun timedOut(target: SecretStorageProbeTarget): DataplaneDiagnosticResult =
    result(target, DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE, TIMED_OUT, "The secret store did not respond in time.")

  private fun readFailed(
    target: SecretStorageProbeTarget,
    e: Exception,
  ): DataplaneDiagnosticResult {
    logger.warn(e) { "Could not read the test secret from secret store ${target.secretStorageId}" }
    return result(target, DataplaneDiagnosticStatus.FAILED, READ_FAILED, "The data plane could not read from this secret store.")
  }

  private fun result(
    target: SecretStorageProbeTarget,
    status: DataplaneDiagnosticStatus,
    errorCode: String? = null,
    errorMessage: String? = null,
  ) = DataplaneDiagnosticResult(
    kind = DataplaneDiagnosticKind.SECRET_STORAGE.value,
    targetId = target.secretStorageId,
    status = status,
    checkedAt = clock.millis(),
    errorCode = errorCode,
    errorMessage = errorMessage,
    observation = mapOf("scope_type" to target.scopeType.value, "scope_id" to target.scopeId.toString()),
  )

  companion object {
    const val STORE_UNAVAILABLE = "STORE_UNAVAILABLE"
    const val DATAPLANE_SECRETS_NOT_CONFIGURED = "DATAPLANE_SECRETS_NOT_CONFIGURED"
    const val READ_FAILED = "READ_FAILED"
    const val TEST_SECRET_UNVERIFIED = "TEST_SECRET_UNVERIFIED"
    const val TEST_SECRET_CREATE_FAILED = "TEST_SECRET_CREATE_FAILED"
    const val TIMED_OUT = "TIMED_OUT"

    fun testSecret(organizationId: UUID) = AirbyteManagedSecretCoordinate("airbyte_diagnostic_$organizationId", 1L)

    fun testSecretValue(organizationId: UUID) = "Organization $organizationId secret storage is reachable"
  }
}
