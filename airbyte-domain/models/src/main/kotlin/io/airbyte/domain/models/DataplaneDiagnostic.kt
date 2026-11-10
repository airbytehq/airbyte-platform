/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.models

import java.time.OffsetDateTime
import java.util.UUID

/**
 * One completed health check reported by a data plane, such as reading from a secret store.
 */
data class DataplaneDiagnostic(
  val kind: DataplaneDiagnosticKind,
  /**
   * What was checked, such as a secret store ID.
   */
  val targetId: UUID,
  val outcome: DataplaneDiagnosticOutcome,
  val checkedAt: OffsetDateTime,
  /**
   * Details specific to the kind of check. Empty when the data plane sent none.
   */
  val observation: Map<String, Any>,
)

/**
 * A check result as saved for a data plane. [updatedAt] is when Airbyte last received it.
 */
data class SavedDataplaneDiagnostic(
  val dataplaneId: UUID,
  val diagnostic: DataplaneDiagnostic,
  val updatedAt: OffsetDateTime,
)

enum class DataplaneDiagnosticKind {
  SECRET_STORAGE,
  STORAGE_REACHABILITY,
}

sealed interface DataplaneDiagnosticOutcome {
  data object Passed : DataplaneDiagnosticOutcome

  data class Failed(
    val errorCode: String,
    val errorMessage: String,
  ) : DataplaneDiagnosticOutcome

  /**
   * The check could not run to the end, so it says nothing about whether the target works.
   */
  data class UnableToComplete(
    val errorCode: String,
    val errorMessage: String,
  ) : DataplaneDiagnosticOutcome

  /**
   * There was nothing to check, such as no log storage set up.
   */
  data object NotConfigured : DataplaneDiagnosticOutcome
}
