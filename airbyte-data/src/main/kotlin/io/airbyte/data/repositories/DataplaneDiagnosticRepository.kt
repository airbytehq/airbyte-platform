/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.DataplaneDiagnostic
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.time.OffsetDateTime
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface DataplaneDiagnosticRepository : PageableRepository<DataplaneDiagnostic, UUID> {
  /**
   * Keeps one row per data plane, kind and target. An older result than the saved one is ignored, so
   * a late heartbeat can't overwrite a newer result. The launcher resends the same result on every
   * heartbeat, so equal checked_at still updates updated_at.
   */
  @Query(
    """
    INSERT INTO dataplane_diagnostic (
      id, dataplane_id, organization_id, diagnostic_kind, target_id, status, checked_at,
      error_code, error_message, observation, created_at, updated_at
    ) VALUES (
      gen_random_uuid(), :dataplaneId, :organizationId, :diagnosticKind, :targetId, :status, :checkedAt,
      :errorCode, :errorMessage, CAST(:observation AS jsonb), now(), now()
    )
    ON CONFLICT (dataplane_id, diagnostic_kind, target_id) DO UPDATE SET
      organization_id = EXCLUDED.organization_id,
      status = EXCLUDED.status,
      checked_at = EXCLUDED.checked_at,
      error_code = EXCLUDED.error_code,
      error_message = EXCLUDED.error_message,
      observation = EXCLUDED.observation,
      updated_at = now()
    WHERE dataplane_diagnostic.checked_at <= EXCLUDED.checked_at
    """,
  )
  fun upsertLatest(
    dataplaneId: UUID,
    organizationId: UUID,
    diagnosticKind: String,
    targetId: UUID,
    status: String,
    checkedAt: OffsetDateTime,
    errorCode: String?,
    errorMessage: String?,
    observation: String?,
  )

  /**
   * Saves a failure Airbyte observed itself, such as a rejected token exchange.
   *
   * ```
   * new failure at checkedAt
   *   |
   *   +-- no row saved yet? ------------------------------ yes --> insert it
   *   +-- saved row newer than checkedAt? ---------------- yes --> keep the saved row
   *   +-- saved row PASSED? ------------------------------ yes --> replace it
   *   +-- same association, saved before the interval? --- yes --> replace it (retries write once per interval)
   *   +-- weaker association? ---------------------------- yes --> replace it
   *   |
   *   +-- otherwise --> keep the saved row
   * ```
   *
   * "Before the interval" means before [repeatedFailureBefore]. A weaker association is one in
   * [replaceableFailureAssociations] other than this failure's own.
   */
  @Query(
    """
    INSERT INTO dataplane_diagnostic AS saved (
      id, dataplane_id, organization_id, diagnostic_kind, target_id, status, checked_at,
      error_code, error_message, observation, created_at, updated_at
    )
    VALUES (
      gen_random_uuid(), :dataplaneId, :organizationId, :diagnosticKind, :targetId, :failedStatus, :checkedAt,
      :errorCode, :errorMessage, CAST(:observation AS jsonb), now(), now()
    )
    -- EXCLUDED is the failure this statement tried to insert
    ON CONFLICT (dataplane_id, diagnostic_kind, target_id) DO UPDATE SET
      organization_id = EXCLUDED.organization_id,
      status = EXCLUDED.status,
      checked_at = EXCLUDED.checked_at,
      error_code = EXCLUDED.error_code,
      error_message = EXCLUDED.error_message,
      observation = EXCLUDED.observation,
      updated_at = now()
    -- never overwrite a newer result
    WHERE saved.checked_at <= EXCLUDED.checked_at
      AND (
        -- a passed result
        saved.status <> :failedStatus
        -- the same association, once the interval is over
        OR (
          saved.observation ->> 'association' = EXCLUDED.observation ->> 'association'
          AND saved.checked_at < :repeatedFailureBefore
        )
        -- a weaker association
        OR (
          saved.observation ->> 'association' <> EXCLUDED.observation ->> 'association'
          AND saved.observation ->> 'association' IN (:replaceableFailureAssociations)
        )
      )
    """,
  )
  fun upsertObservedFailure(
    dataplaneId: UUID,
    organizationId: UUID,
    diagnosticKind: String,
    targetId: UUID,
    failedStatus: String,
    checkedAt: OffsetDateTime,
    repeatedFailureBefore: OffsetDateTime,
    replaceableFailureAssociations: List<String>,
    errorCode: String,
    errorMessage: String,
    observation: String,
  )

  /**
   * Turns a failed result into [passedStatus] in one statement, so only the first call after a failure changes the row and
   * later calls keep its checked_at. A failure saved after [checkedAt] stays, so a check-in that came before a rejection
   * can't clear it.
   */
  @Query(
    """
    UPDATE dataplane_diagnostic SET
      status = :passedStatus,
      checked_at = :checkedAt,
      error_code = NULL,
      error_message = NULL,
      updated_at = now()
    WHERE dataplane_id = :dataplaneId
      AND organization_id = :organizationId
      AND diagnostic_kind = :diagnosticKind
      AND target_id = :targetId
      AND status = :failedStatus
      AND checked_at <= :checkedAt
    """,
  )
  fun markFailedAsPassed(
    dataplaneId: UUID,
    organizationId: UUID,
    diagnosticKind: String,
    targetId: UUID,
    failedStatus: String,
    passedStatus: String,
    checkedAt: OffsetDateTime,
  ): Int

  /**
   * Deletes the data plane's results of one kind for targets it no longer reports. Only results checked
   * before [checkedBefore] are deleted, so a late heartbeat can't delete newer results.
   */
  @Query(
    """
    DELETE FROM dataplane_diagnostic
    WHERE dataplane_id = :dataplaneId
      AND diagnostic_kind = :diagnosticKind
      AND target_id NOT IN (:reportedTargetIds)
      AND checked_at < :checkedBefore
    """,
  )
  fun deleteUnreportedTargets(
    dataplaneId: UUID,
    diagnosticKind: String,
    reportedTargetIds: Collection<UUID>,
    checkedBefore: OffsetDateTime,
  ): Int

  fun findByOrganizationIdAndDataplaneIdIn(
    organizationId: UUID,
    dataplaneIds: List<UUID>,
  ): List<DataplaneDiagnostic>
}
