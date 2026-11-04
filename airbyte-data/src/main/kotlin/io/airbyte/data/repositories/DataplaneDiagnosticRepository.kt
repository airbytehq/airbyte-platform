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

  fun findByOrganizationIdAndDataplaneIdIn(
    organizationId: UUID,
    dataplaneIds: List<UUID>,
  ): List<DataplaneDiagnostic>
}
