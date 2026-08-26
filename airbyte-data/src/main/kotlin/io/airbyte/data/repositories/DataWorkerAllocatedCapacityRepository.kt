/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.DataWorkerAllocatedCapacity
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.CrudRepository
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface DataWorkerAllocatedCapacityRepository : CrudRepository<DataWorkerAllocatedCapacity, UUID> {
  fun findByOrganizationId(organizationId: UUID): List<DataWorkerAllocatedCapacity>

  fun findByOrganizationIdAndDataplaneGroupId(
    organizationId: UUID,
    dataplaneGroupId: UUID,
  ): DataWorkerAllocatedCapacity?

  /**
   * Subtracts [amount] from a region's allocated capacity, but only if that region currently holds
   * at least that much.
   *
   * The sufficiency check lives in the WHERE clause.
   * A concurrent update to the same row causes Postgres to re-evaluate this predicate
   * against the committed version, so a row that no longer has enough capacity stops
   * matching instead of going negative.
   *
   * @return 1 when the subtraction was applied, 0 when the row is missing or holds less than
   *         [amount]. Callers cannot distinguish those two cases from the return value alone and
   *         must read the row if they need to tell them apart.
   */
  @Query(
    """
      UPDATE data_worker_allocated_capacity
      SET allocated_capacity = allocated_capacity - :amount,
          updated_at = NOW()
      WHERE organization_id = :organizationId
        AND dataplane_group_id = :dataplaneGroupId
        AND allocated_capacity >= :amount
    """,
  )
  fun subtractCapacityIfSufficient(
    organizationId: UUID,
    dataplaneGroupId: UUID,
    amount: Double,
  ): Int

  /**
   * Adds [amount] to a region's allocated capacity, creating the row when the organization has no
   * capacity in that region yet.
   *
   * The Stigg backfill gives every organization a single region, so the first move into any other
   * region has nothing to update.
   *
   * The increment is relative (`allocated_capacity + :amount`) rather than absolute so two
   * concurrent additions to the same region both land instead of one overwriting the other.
   *
   * @return the number of rows inserted or updated, always 1.
   */
  @Query(
    """
      INSERT INTO data_worker_allocated_capacity (
        id, organization_id, dataplane_group_id, allocated_capacity
      ) VALUES (
        gen_random_uuid(), :organizationId, :dataplaneGroupId, :amount
      )
      ON CONFLICT (organization_id, dataplane_group_id)
      DO UPDATE SET allocated_capacity = data_worker_allocated_capacity.allocated_capacity + :amount,
                    updated_at = NOW()
    """,
  )
  fun addCapacity(
    organizationId: UUID,
    dataplaneGroupId: UUID,
    amount: Double,
  ): Int
}
