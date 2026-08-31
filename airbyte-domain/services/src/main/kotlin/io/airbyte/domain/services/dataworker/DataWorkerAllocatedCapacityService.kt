/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.dataworker

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.StateConflictProblem
import io.airbyte.data.repositories.DataWorkerAllocatedCapacityRepository
import io.airbyte.data.repositories.DataplaneGroupRepository
import io.airbyte.data.repositories.entities.DataWorkerAllocatedCapacity
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.domain.models.DataplaneGroupId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.dataworker.DataWorkerAllocation
import io.airbyte.domain.models.dataworker.OrganizationDataWorkerAllocations
import io.micronaut.transaction.TransactionOperations
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.sql.Connection

/**
 * The capacity change an amount is being validated for. Renders lowercase so it reads as the verb
 * in the rejection message.
 */
private enum class CapacityOperation {
  ADD,
  REMOVE,
  REALLOCATE,
  ;

  override fun toString(): String = name.lowercase()
}

/**
 * Reads and edits the Data Worker capacity an organization holds in each region.
 *
 * This is the organization's contract, not its live usage: the numbers here change when someone
 * deliberately reallocates capacity between regions, not when jobs start and stop. Admission
 * control against those numbers lives in [DataWorkerCapacityService].
 */
@Singleton
open class DataWorkerAllocatedCapacityService(
  private val dataWorkerAllocatedCapacityRepository: DataWorkerAllocatedCapacityRepository,
  private val dataplaneGroupRepository: DataplaneGroupRepository,
  private val dataplaneGroupService: DataplaneGroupService,
  @param:Named("config") private val configTransactionOperations: TransactionOperations<Connection>,
) {
  /**
   * Every region in which the organization holds capacity, and the total across them.
   *
   * Regions the organization has never been allocated capacity in are absent rather than present
   * with zero, because no row exists for them. An organization with no rows at all is a real
   * state, reported as an empty list and a total of zero rather than as an error.
   */
  open fun getAllocations(organizationId: OrganizationId): OrganizationDataWorkerAllocations {
    val allocations =
      dataWorkerAllocatedCapacityRepository
        .findByOrganizationId(organizationId.value)
        .map { it.toAllocation() }

    return OrganizationDataWorkerAllocations(
      organizationId = organizationId,
      totalAllocatedCapacity = allocations.sumOf { it.allocatedCapacity },
      allocations = allocations,
    )
  }

  /**
   * The organization's capacity in a single region, or null when it holds none there.
   */
  open fun getAllocation(
    organizationId: OrganizationId,
    dataplaneGroupId: DataplaneGroupId,
  ): DataWorkerAllocation? =
    dataWorkerAllocatedCapacityRepository
      .findByOrganizationIdAndDataplaneGroupId(organizationId.value, dataplaneGroupId.value)
      ?.toAllocation()

  /**
   * Reallocates [amount] of capacity from one of the organization's regions to another.
   *
   * The subtraction and the addition are one transaction, so the organization's total capacity is
   * never observed mid-move.
   *
   * @throws BadRequestProblem if the two regions are the same, the amount is not a positive finite
   *   number the capacity column can hold, either region does not exist, or the destination region
   *   has been deleted.
   * @throws StateConflictProblem if the source region holds less than [amount].
   */
  open fun reallocateCapacity(
    organizationId: OrganizationId,
    from: DataplaneGroupId,
    to: DataplaneGroupId,
    amount: Double,
  ) {
    if (from == to) {
      throw BadRequestProblem(ProblemMessageData().message("Source and destination regions must be different."))
    }
    requireValidAmount(amount, CapacityOperation.REALLOCATE)

    requireRegionExists(from)
    requireDestinationRegionIsUsable(to)

    configTransactionOperations.executeWrite { _ ->
      // Both statements in a move touch the same two rows, so two moves in opposite directions
      // between the same pair of regions would deadlock if each grabbed its own source first.
      // Ordering by region id makes every concurrent move agree on which row to take first.
      if (from.value < to.value) {
        subtractOrThrow(organizationId, from, amount)
        dataWorkerAllocatedCapacityRepository.addCapacity(organizationId.value, to.value, amount)
      } else {
        dataWorkerAllocatedCapacityRepository.addCapacity(organizationId.value, to.value, amount)
        subtractOrThrow(organizationId, from, amount)
      }
    }
  }

  /**
   * Raises the organization's total capacity by [amount].
   *
   * This changes what the organization holds overall, so it added by the admin user to an organization.
   * Capacity is stored per region, so the new capacity goes to the default region.
   * Use [reallocateCapacity] afterwards to move capacity across regions.
   *
   * @throws BadRequestProblem if the amount is not a positive finite number the capacity column can
   *   hold.
   */
  open fun addCapacity(
    organizationId: OrganizationId,
    amount: Double,
  ) {
    requireValidAmount(amount, CapacityOperation.ADD)

    // The default region is configuration rather than caller input, so it is not validated the way
    // a supplied region is — a missing default is our own misconfiguration, not a bad request.
    val defaultRegionId = dataplaneGroupService.getDefaultDataplaneGroup().id

    dataWorkerAllocatedCapacityRepository.addCapacity(organizationId.value, defaultRegionId, amount)
  }

  /**
   * This removed the organization's total capacity by [amount], taking it from [dataplaneGroupId].
   *
   * The region is required here because this capacity is already allocated,
   * and only the caller knows which region it should come out of.
   *
   * @throws BadRequestProblem if the amount is not a positive finite number the capacity column can
   *   hold, the region does not exist, or the region holds less than [amount].
   */
  open fun removeCapacity(
    organizationId: OrganizationId,
    dataplaneGroupId: DataplaneGroupId,
    amount: Double,
  ) {
    requireValidAmount(amount, CapacityOperation.REMOVE)
    requireRegionExists(dataplaneGroupId)

    val updated =
      dataWorkerAllocatedCapacityRepository.subtractCapacityIfSufficient(
        organizationId.value,
        dataplaneGroupId.value,
        amount,
      )
    // The repository cannot tell "deficit capacity" apart from "no capacity", so this
    // message covers both rather than claiming which one happened.
    if (updated == 0) {
      throw BadRequestProblem(
        ProblemMessageData().message(
          "Region ${dataplaneGroupId.value} does not have $amount Data Workers of capacity available to remove.",
        ),
      )
    }
  }

  /**
   * Rejects an amount the capacity column cannot hold.
   *
   * NaN fails every comparison, so `amount <= 0` alone would let it through and poison the row.
   * The upper bound is the column's: `allocated_capacity` is REAL, and where the write happens
   * before any sufficiency check, an amount past what float4 holds reaches Postgres and fails there
   * as a 500 instead of here as a 400.
   */
  private fun requireValidAmount(
    amount: Double,
    operation: CapacityOperation,
  ) {
    if (!amount.isFinite() || amount <= 0 || amount > Float.MAX_VALUE) {
      throw BadRequestProblem(
        ProblemMessageData().message(
          "Amount to $operation must be a positive number no greater than ${Float.MAX_VALUE}, but was $amount.",
        ),
      )
    }
  }

  /**
   * Subtracts from the source region, failing the whole transaction if it has too little.
   *
   * The repository reports 0 rows changed both when the region holds too little and when the
   * organization has no capacity there at all; neither is a state a move can proceed from.
   */
  private fun subtractOrThrow(
    organizationId: OrganizationId,
    from: DataplaneGroupId,
    amount: Double,
  ) {
    val updated =
      dataWorkerAllocatedCapacityRepository.subtractCapacityIfSufficient(
        organizationId.value,
        from.value,
        amount,
      )
    if (updated == 0) {
      throw StateConflictProblem(
        ProblemMessageData().message(
          "Region ${from.value} does not have $amount Data Workers of capacity available to move.",
        ),
      )
    }
  }

  /**
   * The allocation table has no foreign key to `dataplane_group`, so an unrecognized region id
   * would otherwise create capacity in a region that does not exist.
   */
  private fun requireRegionExists(dataplaneGroupId: DataplaneGroupId) {
    if (dataplaneGroupRepository.findById(dataplaneGroupId.value).isEmpty) {
      throw BadRequestProblem(ProblemMessageData().message("Region ${dataplaneGroupId.value} does not exist."))
    }
  }

  /**
   * A deleted region may still hold capacity that needs moving out of it, so only the destination
   * is checked for deletion.
   */
  private fun requireDestinationRegionIsUsable(dataplaneGroupId: DataplaneGroupId) {
    val region =
      dataplaneGroupRepository.findById(dataplaneGroupId.value).orElse(null)
        ?: throw BadRequestProblem(ProblemMessageData().message("Region ${dataplaneGroupId.value} does not exist."))
    if (region.tombstone) {
      throw BadRequestProblem(ProblemMessageData().message("Destination region ${dataplaneGroupId.value} has been deleted."))
    }
  }

  private fun DataWorkerAllocatedCapacity.toAllocation() =
    DataWorkerAllocation(
      dataplaneGroupId = DataplaneGroupId(dataplaneGroupId),
      allocatedCapacity = allocatedCapacity,
    )
}
