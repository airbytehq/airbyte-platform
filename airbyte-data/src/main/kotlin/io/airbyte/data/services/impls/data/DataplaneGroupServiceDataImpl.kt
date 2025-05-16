/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.commons.constants.AUTO_DATAPLANE_GROUP
import io.airbyte.commons.constants.DEFAULT_ORGANIZATION_ID
import io.airbyte.commons.constants.US_DATAPLANE_GROUP
import io.airbyte.config.ConfigSchema
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.config.DataplaneGroup
import io.airbyte.data.exceptions.ConfigNotFoundException
import io.airbyte.data.repositories.DataplaneGroupRepository
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.impls.data.mappers.DataplaneGroupMapper.toConfigModel
import io.airbyte.data.services.impls.data.mappers.DataplaneGroupMapper.toEntity
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.transaction.annotation.Transactional
import jakarta.inject.Singleton
import java.util.UUID

private val logger = KotlinLogging.logger {}

@Singleton
open class DataplaneGroupServiceDataImpl(
  private val repository: DataplaneGroupRepository,
) : DataplaneGroupService {
  override fun getDataplaneGroup(id: UUID): DataplaneGroup =
    repository
      .findById(id)
      .orElseThrow {
        ConfigNotFoundException(ConfigSchema.DATAPLANE_GROUP, id)
      }.toConfigModel()

  override fun getDataplaneGroupByOrganizationIdAndName(
    organizationId: UUID,
    name: String,
  ): DataplaneGroup =
    repository
      .findAllByOrganizationIdAndNameIgnoreCase(organizationId, name)
      .ifEmpty {
        listOf(
          repository
            .findAllByOrganizationIdAndNameIgnoreCase(DEFAULT_ORGANIZATION_ID, name)
            // We have a uniqueness constraint on (organizationId, name) so can just return the first
            .first(),
        )
      }.first()
      .toConfigModel()

  @Transactional("config")
  override fun writeDataplaneGroup(dataplaneGroup: DataplaneGroup): DataplaneGroup {
    validateDataplaneGroupName(dataplaneGroup)

    val entity = dataplaneGroup.toEntity()

    if (dataplaneGroup.id != null && repository.existsById(dataplaneGroup.id)) {
      logger.info { "Updating existing dataplane group: dataplaneGroup=$dataplaneGroup entity=$entity" }
      return repository.update(entity).toConfigModel()
    }
    logger.info { "Creating new dataplane group: dataplaneGroup=$dataplaneGroup entity=$entity" }
    return repository.save(entity).toConfigModel()
  }

  override fun listDataplaneGroups(
    organizationIds: List<UUID>,
    withTombstone: Boolean,
  ): List<DataplaneGroup> =
    if (withTombstone) {
      repository
        .findAllByOrganizationIdInOrderByUpdatedAtDesc(
          organizationIds,
        ).map { unit ->
          unit.toConfigModel()
        }
    } else {
      repository
        .findAllByOrganizationIdInAndTombstoneFalseOrderByUpdatedAtDesc(
          organizationIds,
        ).map { unit ->
          unit.toConfigModel()
        }
    }

  override fun getDefaultDataplaneGroupForAirbyteEdition(airbyteEdition: AirbyteEdition): DataplaneGroup =
    if (airbyteEdition == AirbyteEdition.CLOUD) {
      getDataplaneGroupByOrganizationIdAndName(DEFAULT_ORGANIZATION_ID, US_DATAPLANE_GROUP)
    } else {
      getDataplaneGroupByOrganizationIdAndName(DEFAULT_ORGANIZATION_ID, AUTO_DATAPLANE_GROUP)
    }

  fun validateDataplaneGroupName(dataplaneGroup: DataplaneGroup) {
    if (dataplaneGroup.organizationId != DEFAULT_ORGANIZATION_ID) {
      val defaultGroups = listDataplaneGroups(listOf(DEFAULT_ORGANIZATION_ID), false)
      val reservedNames = defaultGroups.map { it.name }.toSet()

      if (dataplaneGroup.name in reservedNames) {
        throw RuntimeException(
          "Dataplane group name conflicts with a default group name. " +
            "dataplaneGroup.id=${dataplaneGroup.id} dataplaneGroup.name=${dataplaneGroup.name}",
        )
      }
    }
  }
}
