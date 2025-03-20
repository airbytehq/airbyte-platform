/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.config.ConfigSchema
import io.airbyte.config.DataplaneGroup
import io.airbyte.config.Geography
import io.airbyte.data.config.DEFAULT_ORGANIZATION_ID
import io.airbyte.data.exceptions.ConfigNotFoundException
import io.airbyte.data.repositories.DataplaneGroupRepository
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.airbyte.data.services.impls.data.mappers.toEntity
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.util.UUID

private val logger = KotlinLogging.logger {}

@Singleton
class DataplaneGroupServiceDataImpl(
  private val repository: DataplaneGroupRepository,
) : DataplaneGroupService {
  override fun getDataplaneGroup(id: UUID): DataplaneGroup =
    repository
      .findById(id)
      .orElseThrow {
        ConfigNotFoundException(ConfigSchema.DATAPLANE_GROUP, id)
      }.toConfigModel()

  override fun getDataplaneGroupByOrganizationIdAndGeography(
    organizationId: UUID,
    geography: Geography,
  ): DataplaneGroup =
    repository
      .findAllByOrganizationIdAndName(organizationId, geography.name)
      .ifEmpty { listOf(repository.findAllByOrganizationIdAndName(DEFAULT_ORGANIZATION_ID, geography.name).first()) }
      // We have a uniqueness constraint on (organizationId, name) so can just return the first
      .first()
      .toConfigModel()

  override fun writeDataplaneGroup(dataplaneGroup: DataplaneGroup): DataplaneGroup {
    val entity = dataplaneGroup.toEntity()

    if (dataplaneGroup.id != null && repository.existsById(dataplaneGroup.id)) {
      logger.info { "Updating existing dataplane group: dataplaneGroup=$dataplaneGroup entity=$entity" }
      return repository.update(entity).toConfigModel()
    }
    logger.info { "Creating new dataplane group: dataplaneGroup=$dataplaneGroup entity=$entity" }
    return repository.save(entity).toConfigModel()
  }

  override fun listDataplaneGroups(
    organizationId: UUID,
    withTombstone: Boolean,
  ): List<DataplaneGroup> =
    if (withTombstone) {
      repository
        .findAllByOrganizationIdOrderByUpdatedAtDesc(
          organizationId,
        ).map { unit ->
          unit.toConfigModel()
        }
    } else {
      repository
        .findAllByOrganizationIdAndTombstoneFalseOrderByUpdatedAtDesc(
          organizationId,
        ).map { unit ->
          unit.toConfigModel()
        }
    }
}
