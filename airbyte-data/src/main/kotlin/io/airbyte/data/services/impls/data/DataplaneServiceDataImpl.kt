/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.config.Dataplane
import io.airbyte.data.exceptions.ConfigNotFoundException
import io.airbyte.data.repositories.DataplaneRepository
import io.airbyte.data.services.DataplaneService
import io.airbyte.data.services.impls.data.mappers.DataplaneMapper.toConfigModel
import io.airbyte.data.services.impls.data.mappers.DataplaneMapper.toEntity
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.util.UUID

private val logger = KotlinLogging.logger {}

@Singleton
class DataplaneServiceDataImpl(
  private val repository: DataplaneRepository,
) : DataplaneService {
  override fun getDataplane(id: UUID): Dataplane =
    repository
      .findById(id)
      .orElseThrow {
        ConfigNotFoundException("ConnectorRollout", id.toString())
      }.toConfigModel()

  override fun writeDataplane(dataplane: Dataplane): Dataplane {
    val entity = dataplane.toEntity()

    if (dataplane.id != null && repository.existsById(dataplane.id)) {
      logger.info { "Updating existing dataplane: dataplane=$dataplane entity=$entity" }
      return repository.update(entity).toConfigModel()
    }
    logger.info { "Creating new dataplane: dataplane=$dataplane entity=$entity" }
    return repository.save(entity).toConfigModel()
  }

  override fun listDataplanes(
    dataplaneGroupId: UUID,
    withTombstone: Boolean,
  ): List<Dataplane> =
    if (withTombstone) {
      repository
        .findAllByDataplaneGroupIdOrderByUpdatedAtDesc(
          dataplaneGroupId,
        ).map { unit ->
          unit.toConfigModel()
        }
    } else {
      repository
        .findAllByDataplaneGroupIdAndTombstoneFalseOrderByUpdatedAtDesc(
          dataplaneGroupId,
        ).map { unit ->
          unit.toConfigModel()
        }
    }

  override fun listDataplanes(withTombstone: Boolean): List<Dataplane> =
    repository.findAllByTombstone(withTombstone).map { unit ->
      unit.toConfigModel()
    }
}
