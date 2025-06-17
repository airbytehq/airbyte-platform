/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.config.ConfigSchema
import io.airbyte.config.Dataplane
import io.airbyte.config.Permission
import io.airbyte.data.exceptions.ConfigNotFoundException
import io.airbyte.data.repositories.DataplaneGroupRepository
import io.airbyte.data.repositories.DataplaneRepository
import io.airbyte.data.services.DataplaneService
import io.airbyte.data.services.PermissionDao
import io.airbyte.data.services.ServiceAccountsService
import io.airbyte.data.services.impls.data.mappers.DataplaneMapper.toConfigModel
import io.airbyte.data.services.impls.data.mappers.DataplaneMapper.toEntity
import io.airbyte.data.services.shared.DataplaneWithServiceAccount
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.transaction.annotation.Transactional
import jakarta.inject.Singleton
import java.util.UUID

private val logger = KotlinLogging.logger {}

@Singleton
open class DataplaneServiceDataImpl(
  private val repository: DataplaneRepository,
  private val groupRepository: DataplaneGroupRepository,
  private val serviceAccountsService: ServiceAccountsService,
  private val permissionDao: PermissionDao,
) : DataplaneService {
  override fun getDataplane(id: UUID): Dataplane =
    repository
      .findById(id)
      .orElseThrow {
        ConfigNotFoundException("Dataplane", id.toString())
      }.toConfigModel()

  override fun updateDataplane(dataplane: Dataplane): Dataplane {
    val entity = dataplane.toEntity()

    if (dataplane.id != null && repository.existsById(dataplane.id)) {
      logger.info { "Updating existing dataplane: dataplane=$dataplane entity=$entity" }
      return repository.update(entity).toConfigModel()
    }
    logger.info { "Creating new dataplane: dataplane=$dataplane entity=$entity" }
    return repository.save(entity).toConfigModel()
  }

  @Transactional("config")
  override fun createDataplaneAndServiceAccount(
    dataplane: Dataplane,
    instanceScope: Boolean,
  ): DataplaneWithServiceAccount {
    if (dataplane.id == null) {
      throw DataplaneIdMissingException("Dataplane is missing an id, cannot create")
    }

    if (repository.existsById(dataplane.id)) {
      throw DataplaneAlreadyExistsException("Dataplane with id ${dataplane.id} already exists, cannot create")
    }

    val group =
      groupRepository.findById(dataplane.dataplaneGroupId).orElseThrow {
        ConfigNotFoundException(ConfigSchema.DATAPLANE_GROUP, dataplane.dataplaneGroupId.toString())
      }

    val serviceAccountName = "dataplane-${dataplane.id}"
    logger.info { "Creating dataplane service account: name=$serviceAccountName" }
    val serviceAccount = serviceAccountsService.create(name = serviceAccountName, managed = true)

    dataplane.serviceAccountId = serviceAccount.id
    val entity = dataplane.toEntity()
    logger.info { "Creating new dataplane: dataplane=$dataplane entity=$entity" }
    val dataplaneConfigModel = repository.save(entity).toConfigModel()

    // we must grant the newly created service account the dataplane permission as well
    val perm =
      Permission()
        .withPermissionId(UUID.randomUUID())
        .withServiceAccountId(serviceAccount.id)
        .withPermissionType(Permission.PermissionType.DATAPLANE)

    if (!instanceScope) {
      perm.withOrganizationId(group.organizationId)
    }
    permissionDao.createServiceAccountPermission(perm)

    return DataplaneWithServiceAccount(dataplaneConfigModel, serviceAccount)
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

  override fun getDataplaneByServiceAccountId(serviceAccountId: String): Dataplane? =
    repository.findByServiceAccountId(UUID.fromString(serviceAccountId))?.toConfigModel()
}

class DataplaneAlreadyExistsException(
  message: String,
) : Exception(message)

class DataplaneIdMissingException(
  message: String,
) : Exception(message)
