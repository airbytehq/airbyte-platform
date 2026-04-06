/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.data.repositories.PrivateLinkRepository
import io.airbyte.data.services.impls.data.mappers.toDomainModel
import io.airbyte.data.services.impls.data.mappers.toEntity
import io.airbyte.data.services.impls.data.mappers.toEntityEnum
import io.airbyte.domain.models.PrivateLink
import io.airbyte.domain.models.PrivateLinkStatus
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.util.UUID

private val logger = KotlinLogging.logger {}

@Singleton
class PrivateLinkService(
  private val repository: PrivateLinkRepository,
) {
  fun createPrivateLink(
    workspaceId: UUID,
    dataplaneGroupId: UUID,
    name: String,
    serviceRegion: String,
    serviceName: String,
  ): PrivateLink {
    val privateLink =
      PrivateLink(
        workspaceId = workspaceId,
        dataplaneGroupId = dataplaneGroupId,
        name = name,
        status = PrivateLinkStatus.CREATING,
        serviceRegion = serviceRegion,
        serviceName = serviceName,
      )

    val savedEntity = repository.save(privateLink.toEntity())
    logger.info { "Created private link ${savedEntity.id} for workspace $workspaceId" }
    return savedEntity.toDomainModel()
  }

  fun getPrivateLink(id: UUID): PrivateLink {
    val entity = repository.findById(id).orElseThrow { NoSuchElementException("Private link $id not found") }
    return entity.toDomainModel()
  }

  fun listByWorkspaceId(workspaceId: UUID): List<PrivateLink> = repository.findByWorkspaceId(workspaceId).map { it.toDomainModel() }

  fun updatePrivateLink(
    id: UUID,
    endpointId: String? = null,
    dnsName: String? = null,
    scopedConfigurationId: UUID? = null,
    status: PrivateLinkStatus? = null,
  ): PrivateLink {
    val entity = repository.findById(id).orElseThrow { NoSuchElementException("Private link $id not found") }
    endpointId?.let { entity.endpointId = it }
    dnsName?.let { entity.dnsName = it }
    scopedConfigurationId?.let { entity.scopedConfigurationId = it }
    status?.let { entity.status = it.toEntityEnum() }
    return repository.update(entity).toDomainModel()
  }

  fun clearScopedConfigurationId(id: UUID): PrivateLink {
    val entity = repository.findById(id).orElseThrow { NoSuchElementException("Private link $id not found") }
    entity.scopedConfigurationId = null
    return repository.update(entity).toDomainModel()
  }

  fun deletePrivateLink(id: UUID) {
    repository.deleteById(id)
    logger.info { "Deleted private link $id" }
  }
}
