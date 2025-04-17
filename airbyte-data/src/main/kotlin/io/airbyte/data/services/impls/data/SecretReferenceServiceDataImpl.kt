/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.data.repositories.SecretReferenceRepository
import io.airbyte.data.repositories.SecretReferenceWithConfigRepository
import io.airbyte.data.services.SecretReferenceService
import io.airbyte.data.services.impls.data.mappers.SecretReferenceMapper.toConfigModel
import io.airbyte.data.services.impls.data.mappers.SecretReferenceMapper.toEntity
import io.airbyte.data.services.impls.data.mappers.SecretReferenceWithConfigMapper.toConfigModel
import io.airbyte.domain.models.SecretReference
import io.airbyte.domain.models.SecretReferenceCreate
import io.airbyte.domain.models.SecretReferenceId
import io.airbyte.domain.models.SecretReferenceScopeType
import io.airbyte.domain.models.SecretReferenceWithConfig
import io.micronaut.transaction.annotation.Transactional
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
open class SecretReferenceServiceDataImpl(
  private val secretReferenceRepository: SecretReferenceRepository,
  private val secretReferenceWithConfigRepository: SecretReferenceWithConfigRepository,
) : SecretReferenceService {
  @Transactional("config")
  override fun createAndReplace(secretReference: SecretReferenceCreate): SecretReference {
    // Delete any existing secret reference with the same scope type, scope id, and hydration path
    // as the new one, if present.
    secretReferenceRepository.deleteByScopeTypeAndScopeIdAndHydrationPath(
      secretReference.scopeType.toEntity(),
      secretReference.scopeId,
      secretReference.hydrationPath,
    )
    return secretReferenceRepository.save(secretReference.toEntity()).toConfigModel()
  }

  override fun deleteByScopeTypeAndScopeIdAndHydrationPath(
    scopeType: SecretReferenceScopeType,
    scopeId: UUID,
    hydrationPath: String,
  ) {
    secretReferenceRepository.deleteByScopeTypeAndScopeIdAndHydrationPath(scopeType.toEntity(), scopeId, hydrationPath)
  }

  override fun deleteByScopeTypeAndScopeId(
    scopeType: SecretReferenceScopeType,
    scopeId: UUID,
  ) {
    secretReferenceRepository.deleteByScopeTypeAndScopeId(scopeType.toEntity(), scopeId)
  }

  override fun findById(id: SecretReferenceId): SecretReference? = secretReferenceRepository.findById(id.value).orElse(null)?.toConfigModel()

  override fun listByScopeTypeAndScopeId(
    scopeType: SecretReferenceScopeType,
    scopeId: UUID,
  ): List<SecretReference> = secretReferenceRepository.listByScopeTypeAndScopeId(scopeType.toEntity(), scopeId).map { it.toConfigModel() }

  override fun listWithConfigByScopeTypeAndScopeId(
    scopeType: SecretReferenceScopeType,
    scopeId: UUID,
  ): List<SecretReferenceWithConfig> =
    secretReferenceWithConfigRepository.listByScopeTypeAndScopeId(scopeType.toEntity(), scopeId).map { it.toConfigModel() }

  override fun listWithConfigByScopeTypeAndScopeIds(
    scopeType: SecretReferenceScopeType,
    scopeIds: List<UUID>,
  ): List<SecretReferenceWithConfig> =
    secretReferenceWithConfigRepository.listByScopeTypeAndScopeIdIn(scopeType.toEntity(), scopeIds).map {
      it.toConfigModel()
    }
}
