/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.data.repositories.SecretReferenceRepository
import io.airbyte.data.repositories.SecretReferenceWithConfigRepository
import io.airbyte.data.services.SecretReferenceService
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import io.airbyte.data.services.impls.data.mappers.toEntity
import io.airbyte.domain.models.SecretReference
import io.airbyte.domain.models.SecretReferenceScopeType
import io.airbyte.domain.models.SecretReferenceWithConfig
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
class SecretReferenceServiceDataImpl(
  private val secretReferenceRepository: SecretReferenceRepository,
  private val secretReferenceWithConfigRepository: SecretReferenceWithConfigRepository,
) : SecretReferenceService {
  override fun findById(id: UUID): SecretReference? = secretReferenceRepository.findById(id).orElse(null)?.toConfigModel()

  override fun listByScopeTypeAndScopeId(
    scopeType: SecretReferenceScopeType,
    scopeId: UUID,
  ): List<SecretReference> = secretReferenceRepository.listByScopeTypeAndScopeId(scopeType.toEntity(), scopeId).map { it.toConfigModel() }

  override fun listWithConfigByScopeTypeAndScopeId(
    scopeType: SecretReferenceScopeType,
    scopeId: UUID,
  ): List<SecretReferenceWithConfig> =
    secretReferenceWithConfigRepository.listByScopeTypeAndScopeId(scopeType.toEntity(), scopeId).map { it.toConfigModel() }
}
