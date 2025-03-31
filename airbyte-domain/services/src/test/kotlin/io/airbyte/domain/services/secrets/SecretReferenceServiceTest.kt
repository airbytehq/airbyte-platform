/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.secrets

import io.airbyte.commons.json.Jsons
import io.airbyte.config.secrets.SecretCoordinate
import io.airbyte.config.secrets.SecretReferenceConfig
import io.airbyte.data.services.SecretReferenceService
import io.airbyte.domain.models.SecretReferenceId
import io.airbyte.domain.models.SecretReferenceScopeType
import io.airbyte.domain.models.SecretReferenceWithConfig
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class SecretReferenceServiceTest {
  private val secretReferenceRepository = mockk<SecretReferenceService>()
  private val secretReferenceService = SecretReferenceService(secretReferenceRepository)

  @Test
  fun testGetConfigWithSecretReferences() {
    val actorId = UUID.randomUUID()
    val storageId = UUID.randomUUID()

    val urlRefId = UUID.randomUUID()
    val apiKeyRefId = UUID.randomUUID()

    val jsonConfig =
      Jsons.jsonNode(
        mapOf(
          "username" to "bob",
          "secretUrl" to mapOf("_secret_reference_id" to urlRefId.toString()),
          "auth" to
            mapOf(
              "password" to mapOf("_secret" to "airbyte_secret_coordinate_v1"),
              "apiKey" to mapOf("_secret_reference_id" to apiKeyRefId.toString()),
            ),
        ),
      )

    val secretUrlRef = mockk<SecretReferenceWithConfig>()
    every { secretUrlRef.secretReference.id } returns SecretReferenceId(urlRefId)
    every { secretUrlRef.secretReference.hydrationPath } returns "$.secretUrl"
    every { secretUrlRef.secretConfig.secretStorageId } returns storageId
    every { secretUrlRef.secretConfig.airbyteManaged } returns true
    every { secretUrlRef.secretConfig.externalCoordinate } returns "url-coord"

    val apiKeyRef = mockk<SecretReferenceWithConfig>()
    every { apiKeyRef.secretReference.id } returns SecretReferenceId(apiKeyRefId)
    every { apiKeyRef.secretReference.hydrationPath } returns "$.auth.apiKey"
    every { apiKeyRef.secretConfig.secretStorageId } returns storageId
    every { apiKeyRef.secretConfig.airbyteManaged } returns false
    every { apiKeyRef.secretConfig.externalCoordinate } returns "auth-key-coord"

    every { secretReferenceRepository.listWithConfigByScopeTypeAndScopeId(SecretReferenceScopeType.ACTOR, actorId) } returns
      listOf(
        secretUrlRef,
        apiKeyRef,
      )

    val expectedReferencedSecretsMap =
      mapOf(
        "$.auth.password" to SecretReferenceConfig(SecretCoordinate.AirbyteManagedSecretCoordinate("airbyte_secret_coordinate"), null),
        "$.secretUrl" to SecretReferenceConfig(SecretCoordinate.ExternalSecretCoordinate("url-coord"), storageId),
        "$.auth.apiKey" to SecretReferenceConfig(SecretCoordinate.ExternalSecretCoordinate("auth-key-coord"), storageId),
      )

    val actual = secretReferenceService.getConfigWithSecretReferences(SecretReferenceScopeType.ACTOR, actorId, jsonConfig)
    assertEquals(expectedReferencedSecretsMap, actual.referencedSecrets)
  }
}
