package io.airbyte.workers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.client.generated.SecretsPersistenceConfigApi
import io.airbyte.api.client.invoker.generated.ApiException
import io.airbyte.api.client.model.generated.ScopeType
import io.airbyte.api.client.model.generated.SecretPersistenceConfig
import io.airbyte.api.client.model.generated.SecretPersistenceConfigGetRequestBody
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.UseRuntimeSecretPersistence
import io.airbyte.workers.helper.SecretPersistenceConfigHelper
import java.lang.RuntimeException
import java.util.UUID

class CheckConnectionInputHydrator(
  private val secretsRepositoryReader: SecretsRepositoryReader,
  private val secretsApiClient: SecretsPersistenceConfigApi,
  private val featureFlagClient: FeatureFlagClient,
) {
  fun getHydratedStandardCheckInput(rawInput: StandardCheckConnectionInput): StandardCheckConnectionInput {
    val fullConfig: JsonNode?
    val organizationId: UUID? = rawInput.actorContext.organizationId

    fullConfig =
      if (useRuntimeHydration(organizationId)) {
        hydrateFromRuntimePersistence(rawInput.connectionConfiguration, organizationId!!)
      } else {
        secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(rawInput.connectionConfiguration)
      }

    return StandardCheckConnectionInput()
      .withActorId(rawInput.actorId)
      .withActorType(rawInput.actorType)
      .withConnectionConfiguration(fullConfig)
      .withActorContext(rawInput.actorContext)
  }

  private fun hydrateFromRuntimePersistence(
    jsonConfig: JsonNode,
    organizationId: UUID,
  ): JsonNode? {
    val secretPersistenceConfig: SecretPersistenceConfig
    try {
      secretPersistenceConfig =
        secretsApiClient.getSecretsPersistenceConfig(
          SecretPersistenceConfigGetRequestBody()
            .scopeType(ScopeType.ORGANIZATION)
            .scopeId(organizationId),
        )
    } catch (e: ApiException) {
      throw RuntimeException(e)
    }

    val runtimeSecretPersistence =
      SecretPersistenceConfigHelper
        .fromApiSecretPersistenceConfig(secretPersistenceConfig)

    return secretsRepositoryReader.hydrateConfigFromRuntimeSecretPersistence(
      jsonConfig,
      runtimeSecretPersistence,
    )
  }

  private fun useRuntimeHydration(organizationId: UUID?): Boolean =
    organizationId != null && featureFlagClient.boolVariation(UseRuntimeSecretPersistence, Organization(organizationId))
}
