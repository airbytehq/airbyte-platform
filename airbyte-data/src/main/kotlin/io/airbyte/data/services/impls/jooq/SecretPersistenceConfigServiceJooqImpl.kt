/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq

import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.ScopeType
import io.airbyte.config.SecretPersistenceCoordinate
import io.airbyte.config.secrets.SecretCoordinate.Companion.fromFullCoordinate
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.db.Database
import io.airbyte.db.ExceptionWrappingDatabase
import io.airbyte.db.instance.configs.jooq.generated.enums.SecretPersistenceScopeType
import io.airbyte.db.instance.configs.jooq.generated.enums.SecretPersistenceType
import io.airbyte.db.instance.configs.jooq.generated.tables.SecretPersistenceConfig
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.jooq.DSLContext
import org.jooq.Record
import org.jooq.impl.DSL
import java.io.IOException
import java.util.List
import java.util.Optional
import java.util.UUID

@Singleton
class SecretPersistenceConfigServiceJooqImpl(
  @Named("configDatabase") database: Database?,
  private val secretsRepositoryReader: SecretsRepositoryReader,
) : SecretPersistenceConfigService {
  private val database = ExceptionWrappingDatabase(database)

  @Throws(IOException::class)
  private fun getSecretPersistenceCoordinate(
    scope: ScopeType,
    scopeId: UUID,
  ): Optional<SecretPersistenceCoordinate> {
    val result =
      database.query { ctx: DSLContext ->
        val query =
          ctx
            .select(DSL.asterisk())
            .from(SecretPersistenceConfig.SECRET_PERSISTENCE_CONFIG)
        query
          .where(
            SecretPersistenceConfig.SECRET_PERSISTENCE_CONFIG.SCOPE_TYPE.eq(
              SecretPersistenceScopeType.valueOf(scope.value()),
            ),
            SecretPersistenceConfig.SECRET_PERSISTENCE_CONFIG.SCOPE_ID.eq(scopeId),
          ).fetch()
      }

    return result.stream().findFirst().map { record: Record ->
      DbConverter.buildSecretPersistenceCoordinate(
        record,
      )
    }
  }

  @Throws(IOException::class, ConfigNotFoundException::class)
  override fun get(
    scope: ScopeType,
    scopeId: UUID,
  ): io.airbyte.config.SecretPersistenceConfig {
    val secretPersistenceCoordinate = getSecretPersistenceCoordinate(scope, scopeId)
    if (secretPersistenceCoordinate.isPresent) {
      val configuration =
        secretsRepositoryReader.fetchJsonSecretFromDefaultSecretPersistence(
          fromFullCoordinate(secretPersistenceCoordinate.get().coordinate),
        )

      return io.airbyte.config
        .SecretPersistenceConfig()
        .withSecretPersistenceType(secretPersistenceCoordinate.get().secretPersistenceType)
        .withScopeId(secretPersistenceCoordinate.get().scopeId)
        .withScopeType(secretPersistenceCoordinate.get().scopeType)
        .withConfiguration(Jsons.deserializeToStringMap(configuration))
    }
    throw ConfigNotFoundException(ConfigNotFoundType.SECRET_PERSISTENCE_CONFIG, List.of(scope, scopeId).toString())
  }

  @Throws(IOException::class)
  override fun createOrUpdate(
    scope: ScopeType,
    scopeId: UUID,
    secretPersistenceType: io.airbyte.config.SecretPersistenceConfig.SecretPersistenceType,
    secretPersistenceConfigCoordinate: String,
  ): Optional<SecretPersistenceCoordinate> {
    val dbSecretPersistenceType =
      SecretPersistenceType.valueOf(secretPersistenceType.value())
    val result =
      database.transaction { ctx: DSLContext ->
        ctx
          .insertInto(SecretPersistenceConfig.SECRET_PERSISTENCE_CONFIG)
          .set(
            SecretPersistenceConfig.SECRET_PERSISTENCE_CONFIG.ID,
            UUID.randomUUID(),
          ).set(
            SecretPersistenceConfig.SECRET_PERSISTENCE_CONFIG.SCOPE_TYPE,
            SecretPersistenceScopeType.valueOf(scope.value()),
          ).set(
            SecretPersistenceConfig.SECRET_PERSISTENCE_CONFIG.SCOPE_ID,
            scopeId,
          ).set(
            SecretPersistenceConfig.SECRET_PERSISTENCE_CONFIG.SECRET_PERSISTENCE_TYPE,
            dbSecretPersistenceType,
          ).set(
            SecretPersistenceConfig.SECRET_PERSISTENCE_CONFIG.SECRET_PERSISTENCE_CONFIG_COORDINATE,
            secretPersistenceConfigCoordinate,
          ).onConflict(
            SecretPersistenceConfig.SECRET_PERSISTENCE_CONFIG.SCOPE_ID,
            SecretPersistenceConfig.SECRET_PERSISTENCE_CONFIG.SCOPE_TYPE,
          ).doUpdate()
          .set(
            SecretPersistenceConfig.SECRET_PERSISTENCE_CONFIG.SECRET_PERSISTENCE_CONFIG_COORDINATE,
            secretPersistenceConfigCoordinate,
          ).set(
            SecretPersistenceConfig.SECRET_PERSISTENCE_CONFIG.SECRET_PERSISTENCE_TYPE,
            dbSecretPersistenceType,
          ).returningResult(DSL.asterisk())
          .fetch()
      }

    return result.stream().findFirst().map { record: Record ->
      DbConverter.buildSecretPersistenceCoordinate(
        record,
      )
    }
  }

  companion object {
    private const val SORT_ORDER = "sort_order"
  }
}
