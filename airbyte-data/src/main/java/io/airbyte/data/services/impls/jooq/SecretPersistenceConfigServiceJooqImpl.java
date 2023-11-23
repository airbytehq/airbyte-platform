/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq;

import static io.airbyte.db.instance.configs.jooq.generated.tables.SecretPersistenceConfig.SECRET_PERSISTENCE_CONFIG;
import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.inline;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.ScopeType;
import io.airbyte.config.SecretPersistenceConfig;
import io.airbyte.config.SecretPersistenceConfig.SecretPersistenceType;
import io.airbyte.config.SecretPersistenceCoordinate;
import io.airbyte.config.secrets.SecretCoordinate;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.db.Database;
import io.airbyte.db.ExceptionWrappingDatabase;
import io.airbyte.db.instance.configs.jooq.generated.enums.SecretPersistenceScopeType;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SelectJoinStep;

@Singleton
public class SecretPersistenceConfigServiceJooqImpl implements SecretPersistenceConfigService {

  private final ExceptionWrappingDatabase database;
  private final SecretsRepositoryReader secretsRepositoryReader;
  private static final String SORT_ORDER = "sort_order";

  public SecretPersistenceConfigServiceJooqImpl(@Named("configDatabase") final Database database,
                                                final SecretsRepositoryReader secretsRepositoryReader) {
    this.database = new ExceptionWrappingDatabase(database);
    this.secretsRepositoryReader = secretsRepositoryReader;
  }

  private Optional<SecretPersistenceCoordinate> getSecretPersistenceCoordinate(final ScopeType scope, final UUID scopeId) throws IOException {
    final Result<Record> result = database.query(ctx -> {
      final SelectJoinStep<Record> query = ctx.select(asterisk()).from(SECRET_PERSISTENCE_CONFIG);
      // There's a unique constraint on scope_type/scope_id, no need to filter by anything else.
      return query.where(
          SECRET_PERSISTENCE_CONFIG.SCOPE_TYPE.eq(SecretPersistenceScopeType.valueOf(scope.value())),
          SECRET_PERSISTENCE_CONFIG.SCOPE_ID.eq(scopeId))
          .fetch();
    });

    return result.stream().findFirst().map(DbConverter::buildSecretPersistenceCoordinate);
  }

  @Override
  public SecretPersistenceConfig getSecretPersistenceConfig(final ScopeType scope, final UUID scopeId) throws IOException, ConfigNotFoundException {
    final Optional<SecretPersistenceCoordinate> secretPersistenceCoordinate = getSecretPersistenceCoordinate(scope, scopeId);
    if (secretPersistenceCoordinate.isPresent()) {
      final JsonNode configuration = secretsRepositoryReader.fetchSecretFromDefaultSecretPersistence(
          SecretCoordinate.Companion.fromFullCoordinate(secretPersistenceCoordinate.get().getCoordinate()));

      return new SecretPersistenceConfig()
          .withSecretPersistenceType(secretPersistenceCoordinate.get().getSecretPersistenceType())
          .withScopeId(secretPersistenceCoordinate.get().getScopeId())
          .withScopeType(secretPersistenceCoordinate.get().getScopeType())
          .withConfiguration(Jsons.deserializeToStringMap(configuration));
    }
    throw new ConfigNotFoundException(ConfigSchema.SECRET_PERSISTENCE_CONFIG, List.of(scope, scopeId).toString());
  }

  /**
   * Retrieve secret persistence configs in order of precedence: workspace -> organization.
   *
   * @param workspaceId workspace ID
   * @param organizationId organization ID
   * @return Optional secret persistence config for the first scope found.
   * @throws IOException it could happen
   */
  @Override
  public Optional<SecretPersistenceCoordinate> getSecretPersistenceCoordinate(final UUID workspaceId, final UUID organizationId) throws IOException {
    final Result<Record> result = database.query(ctx -> {
      final SelectJoinStep<Record> query = ctx.select(asterisk(), inline(1).as(SORT_ORDER)).from(SECRET_PERSISTENCE_CONFIG);
      return query.where(
          SECRET_PERSISTENCE_CONFIG.SCOPE_TYPE.eq(SecretPersistenceScopeType.workspace),
          SECRET_PERSISTENCE_CONFIG.SCOPE_ID.eq(workspaceId))
          .unionAll(
              ctx.select(asterisk(), inline(2).as(SORT_ORDER)).from(SECRET_PERSISTENCE_CONFIG)
                  .where(
                      SECRET_PERSISTENCE_CONFIG.SCOPE_TYPE.eq(SecretPersistenceScopeType.organization),
                      SECRET_PERSISTENCE_CONFIG.SCOPE_ID.eq(organizationId)))
          .orderBy(field(SORT_ORDER).asc())
          .limit(1).fetch();
    });

    return result.stream().findFirst().map(DbConverter::buildSecretPersistenceCoordinate);
  }

  @Override
  public Optional<SecretPersistenceCoordinate> createOrUpdateSecretPersistenceConfig(final ScopeType scope,
                                                                                     final UUID scopeId,
                                                                                     final SecretPersistenceType secretPersistenceType,
                                                                                     final String secretPersistenceConfigCoordinate)
      throws IOException {
    io.airbyte.db.instance.configs.jooq.generated.enums.SecretPersistenceType dbSecretPersistenceType =
        io.airbyte.db.instance.configs.jooq.generated.enums.SecretPersistenceType.valueOf(secretPersistenceType.value());
    final Result<Record> result = database.transaction(ctx -> ctx.insertInto(SECRET_PERSISTENCE_CONFIG)
        .set(SECRET_PERSISTENCE_CONFIG.ID, UUID.randomUUID())
        .set(SECRET_PERSISTENCE_CONFIG.SCOPE_TYPE, SecretPersistenceScopeType.valueOf(scope.value()))
        .set(SECRET_PERSISTENCE_CONFIG.SCOPE_ID, scopeId)
        .set(SECRET_PERSISTENCE_CONFIG.SECRET_PERSISTENCE_TYPE, dbSecretPersistenceType)
        .set(SECRET_PERSISTENCE_CONFIG.SECRET_PERSISTENCE_CONFIG_COORDINATE, secretPersistenceConfigCoordinate)
        .onConflict(SECRET_PERSISTENCE_CONFIG.SCOPE_ID, SECRET_PERSISTENCE_CONFIG.SCOPE_TYPE)
        .doUpdate()
        .set(SECRET_PERSISTENCE_CONFIG.SECRET_PERSISTENCE_CONFIG_COORDINATE, secretPersistenceConfigCoordinate)
        .set(SECRET_PERSISTENCE_CONFIG.SECRET_PERSISTENCE_TYPE, dbSecretPersistenceType)
        .returningResult(asterisk())
        .fetch());

    return result.stream().findFirst().map(DbConverter::buildSecretPersistenceCoordinate);
  }

}
