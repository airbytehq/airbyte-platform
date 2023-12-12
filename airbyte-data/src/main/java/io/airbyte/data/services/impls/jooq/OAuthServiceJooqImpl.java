/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq;

import static io.airbyte.db.instance.configs.jooq.generated.Tables.ACTOR_OAUTH_PARAMETER;
import static io.airbyte.db.instance.configs.jooq.generated.Tables.WORKSPACE;
import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.select;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ConfigSchema;
import io.airbyte.config.DestinationOAuthParameter;
import io.airbyte.config.ScopeType;
import io.airbyte.config.SecretPersistenceConfig;
import io.airbyte.config.SourceOAuthParameter;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.OAuthService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.db.Database;
import io.airbyte.db.ExceptionWrappingDatabase;
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.Organization;
import io.airbyte.featureflag.UseRuntimeSecretPersistence;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SelectJoinStep;

@Singleton
public class OAuthServiceJooqImpl implements OAuthService {

  private final ExceptionWrappingDatabase database;
  private final FeatureFlagClient featureFlagClient;
  private final SecretsRepositoryReader secretsRepositoryReader;
  private final SecretPersistenceConfigService secretPersistenceConfigService;

  public OAuthServiceJooqImpl(@Named("configDatabase") final Database database,
                              final FeatureFlagClient featureFlagClient,
                              final SecretsRepositoryReader secretsRepositoryReader,
                              final SecretPersistenceConfigService secretPersistenceConfigService) {
    this.database = new ExceptionWrappingDatabase(database);
    this.featureFlagClient = featureFlagClient;
    this.secretsRepositoryReader = secretsRepositoryReader;
    this.secretPersistenceConfigService = secretPersistenceConfigService;
  }

  /**
   * Get source oauth parameter.
   *
   * @param workspaceId workspace id
   * @param sourceDefinitionId source definition id
   * @return source oauth parameter
   * @throws IOException if there is an issue while interacting with db.
   */
  @Override
  public Optional<SourceOAuthParameter> getSourceOAuthParamByDefinitionIdOptional(final UUID workspaceId, final UUID sourceDefinitionId)
      throws IOException {
    final Result<Record> result = database.query(ctx -> {
      final SelectJoinStep<Record> query = ctx.select(asterisk()).from(ACTOR_OAUTH_PARAMETER);
      return query.where(ACTOR_OAUTH_PARAMETER.ACTOR_TYPE.eq(ActorType.source),
          ACTOR_OAUTH_PARAMETER.WORKSPACE_ID.eq(workspaceId),
          ACTOR_OAUTH_PARAMETER.ACTOR_DEFINITION_ID.eq(sourceDefinitionId)).fetch();
    });

    return result.stream().findFirst().map(DbConverter::buildSourceOAuthParameter);
  }

  /**
   * Write source oauth param.
   *
   * @param sourceOAuthParameter source oauth param
   * @throws IOException if there is an issue while interacting with db.
   */
  @Override
  public void writeSourceOAuthParam(final SourceOAuthParameter sourceOAuthParameter) throws IOException {
    database.transaction(ctx -> {
      writeSourceOauthParameter(Collections.singletonList(sourceOAuthParameter), ctx);
      return null;
    });
  }

  private UUID getOrganizationIdFor(final UUID actorOAuthParameterId) throws IOException {
    return database.query(ctx -> ctx.select(WORKSPACE.ORGANIZATION_ID)
        .from(ACTOR_OAUTH_PARAMETER)
        .join(WORKSPACE).on(WORKSPACE.ID.eq(ACTOR_OAUTH_PARAMETER.WORKSPACE_ID))
        .where(ACTOR_OAUTH_PARAMETER.ID.eq(actorOAuthParameterId))
        .fetchOne(WORKSPACE.ORGANIZATION_ID));
  }

  /**
   * Gets Source OAuth Parameter based on the workspaceId and sourceDefinitionId.
   *
   * @return Optional<SourceOAuthParameter>
   * @throws IOException
   * @throws ConfigNotFoundException
   */
  @Override
  public SourceOAuthParameter getSourceOAuthParameterWithSecrets(final UUID workspaceId, final UUID sourceDefinitionId)
      throws IOException, ConfigNotFoundException {
    SourceOAuthParameter sourceOAuthParameter =
        getSourceOAuthParameterOptional(workspaceId, sourceDefinitionId).orElseThrow(() -> new ConfigNotFoundException(
            ConfigSchema.SOURCE_OAUTH_PARAM,
            String.format("workspaceId: %s, sourceDefinitionId: %s", workspaceId, sourceDefinitionId)));
    final UUID organizationId = getOrganizationIdFor(sourceOAuthParameter.getOauthParameterId());
    final JsonNode hydratedConfig;
    if (organizationId != null && featureFlagClient.boolVariation(UseRuntimeSecretPersistence.INSTANCE, new Organization(organizationId))) {
      final SecretPersistenceConfig secretPersistenceConfig =
          secretPersistenceConfigService.getSecretPersistenceConfig(ScopeType.ORGANIZATION, organizationId);
      hydratedConfig =
          secretsRepositoryReader.hydrateConfigFromRuntimeSecretPersistence(sourceOAuthParameter.getConfiguration(),
              new RuntimeSecretPersistence(secretPersistenceConfig));
    } else {
      hydratedConfig = secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(sourceOAuthParameter.getConfiguration());
    }
    sourceOAuthParameter.setConfiguration(hydratedConfig);
    return sourceOAuthParameter;
  }

  /**
   * Gets a source OAuth parameter based on the workspace ID and source definition Id. Defaults to the
   * global param.
   *
   * @param workspaceId workspace ID
   * @param sourceDefinitionId source definition Id
   * @return the found source OAuth parameter
   * @throws IOException it could happen
   */
  @Override
  public Optional<SourceOAuthParameter> getSourceOAuthParameterOptional(final UUID workspaceId, final UUID sourceDefinitionId)
      throws IOException {
    final Result<Record> result = database.query(ctx -> {
      final SelectJoinStep<Record> query = ctx.select(asterisk()).from(ACTOR_OAUTH_PARAMETER);
      return query.where(ACTOR_OAUTH_PARAMETER.ACTOR_TYPE.eq(ActorType.source),
          ACTOR_OAUTH_PARAMETER.WORKSPACE_ID.eq(workspaceId).or(ACTOR_OAUTH_PARAMETER.WORKSPACE_ID.isNull()),
          ACTOR_OAUTH_PARAMETER.ACTOR_DEFINITION_ID.eq(sourceDefinitionId))
          .orderBy(ACTOR_OAUTH_PARAMETER.WORKSPACE_ID.isNotNull().desc()).fetch();
    });

    return result.stream().findFirst().map(DbConverter::buildSourceOAuthParameter);
  }

  /**
   * Gets Destination OAuth Parameter based on the workspaceId and sourceDefinitionId.
   *
   * @return Optional<DestinationOAuthParameter>
   * @throws IOException
   * @throws ConfigNotFoundException
   */
  @Override
  public DestinationOAuthParameter getDestinationOAuthParameterWithSecrets(final UUID workspaceId, final UUID destinationDefinitionId)
      throws IOException, ConfigNotFoundException {
    DestinationOAuthParameter destinationOAuthParameter =
        getDestinationOAuthParameterOptional(workspaceId, destinationDefinitionId).orElseThrow(() -> new ConfigNotFoundException(
            ConfigSchema.SOURCE_OAUTH_PARAM,
            String.format("workspaceId: %s, sourceDefinitionId: %s", workspaceId, destinationDefinitionId)));
    final UUID organizationId = getOrganizationIdFor(destinationOAuthParameter.getOauthParameterId());
    final JsonNode hydratedConfig;
    if (organizationId != null && featureFlagClient.boolVariation(UseRuntimeSecretPersistence.INSTANCE, new Organization(organizationId))) {
      final SecretPersistenceConfig secretPersistenceConfig =
          secretPersistenceConfigService.getSecretPersistenceConfig(ScopeType.ORGANIZATION, organizationId);
      hydratedConfig =
          secretsRepositoryReader.hydrateConfigFromRuntimeSecretPersistence(destinationOAuthParameter.getConfiguration(),
              new RuntimeSecretPersistence(secretPersistenceConfig));
    } else {
      hydratedConfig = secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(destinationOAuthParameter.getConfiguration());
    }
    destinationOAuthParameter.setConfiguration(hydratedConfig);
    return destinationOAuthParameter;
  }

  /**
   * Gets a source OAuth parameter based on the workspace ID and source definition Id. Defaults to the
   * global param.
   *
   * @param workspaceId workspace ID
   * @param destinationDefinitionId source definition Id
   * @return the found source OAuth parameter
   * @throws IOException it could happen
   */
  @Override
  public Optional<DestinationOAuthParameter> getDestinationOAuthParameterOptional(final UUID workspaceId, final UUID destinationDefinitionId)
      throws IOException {
    final Result<Record> result = database.query(ctx -> {
      final SelectJoinStep<Record> query = ctx.select(asterisk()).from(ACTOR_OAUTH_PARAMETER);
      return query.where(ACTOR_OAUTH_PARAMETER.ACTOR_TYPE.eq(ActorType.destination),
          ACTOR_OAUTH_PARAMETER.WORKSPACE_ID.eq(workspaceId).or(ACTOR_OAUTH_PARAMETER.WORKSPACE_ID.isNull()),
          ACTOR_OAUTH_PARAMETER.ACTOR_DEFINITION_ID.eq(destinationDefinitionId))
          .orderBy(ACTOR_OAUTH_PARAMETER.WORKSPACE_ID.isNotNull().desc()).fetch();
    });

    return result.stream().findFirst().map(DbConverter::buildDestinationOAuthParameter);
  }

  /**
   * Get destination oauth parameter.
   *
   * @param workspaceId workspace id
   * @param destinationDefinitionId destination definition id
   * @return oauth parameters if present
   * @throws IOException if there is an issue while interacting with db.
   */
  @Override
  public Optional<DestinationOAuthParameter> getDestinationOAuthParamByDefinitionIdOptional(final UUID workspaceId,
                                                                                            final UUID destinationDefinitionId)
      throws IOException {
    final Result<Record> result = database.query(ctx -> {
      final SelectJoinStep<Record> query = ctx.select(asterisk()).from(ACTOR_OAUTH_PARAMETER);
      return query.where(ACTOR_OAUTH_PARAMETER.ACTOR_TYPE.eq(ActorType.destination),
          ACTOR_OAUTH_PARAMETER.WORKSPACE_ID.eq(workspaceId),
          ACTOR_OAUTH_PARAMETER.ACTOR_DEFINITION_ID.eq(destinationDefinitionId)).fetch();
    });

    return result.stream().findFirst().map(DbConverter::buildDestinationOAuthParameter);
  }

  /**
   * Write destination oauth param.
   *
   * @param destinationOAuthParameter destination oauth parameter
   * @throws IOException if there is an issue while interacting with db.
   */
  @Override
  public void writeDestinationOAuthParam(final DestinationOAuthParameter destinationOAuthParameter) throws IOException {
    database.transaction(ctx -> {
      writeDestinationOauthParameter(Collections.singletonList(destinationOAuthParameter), ctx);
      return null;
    });
  }

  private void writeSourceOauthParameter(final List<SourceOAuthParameter> configs, final DSLContext ctx) {
    final OffsetDateTime timestamp = OffsetDateTime.now();
    configs.forEach((sourceOAuthParameter) -> {
      final boolean isExistingConfig = ctx.fetchExists(select()
          .from(ACTOR_OAUTH_PARAMETER)
          .where(ACTOR_OAUTH_PARAMETER.ID.eq(sourceOAuthParameter.getOauthParameterId())));

      if (isExistingConfig) {
        ctx.update(ACTOR_OAUTH_PARAMETER)
            .set(ACTOR_OAUTH_PARAMETER.ID, sourceOAuthParameter.getOauthParameterId())
            .set(ACTOR_OAUTH_PARAMETER.WORKSPACE_ID, sourceOAuthParameter.getWorkspaceId())
            .set(ACTOR_OAUTH_PARAMETER.ACTOR_DEFINITION_ID, sourceOAuthParameter.getSourceDefinitionId())
            .set(ACTOR_OAUTH_PARAMETER.CONFIGURATION, JSONB.valueOf(Jsons.serialize(sourceOAuthParameter.getConfiguration())))
            .set(ACTOR_OAUTH_PARAMETER.ACTOR_TYPE, ActorType.source)
            .set(ACTOR_OAUTH_PARAMETER.UPDATED_AT, timestamp)
            .where(ACTOR_OAUTH_PARAMETER.ID.eq(sourceOAuthParameter.getOauthParameterId()))
            .execute();
      } else {
        ctx.insertInto(ACTOR_OAUTH_PARAMETER)
            .set(ACTOR_OAUTH_PARAMETER.ID, sourceOAuthParameter.getOauthParameterId())
            .set(ACTOR_OAUTH_PARAMETER.WORKSPACE_ID, sourceOAuthParameter.getWorkspaceId())
            .set(ACTOR_OAUTH_PARAMETER.ACTOR_DEFINITION_ID, sourceOAuthParameter.getSourceDefinitionId())
            .set(ACTOR_OAUTH_PARAMETER.CONFIGURATION, JSONB.valueOf(Jsons.serialize(sourceOAuthParameter.getConfiguration())))
            .set(ACTOR_OAUTH_PARAMETER.ACTOR_TYPE, ActorType.source)
            .set(ACTOR_OAUTH_PARAMETER.CREATED_AT, timestamp)
            .set(ACTOR_OAUTH_PARAMETER.UPDATED_AT, timestamp)
            .execute();
      }
    });
  }

  private void writeDestinationOauthParameter(final List<DestinationOAuthParameter> configs, final DSLContext ctx) {
    final OffsetDateTime timestamp = OffsetDateTime.now();
    configs.forEach((destinationOAuthParameter) -> {
      final boolean isExistingConfig = ctx.fetchExists(select()
          .from(ACTOR_OAUTH_PARAMETER)
          .where(ACTOR_OAUTH_PARAMETER.ID.eq(destinationOAuthParameter.getOauthParameterId())));

      if (isExistingConfig) {
        ctx.update(ACTOR_OAUTH_PARAMETER)
            .set(ACTOR_OAUTH_PARAMETER.ID, destinationOAuthParameter.getOauthParameterId())
            .set(ACTOR_OAUTH_PARAMETER.WORKSPACE_ID, destinationOAuthParameter.getWorkspaceId())
            .set(ACTOR_OAUTH_PARAMETER.ACTOR_DEFINITION_ID, destinationOAuthParameter.getDestinationDefinitionId())
            .set(ACTOR_OAUTH_PARAMETER.CONFIGURATION, JSONB.valueOf(Jsons.serialize(destinationOAuthParameter.getConfiguration())))
            .set(ACTOR_OAUTH_PARAMETER.ACTOR_TYPE, ActorType.destination)
            .set(ACTOR_OAUTH_PARAMETER.UPDATED_AT, timestamp)
            .where(ACTOR_OAUTH_PARAMETER.ID.eq(destinationOAuthParameter.getOauthParameterId()))
            .execute();

      } else {
        ctx.insertInto(ACTOR_OAUTH_PARAMETER)
            .set(ACTOR_OAUTH_PARAMETER.ID, destinationOAuthParameter.getOauthParameterId())
            .set(ACTOR_OAUTH_PARAMETER.WORKSPACE_ID, destinationOAuthParameter.getWorkspaceId())
            .set(ACTOR_OAUTH_PARAMETER.ACTOR_DEFINITION_ID, destinationOAuthParameter.getDestinationDefinitionId())
            .set(ACTOR_OAUTH_PARAMETER.CONFIGURATION, JSONB.valueOf(Jsons.serialize(destinationOAuthParameter.getConfiguration())))
            .set(ACTOR_OAUTH_PARAMETER.ACTOR_TYPE, ActorType.destination)
            .set(ACTOR_OAUTH_PARAMETER.CREATED_AT, timestamp)
            .set(ACTOR_OAUTH_PARAMETER.UPDATED_AT, timestamp)
            .execute();
      }
    });

  }

}
