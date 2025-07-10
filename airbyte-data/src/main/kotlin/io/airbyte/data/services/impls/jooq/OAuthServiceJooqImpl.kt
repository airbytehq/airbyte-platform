/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.config.DestinationOAuthParameter
import io.airbyte.config.ScopeType
import io.airbyte.config.SourceOAuthParameter
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.persistence.RuntimeSecretPersistence
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.OAuthService
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.db.Database
import io.airbyte.db.ExceptionWrappingDatabase
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.db.instance.configs.jooq.generated.enums.ActorType
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Organization
import io.airbyte.featureflag.UseRuntimeSecretPersistence
import io.airbyte.metrics.MetricClient
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.Record
import org.jooq.impl.DSL
import java.io.IOException
import java.time.OffsetDateTime
import java.util.Optional
import java.util.UUID
import java.util.function.Consumer
import java.util.function.Function

@Singleton
class OAuthServiceJooqImpl(
  @Named("configDatabase") database: Database?,
  private val featureFlagClient: FeatureFlagClient,
  private val secretsRepositoryReader: SecretsRepositoryReader,
  private val secretPersistenceConfigService: SecretPersistenceConfigService,
  private val metricClient: MetricClient,
  private val workspaceService: WorkspaceService,
) : OAuthService {
  private val database = ExceptionWrappingDatabase(database)

  /**
   * Get source oauth parameter.
   *
   * @param workspaceId workspace id
   * @param sourceDefinitionId source definition id
   * @return source oauth parameter
   * @throws IOException if there is an issue while interacting with db.
   */
  @Throws(IOException::class)
  override fun getSourceOAuthParamByDefinitionIdOptional(
    workspaceId: Optional<UUID>,
    organizationId: Optional<UUID>,
    sourceDefinitionId: UUID,
  ): Optional<SourceOAuthParameter> {
    val result =
      database.query { ctx: DSLContext ->
        val query = ctx.select(DSL.asterisk()).from(Tables.ACTOR_OAUTH_PARAMETER)
        query
          .where(
            Tables.ACTOR_OAUTH_PARAMETER.ACTOR_TYPE.eq(ActorType.source),
            workspaceId
              .map { t: UUID? ->
                Tables.ACTOR_OAUTH_PARAMETER.WORKSPACE_ID.eq(
                  t,
                )
              }.orElseGet { Tables.ACTOR_OAUTH_PARAMETER.WORKSPACE_ID.isNull() },
            organizationId
              .map { t: UUID? ->
                Tables.ACTOR_OAUTH_PARAMETER.ORGANIZATION_ID.eq(
                  t,
                )
              }.orElseGet { Tables.ACTOR_OAUTH_PARAMETER.ORGANIZATION_ID.isNull() },
            Tables.ACTOR_OAUTH_PARAMETER.ACTOR_DEFINITION_ID.eq(sourceDefinitionId),
          ).fetch()
      }

    return result.stream().findFirst().map { record: Record -> DbConverter.buildSourceOAuthParameter(record) }
  }

  /**
   * Write source oauth param.
   *
   * @param sourceOAuthParameter source oauth param
   * @throws IOException if there is an issue while interacting with db.
   */
  @Throws(IOException::class)
  override fun writeSourceOAuthParam(sourceOAuthParameter: SourceOAuthParameter) {
    database.transaction<Any?> { ctx: DSLContext ->
      writeSourceOauthParameter(listOf(sourceOAuthParameter), ctx)
      null
    }
  }

  /**
   * Gets Source OAuth Parameter based on the workspaceId and sourceDefinitionId.
   *
   * @return Optional<SourceOAuthParameter>
   * @throws IOException
   * @throws ConfigNotFoundException if secret persistence coordinate is not present
   </SourceOAuthParameter> */
  @Throws(IOException::class, ConfigNotFoundException::class)
  override fun getSourceOAuthParameterWithSecretsOptional(
    workspaceId: UUID,
    sourceDefinitionId: UUID,
  ): Optional<SourceOAuthParameter> {
    val organizationId = workspaceService.getOrganizationIdFromWorkspaceId(workspaceId).get()
    val sourceOAuthParameterOptional =
      getSourceOAuthParameterOptional(workspaceId, organizationId, sourceDefinitionId)
    if (sourceOAuthParameterOptional.isEmpty) {
      return sourceOAuthParameterOptional
    }

    val sourceOAuthParameter = sourceOAuthParameterOptional.get()

    val hydratedConfig = hydrateConfig(sourceOAuthParameter.configuration, organizationId)
    return Optional.of(sourceOAuthParameter.withConfiguration(hydratedConfig))
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
  @Throws(IOException::class)
  override fun getSourceOAuthParameterOptional(
    workspaceId: UUID,
    sourceDefinitionId: UUID,
  ): Optional<SourceOAuthParameter> =
    getSourceOAuthParameterOptional(workspaceId, workspaceService.getOrganizationIdFromWorkspaceId(workspaceId).get(), sourceDefinitionId)

  /**
   * Gets a source OAuth parameter based on the workspace ID and source definition Id. Defaults to the
   * global param.
   *
   * @param workspaceId workspace ID
   * @param organizationId organization ID
   * @param sourceDefinitionId source definition Id
   * @return the found source OAuth parameter
   * @throws IOException it could happen
   */
  @Throws(IOException::class)
  override fun getSourceOAuthParameterOptional(
    workspaceId: UUID,
    organizationId: UUID,
    sourceDefinitionId: UUID,
  ): Optional<SourceOAuthParameter> =
    getActorOAuthParameterOptional(
      workspaceId,
      organizationId,
      sourceDefinitionId,
      ActorType.source,
    ) { record: Record -> DbConverter.buildSourceOAuthParameter(record) }

  @Throws(IOException::class)
  private fun <T> getActorOAuthParameterOptional(
    workspaceId: UUID,
    organizationId: UUID,
    actorDefinitionId: UUID,
    actorType: ActorType,
    converter: Function<Record, T>,
  ): Optional<T> {
    val result =
      database.query { ctx: DSLContext ->
        val query =
          ctx.select(DSL.asterisk()).from(Tables.ACTOR_OAUTH_PARAMETER)
        query
          .where(
            Tables.ACTOR_OAUTH_PARAMETER.ACTOR_TYPE.eq(actorType),
            Tables.ACTOR_OAUTH_PARAMETER.WORKSPACE_ID
              .eq(workspaceId)
              .or(Tables.ACTOR_OAUTH_PARAMETER.WORKSPACE_ID.isNull()),
            Tables.ACTOR_OAUTH_PARAMETER.ORGANIZATION_ID
              .eq(organizationId)
              .or(Tables.ACTOR_OAUTH_PARAMETER.ORGANIZATION_ID.isNull()),
            Tables.ACTOR_OAUTH_PARAMETER.ACTOR_DEFINITION_ID.eq(actorDefinitionId),
          ).orderBy(
            Tables.ACTOR_OAUTH_PARAMETER.WORKSPACE_ID
              .isNotNull()
              .desc(),
            Tables.ACTOR_OAUTH_PARAMETER.ORGANIZATION_ID
              .isNotNull()
              .desc(),
          ).fetch()
      }

    return result.stream().findFirst().map(converter)
  }

  /**
   * Gets Destination OAuth Parameter based on the workspaceId and destinationDefinitionId.
   *
   * @return Optional<DestinationOAuthParameter>
   * @throws IOException
   * @throws ConfigNotFoundException if secret persistence coordinate is not present
   </DestinationOAuthParameter> */
  @Throws(IOException::class, ConfigNotFoundException::class)
  override fun getDestinationOAuthParameterWithSecretsOptional(
    workspaceId: UUID,
    destinationDefinitionId: UUID,
  ): Optional<DestinationOAuthParameter> {
    val organizationId = workspaceService.getOrganizationIdFromWorkspaceId(workspaceId).get()
    val destinationOAuthParameterOptional =
      getDestinationOAuthParameterOptional(workspaceId, organizationId, destinationDefinitionId)
    if (destinationOAuthParameterOptional.isEmpty) {
      return destinationOAuthParameterOptional
    }

    val destinationOAuthParameter = destinationOAuthParameterOptional.get()

    val hydratedConfig = hydrateConfig(destinationOAuthParameter.configuration, organizationId)
    return Optional.of(destinationOAuthParameter.withConfiguration(hydratedConfig))
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
  @Throws(IOException::class)
  override fun getDestinationOAuthParameterOptional(
    workspaceId: UUID,
    destinationDefinitionId: UUID,
  ): Optional<DestinationOAuthParameter> {
    val organizationId = workspaceService.getOrganizationIdFromWorkspaceId(workspaceId).get()
    return getDestinationOAuthParameterOptional(workspaceId, organizationId, destinationDefinitionId)
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
  @Throws(IOException::class)
  override fun getDestinationOAuthParameterOptional(
    workspaceId: UUID,
    organizationId: UUID,
    destinationDefinitionId: UUID,
  ): Optional<DestinationOAuthParameter> =
    getActorOAuthParameterOptional(
      workspaceId,
      organizationId,
      destinationDefinitionId,
      ActorType.destination,
    ) { record: Record -> DbConverter.buildDestinationOAuthParameter(record) }

  /**
   * Get destination oauth parameter.
   *
   * @param workspaceId workspace id
   * @param destinationDefinitionId destination definition id
   * @return oauth parameters if present
   * @throws IOException if there is an issue while interacting with db.
   */
  @Throws(IOException::class)
  override fun getDestinationOAuthParamByDefinitionIdOptional(
    workspaceId: Optional<UUID>,
    organizationId: Optional<UUID>,
    destinationDefinitionId: UUID,
  ): Optional<DestinationOAuthParameter> {
    val result =
      database.query { ctx: DSLContext ->
        val query = ctx.select(DSL.asterisk()).from(Tables.ACTOR_OAUTH_PARAMETER)
        query
          .where(
            Tables.ACTOR_OAUTH_PARAMETER.ACTOR_TYPE.eq(ActorType.destination),
            workspaceId
              .map { t: UUID? ->
                Tables.ACTOR_OAUTH_PARAMETER.WORKSPACE_ID.eq(
                  t,
                )
              }.orElseGet { Tables.ACTOR_OAUTH_PARAMETER.WORKSPACE_ID.isNull() },
            organizationId
              .map { t: UUID? ->
                Tables.ACTOR_OAUTH_PARAMETER.ORGANIZATION_ID.eq(
                  t,
                )
              }.orElseGet { Tables.ACTOR_OAUTH_PARAMETER.ORGANIZATION_ID.isNull() },
            Tables.ACTOR_OAUTH_PARAMETER.ACTOR_DEFINITION_ID.eq(destinationDefinitionId),
          ).fetch()
      }

    return result.stream().findFirst().map { record: Record ->
      DbConverter.buildDestinationOAuthParameter(
        record,
      )
    }
  }

  /**
   * Write destination oauth param.
   *
   * @param destinationOAuthParameter destination oauth parameter
   * @throws IOException if there is an issue while interacting with db.
   */
  @Throws(IOException::class)
  override fun writeDestinationOAuthParam(destinationOAuthParameter: DestinationOAuthParameter) {
    database.transaction<Any?> { ctx: DSLContext ->
      writeDestinationOauthParameter(listOf(destinationOAuthParameter), ctx)
      null
    }
  }

  @Throws(IOException::class, ConfigNotFoundException::class)
  private fun hydrateConfig(
    config: JsonNode,
    organizationId: UUID?,
  ): JsonNode? {
    // TODO: this can probably be later replaced with a ConnectorSecretsHydrator
    if (organizationId != null && featureFlagClient.boolVariation(UseRuntimeSecretPersistence, Organization(organizationId))) {
      val secretPersistenceConfig =
        secretPersistenceConfigService.get(ScopeType.ORGANIZATION, organizationId)
      return secretsRepositoryReader.hydrateConfigFromRuntimeSecretPersistence(
        config,
        RuntimeSecretPersistence(secretPersistenceConfig, metricClient),
      )
    } else {
      return secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(config)
    }
  }

  private fun writeSourceOauthParameter(
    configs: List<SourceOAuthParameter>,
    ctx: DSLContext,
  ) {
    val timestamp = OffsetDateTime.now()
    configs.forEach(
      Consumer { sourceOAuthParameter: SourceOAuthParameter ->
        val isExistingConfig =
          ctx.fetchExists(
            DSL
              .select()
              .from(Tables.ACTOR_OAUTH_PARAMETER)
              .where(Tables.ACTOR_OAUTH_PARAMETER.ID.eq(sourceOAuthParameter.oauthParameterId)),
          )
        if (isExistingConfig) {
          ctx
            .update(Tables.ACTOR_OAUTH_PARAMETER)
            .set(Tables.ACTOR_OAUTH_PARAMETER.ID, sourceOAuthParameter.oauthParameterId)
            .set(Tables.ACTOR_OAUTH_PARAMETER.WORKSPACE_ID, sourceOAuthParameter.workspaceId)
            .set(Tables.ACTOR_OAUTH_PARAMETER.ORGANIZATION_ID, sourceOAuthParameter.organizationId)
            .set(Tables.ACTOR_OAUTH_PARAMETER.ACTOR_DEFINITION_ID, sourceOAuthParameter.sourceDefinitionId)
            .set(Tables.ACTOR_OAUTH_PARAMETER.CONFIGURATION, JSONB.valueOf(Jsons.serialize(sourceOAuthParameter.configuration)))
            .set(Tables.ACTOR_OAUTH_PARAMETER.ACTOR_TYPE, ActorType.source)
            .set(Tables.ACTOR_OAUTH_PARAMETER.UPDATED_AT, timestamp)
            .where(Tables.ACTOR_OAUTH_PARAMETER.ID.eq(sourceOAuthParameter.oauthParameterId))
            .execute()
        } else {
          ctx
            .insertInto(Tables.ACTOR_OAUTH_PARAMETER)
            .set(Tables.ACTOR_OAUTH_PARAMETER.ID, sourceOAuthParameter.oauthParameterId)
            .set(Tables.ACTOR_OAUTH_PARAMETER.WORKSPACE_ID, sourceOAuthParameter.workspaceId)
            .set(Tables.ACTOR_OAUTH_PARAMETER.ORGANIZATION_ID, sourceOAuthParameter.organizationId)
            .set(Tables.ACTOR_OAUTH_PARAMETER.ACTOR_DEFINITION_ID, sourceOAuthParameter.sourceDefinitionId)
            .set(Tables.ACTOR_OAUTH_PARAMETER.CONFIGURATION, JSONB.valueOf(Jsons.serialize(sourceOAuthParameter.configuration)))
            .set(Tables.ACTOR_OAUTH_PARAMETER.ACTOR_TYPE, ActorType.source)
            .set(Tables.ACTOR_OAUTH_PARAMETER.CREATED_AT, timestamp)
            .set(Tables.ACTOR_OAUTH_PARAMETER.UPDATED_AT, timestamp)
            .execute()
        }
      },
    )
  }

  private fun writeDestinationOauthParameter(
    configs: List<DestinationOAuthParameter>,
    ctx: DSLContext,
  ) {
    val timestamp = OffsetDateTime.now()
    configs.forEach(
      Consumer { destinationOAuthParameter: DestinationOAuthParameter ->
        val isExistingConfig =
          ctx.fetchExists(
            DSL
              .select()
              .from(Tables.ACTOR_OAUTH_PARAMETER)
              .where(Tables.ACTOR_OAUTH_PARAMETER.ID.eq(destinationOAuthParameter.oauthParameterId)),
          )
        if (isExistingConfig) {
          ctx
            .update(Tables.ACTOR_OAUTH_PARAMETER)
            .set(Tables.ACTOR_OAUTH_PARAMETER.ID, destinationOAuthParameter.oauthParameterId)
            .set(Tables.ACTOR_OAUTH_PARAMETER.WORKSPACE_ID, destinationOAuthParameter.workspaceId)
            .set(Tables.ACTOR_OAUTH_PARAMETER.ORGANIZATION_ID, destinationOAuthParameter.organizationId)
            .set(Tables.ACTOR_OAUTH_PARAMETER.ACTOR_DEFINITION_ID, destinationOAuthParameter.destinationDefinitionId)
            .set(Tables.ACTOR_OAUTH_PARAMETER.CONFIGURATION, JSONB.valueOf(Jsons.serialize(destinationOAuthParameter.configuration)))
            .set(Tables.ACTOR_OAUTH_PARAMETER.ACTOR_TYPE, ActorType.destination)
            .set(Tables.ACTOR_OAUTH_PARAMETER.UPDATED_AT, timestamp)
            .where(Tables.ACTOR_OAUTH_PARAMETER.ID.eq(destinationOAuthParameter.oauthParameterId))
            .execute()
        } else {
          ctx
            .insertInto(Tables.ACTOR_OAUTH_PARAMETER)
            .set(Tables.ACTOR_OAUTH_PARAMETER.ID, destinationOAuthParameter.oauthParameterId)
            .set(Tables.ACTOR_OAUTH_PARAMETER.WORKSPACE_ID, destinationOAuthParameter.workspaceId)
            .set(Tables.ACTOR_OAUTH_PARAMETER.ORGANIZATION_ID, destinationOAuthParameter.organizationId)
            .set(Tables.ACTOR_OAUTH_PARAMETER.ACTOR_DEFINITION_ID, destinationOAuthParameter.destinationDefinitionId)
            .set(Tables.ACTOR_OAUTH_PARAMETER.CONFIGURATION, JSONB.valueOf(Jsons.serialize(destinationOAuthParameter.configuration)))
            .set(Tables.ACTOR_OAUTH_PARAMETER.ACTOR_TYPE, ActorType.destination)
            .set(Tables.ACTOR_OAUTH_PARAMETER.CREATED_AT, timestamp)
            .set(Tables.ACTOR_OAUTH_PARAMETER.UPDATED_AT, timestamp)
            .execute()
        }
      },
    )
  }
}
