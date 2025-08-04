/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons.deserialize
import io.airbyte.config.ActorType
import io.airbyte.config.DestinationOAuthParameter
import io.airbyte.config.ScopeType
import io.airbyte.config.SecretPersistenceConfig
import io.airbyte.config.SourceOAuthParameter
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.db.ContextQueryFunction
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.TestClient
import io.airbyte.metrics.MetricClient
import io.airbyte.test.utils.BaseConfigDatabaseTest
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import java.io.IOException
import java.sql.SQLException
import java.util.Optional
import java.util.UUID

internal class OAuthServiceJooqImplTest : BaseConfigDatabaseTest() {
  private lateinit var oAuthService: OAuthServiceJooqImpl
  private lateinit var secretPersistenceConfigService: SecretPersistenceConfigService
  private lateinit var secretsRepositoryReader: SecretsRepositoryReader
  private lateinit var workspaceService: WorkspaceService

  @BeforeEach
  @Throws(IOException::class, SQLException::class, ConfigNotFoundException::class)
  fun setUp() {
    val featureFlagClient: FeatureFlagClient = Mockito.mock(TestClient::class.java, Mockito.withSettings().withoutAnnotations())
    secretsRepositoryReader =
      Mockito.mock(SecretsRepositoryReader::class.java, Mockito.withSettings().withoutAnnotations())
    secretPersistenceConfigService =
      Mockito.mock(SecretPersistenceConfigService::class.java, Mockito.withSettings().withoutAnnotations())
    val metricClient = Mockito.mock(MetricClient::class.java)

    workspaceService = Mockito.mock(WorkspaceService::class.java)
    Mockito
      .`when`(workspaceService.getOrganizationIdFromWorkspaceId(WORKSPACE_ID))
      .thenReturn(Optional.of<UUID>(ORGANIZATION_ID))
    Mockito.`when`(workspaceService.getOrganizationIdFromWorkspaceId(A_DIFFERENT_WORKSPACE_ID)).thenReturn(
      Optional.of<UUID>(
        A_DIFFERENT_ORGANIZATION_ID,
      ),
    )

    val secretPersistenceConfig = Mockito.mock(SecretPersistenceConfig::class.java)
    Mockito
      .`when`(secretPersistenceConfigService.get(ScopeType.ORGANIZATION, ORGANIZATION_ID))
      .thenReturn(secretPersistenceConfig)
    Mockito.`when`<JsonNode?>(secretsRepositoryReader.hydrateConfigFromDefaultSecretPersistence(CONFIG)).thenReturn(CONFIG)

    oAuthService =
      OAuthServiceJooqImpl(
        database,
        featureFlagClient,
        secretsRepositoryReader,
        secretPersistenceConfigService,
        metricClient,
        workspaceService,
      )

    deleteActorOAuthParams()
  }

  @Throws(SQLException::class)
  private fun deleteActorOAuthParams() {
    database!!.query<Int?>(
      ContextQueryFunction { ctx: DSLContext? ->
        ctx!!
          .deleteFrom(ACTOR_OAUTH_PARAMETER_TABLE)
          .where(ACTOR_DEFINITION_ID_COLUMN.eq(ACTOR_DEFINITION_ID))
          .execute()
      },
    )
  }

  @Nested
  internal inner class SourceOauthTests {
    @Test
    @Throws(IOException::class, ConfigNotFoundException::class)
    fun testGetInstanceWideSourceOAuthParam() {
      createActorOAuthParameter(ID, Optional.empty(), Optional.empty(), ActorType.SOURCE)

      val fetchedOAuthParam =
        oAuthService.getSourceOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID)

      Assertions.assertTrue(fetchedOAuthParam.isPresent)

      Assertions.assertEquals(CONFIG, fetchedOAuthParam.get().configuration)
      Assertions.assertNull(fetchedOAuthParam.get().workspaceId)
      Assertions.assertNull(fetchedOAuthParam.get().organizationId)
    }

    @Test
    @Throws(IOException::class, ConfigNotFoundException::class)
    fun testGetNoInstanceWideSourceOAuthParam() {
      val fetchedOAuthParam =
        oAuthService.getSourceOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID)
      Assertions.assertTrue(fetchedOAuthParam.isEmpty())
    }

    @Test
    @Throws(IOException::class, ConfigNotFoundException::class)
    fun testGetWorkspaceSourceOAuthParam() {
      createActorOAuthParameter(ID, Optional.of<UUID>(WORKSPACE_ID), Optional.empty(), ActorType.SOURCE)

      val fetchedOAuthParam =
        oAuthService.getSourceOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID)

      Assertions.assertTrue(fetchedOAuthParam.isPresent)

      Assertions.assertEquals(CONFIG, fetchedOAuthParam.get().configuration)
      Assertions.assertEquals(WORKSPACE_ID, fetchedOAuthParam.get().workspaceId)
      Assertions.assertNull(fetchedOAuthParam.get().organizationId)
    }

    @Test
    @Throws(IOException::class, ConfigNotFoundException::class)
    fun testGetWorkspaceSourceOAuthParamWithDifferentWorkspaceId() {
      createActorOAuthParameter(ID, Optional.of<UUID>(WORKSPACE_ID), Optional.empty(), ActorType.SOURCE)

      val fetchedOAuthParam =
        oAuthService.getSourceOAuthParameterWithSecretsOptional(A_DIFFERENT_WORKSPACE_ID, ACTOR_DEFINITION_ID)

      Assertions.assertTrue(fetchedOAuthParam.isEmpty())
    }

    @Test
    @Throws(IOException::class, ConfigNotFoundException::class)
    fun testGetOrganizationSourceOAuthParam() {
      createActorOAuthParameter(ID, Optional.empty(), Optional.of<UUID>(ORGANIZATION_ID), ActorType.SOURCE)

      val fetchedOAuthParam =
        oAuthService.getSourceOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID)

      Assertions.assertTrue(fetchedOAuthParam.isPresent)

      Assertions.assertEquals(CONFIG, fetchedOAuthParam.get().configuration)
      Assertions.assertEquals(ORGANIZATION_ID, fetchedOAuthParam.get().organizationId)
      Assertions.assertNull(fetchedOAuthParam.get().workspaceId)
    }

    @Test
    @Throws(IOException::class, ConfigNotFoundException::class)
    fun testGetOrganizationSourceOAuthParamWrongOrg() {
      createActorOAuthParameter(ID, Optional.empty(), Optional.of<UUID>(A_DIFFERENT_ORGANIZATION_ID), ActorType.SOURCE)

      val fetchedOAuthParam =
        oAuthService.getSourceOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID)

      Assertions.assertTrue(fetchedOAuthParam.isEmpty())
    }

    @Test
    @Throws(IOException::class, ConfigNotFoundException::class)
    fun testOrganizationSourceOverridesArePrioritizedOverOrgWideOverridesWithInstanceWideCreatedFirst() {
      val instanceWideParamId = UUID.randomUUID()
      val organizationWideParamId = UUID.randomUUID()

      createActorOAuthParameter(instanceWideParamId, Optional.empty(), Optional.empty(), ActorType.SOURCE)
      createActorOAuthParameter(organizationWideParamId, Optional.empty(), Optional.of<UUID>(ORGANIZATION_ID), ActorType.SOURCE)

      val fetchedOAuthParam =
        oAuthService.getSourceOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID)

      Assertions.assertTrue(fetchedOAuthParam.isPresent)
      Assertions.assertEquals(organizationWideParamId, fetchedOAuthParam.get().oauthParameterId)
    }

    @Test
    @Throws(IOException::class, ConfigNotFoundException::class)
    fun testOrganizationSourceOverridesArePrioritizedOverOrgWideOverridesWithInstanceWideCreatedLast() {
      val instanceWideParamId = UUID.randomUUID()
      val organizationWideParamId = UUID.randomUUID()

      createActorOAuthParameter(organizationWideParamId, Optional.empty(), Optional.of<UUID>(ORGANIZATION_ID), ActorType.SOURCE)
      createActorOAuthParameter(instanceWideParamId, Optional.empty(), Optional.empty(), ActorType.SOURCE)

      val fetchedOAuthParam =
        oAuthService.getSourceOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID)

      Assertions.assertTrue(fetchedOAuthParam.isPresent)
      Assertions.assertEquals(organizationWideParamId, fetchedOAuthParam.get().oauthParameterId)
    }

    @Test
    @Throws(IOException::class, ConfigNotFoundException::class)
    fun testWorkspaceSourceOverridesArePrioritizedOverOrgWideOverridesWithInstanceWideCreatedFirst() {
      val instanceWideParamId = UUID.randomUUID()
      val workspaceWideParamId = UUID.randomUUID()

      createActorOAuthParameter(instanceWideParamId, Optional.empty(), Optional.empty(), ActorType.SOURCE)
      createActorOAuthParameter(workspaceWideParamId, Optional.of<UUID>(WORKSPACE_ID), Optional.empty(), ActorType.SOURCE)

      val orgWideParam =
        SourceOAuthParameter()
          .withSourceDefinitionId(ACTOR_DEFINITION_ID)
          .withOauthParameterId(workspaceWideParamId)
          .withWorkspaceId(WORKSPACE_ID)
          .withConfiguration(CONFIG)
      oAuthService.writeSourceOAuthParam(orgWideParam)

      val fetchedOAuthParam =
        oAuthService.getSourceOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID)

      Assertions.assertTrue(fetchedOAuthParam.isPresent)
      Assertions.assertEquals(workspaceWideParamId, fetchedOAuthParam.get().oauthParameterId)
    }

    @Test
    @Throws(IOException::class, ConfigNotFoundException::class)
    fun testWorkspaceSourceOverridesArePrioritizedOverOrgWideOverridesWithInstanceWideCreatedLast() {
      val instanceWideParamId = UUID.randomUUID()
      val workspaceWideParamId = UUID.randomUUID()

      createActorOAuthParameter(workspaceWideParamId, Optional.of<UUID>(WORKSPACE_ID), Optional.empty(), ActorType.SOURCE)
      createActorOAuthParameter(instanceWideParamId, Optional.empty(), Optional.empty(), ActorType.SOURCE)

      val fetchedOAuthParam =
        oAuthService.getSourceOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID)

      Assertions.assertTrue(fetchedOAuthParam.isPresent)
      Assertions.assertEquals(workspaceWideParamId, fetchedOAuthParam.get().oauthParameterId)
    }

    @Test
    @Throws(IOException::class, ConfigNotFoundException::class)
    fun testWorkspaceSourceOverridesArePrioritizedOverOrgWideOverridesWithWorkspaceOverrideCreatedFirst() {
      val workspaceWideParamId = UUID.randomUUID()
      val organizationWideParamId = UUID.randomUUID()

      createActorOAuthParameter(workspaceWideParamId, Optional.of<UUID>(WORKSPACE_ID), Optional.empty(), ActorType.SOURCE)
      createActorOAuthParameter(organizationWideParamId, Optional.empty(), Optional.of<UUID>(ORGANIZATION_ID), ActorType.SOURCE)

      val fetchedOAuthParam =
        oAuthService.getSourceOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID)

      Assertions.assertTrue(fetchedOAuthParam.isPresent)
      Assertions.assertEquals(workspaceWideParamId, fetchedOAuthParam.get().oauthParameterId)
    }

    @Test
    @Throws(IOException::class, ConfigNotFoundException::class)
    fun testWorkspaceSourceOverridesArePrioritizedOverOrgWideOverridesWithWorkspaceOverrideCreatedLast() {
      val workspaceWideParamId = UUID.randomUUID()
      val organizationWideParamId = UUID.randomUUID()

      createActorOAuthParameter(organizationWideParamId, Optional.empty(), Optional.of<UUID>(ORGANIZATION_ID), ActorType.SOURCE)
      createActorOAuthParameter(workspaceWideParamId, Optional.of<UUID>(WORKSPACE_ID), Optional.empty(), ActorType.SOURCE)

      val fetchedOAuthParam =
        oAuthService.getSourceOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID)

      Assertions.assertTrue(fetchedOAuthParam.isPresent)
      Assertions.assertEquals(workspaceWideParamId, fetchedOAuthParam.get().oauthParameterId)
    }
  }

  @Nested
  internal inner class DestinationOAuthTests {
    @Test
    @Throws(IOException::class, ConfigNotFoundException::class)
    fun testGetInstanceWideDestinatonOAuthParam() {
      createActorOAuthParameter(ID, Optional.empty(), Optional.empty(), ActorType.DESTINATION)

      val fetchedOAuthParam =
        oAuthService.getDestinationOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID)

      Assertions.assertTrue(fetchedOAuthParam.isPresent)

      Assertions.assertEquals(CONFIG, fetchedOAuthParam.get().configuration)
      Assertions.assertNull(fetchedOAuthParam.get().workspaceId)
      Assertions.assertNull(fetchedOAuthParam.get().organizationId)
    }

    @Test
    @Throws(IOException::class, ConfigNotFoundException::class)
    fun testGetNoInstanceWideDestinationOAuthParam() {
      val fetchedOAuthParam =
        oAuthService.getDestinationOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID)
      Assertions.assertTrue(fetchedOAuthParam.isEmpty())
    }

    @Test
    @Throws(IOException::class, ConfigNotFoundException::class)
    fun testGetWorkspaceDestinationOAuthParam() {
      createActorOAuthParameter(ID, Optional.of<UUID>(WORKSPACE_ID), Optional.empty(), ActorType.DESTINATION)

      val fetchedOAuthParam =
        oAuthService.getDestinationOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID)

      Assertions.assertTrue(fetchedOAuthParam.isPresent)

      Assertions.assertEquals(CONFIG, fetchedOAuthParam.get().configuration)
      Assertions.assertEquals(WORKSPACE_ID, fetchedOAuthParam.get().workspaceId)
      Assertions.assertNull(fetchedOAuthParam.get().organizationId)
    }

    @Test
    @Throws(IOException::class, ConfigNotFoundException::class)
    fun testGetWorkspaceDestinationOAuthParamWithDifferentWorkspaceId() {
      createActorOAuthParameter(ID, Optional.of<UUID>(WORKSPACE_ID), Optional.empty(), ActorType.DESTINATION)

      val fetchedOAuthParam =
        oAuthService.getDestinationOAuthParameterWithSecretsOptional(A_DIFFERENT_WORKSPACE_ID, ACTOR_DEFINITION_ID)

      Assertions.assertTrue(fetchedOAuthParam.isEmpty())
    }

    @Test
    @Throws(IOException::class, ConfigNotFoundException::class)
    fun testGetOrganizationDestinationOAuthParam() {
      createActorOAuthParameter(ID, Optional.empty(), Optional.of<UUID>(ORGANIZATION_ID), ActorType.DESTINATION)

      val fetchedOAuthParam =
        oAuthService.getDestinationOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID)

      Assertions.assertTrue(fetchedOAuthParam.isPresent)

      Assertions.assertEquals(CONFIG, fetchedOAuthParam.get().configuration)
      Assertions.assertEquals(ORGANIZATION_ID, fetchedOAuthParam.get().organizationId)
      Assertions.assertNull(fetchedOAuthParam.get().workspaceId)
    }

    @Test
    @Throws(IOException::class, ConfigNotFoundException::class)
    fun testGetOrganizationDestinationOAuthParamWrongOrg() {
      createActorOAuthParameter(ID, Optional.empty(), Optional.of<UUID>(A_DIFFERENT_ORGANIZATION_ID), ActorType.DESTINATION)

      val fetchedOAuthParam =
        oAuthService.getDestinationOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID)

      Assertions.assertTrue(fetchedOAuthParam.isEmpty())
    }

    @Test
    @Throws(IOException::class, ConfigNotFoundException::class)
    fun testOrganizationDestinationOverridesArePrioritizedOverOrgWideOverridesWithInstanceWideCreatedFirst() {
      val instanceWideParamId = UUID.randomUUID()
      val organizationWideParamId = UUID.randomUUID()

      createActorOAuthParameter(instanceWideParamId, Optional.empty(), Optional.empty(), ActorType.DESTINATION)
      createActorOAuthParameter(organizationWideParamId, Optional.empty(), Optional.of<UUID>(ORGANIZATION_ID), ActorType.DESTINATION)

      val fetchedOAuthParam =
        oAuthService.getDestinationOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID)

      Assertions.assertTrue(fetchedOAuthParam.isPresent)
      Assertions.assertEquals(organizationWideParamId, fetchedOAuthParam.get().oauthParameterId)
    }

    @Test
    @Throws(IOException::class, ConfigNotFoundException::class)
    fun testOrganizationDestinationOverridesArePrioritizedOverOrgWideOverridesWithInstanceWideCreatedLast() {
      val instanceWideParamId = UUID.randomUUID()
      val organizationWideParamId = UUID.randomUUID()

      createActorOAuthParameter(organizationWideParamId, Optional.empty(), Optional.of<UUID>(ORGANIZATION_ID), ActorType.DESTINATION)
      createActorOAuthParameter(instanceWideParamId, Optional.empty(), Optional.empty(), ActorType.DESTINATION)

      val fetchedOAuthParam =
        oAuthService.getDestinationOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID)

      Assertions.assertTrue(fetchedOAuthParam.isPresent)
      Assertions.assertEquals(organizationWideParamId, fetchedOAuthParam.get().oauthParameterId)
    }

    @Test
    @Throws(IOException::class, ConfigNotFoundException::class)
    fun testWorkspaceDestinationOverridesArePrioritizedOverOrgWideOverridesWithInstanceWideCreatedFirst() {
      val instanceWideParamId = UUID.randomUUID()
      val workspaceWideParamId = UUID.randomUUID()

      createActorOAuthParameter(instanceWideParamId, Optional.empty(), Optional.empty(), ActorType.DESTINATION)
      createActorOAuthParameter(workspaceWideParamId, Optional.of<UUID>(WORKSPACE_ID), Optional.empty(), ActorType.DESTINATION)

      val fetchedOAuthParam =
        oAuthService.getDestinationOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID)

      Assertions.assertTrue(fetchedOAuthParam.isPresent)
      Assertions.assertEquals(workspaceWideParamId, fetchedOAuthParam.get().oauthParameterId)
    }

    @Test
    @Throws(IOException::class, ConfigNotFoundException::class)
    fun testWorkspaceDestinationOverridesArePrioritizedOverOrgWideOverridesWithInstanceWideCreatedLast() {
      val instanceWideParamId = UUID.randomUUID()
      val workspaceWideParamId = UUID.randomUUID()

      createActorOAuthParameter(workspaceWideParamId, Optional.of<UUID>(WORKSPACE_ID), Optional.empty(), ActorType.DESTINATION)
      createActorOAuthParameter(instanceWideParamId, Optional.empty(), Optional.empty(), ActorType.DESTINATION)

      val fetchedOAuthParam =
        oAuthService.getDestinationOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID)

      Assertions.assertTrue(fetchedOAuthParam.isPresent)
      Assertions.assertEquals(workspaceWideParamId, fetchedOAuthParam.get().oauthParameterId)
    }

    @Test
    @Throws(IOException::class, ConfigNotFoundException::class)
    fun testWorkspaceDestinationOverridesArePrioritizedOverOrgWideOverridesWithWorkspaceOverrideCreatedFirst() {
      val workspaceWideParamId = UUID.randomUUID()
      val organizationWideParamId = UUID.randomUUID()

      createActorOAuthParameter(workspaceWideParamId, Optional.of<UUID>(WORKSPACE_ID), Optional.empty(), ActorType.DESTINATION)
      createActorOAuthParameter(organizationWideParamId, Optional.empty(), Optional.of<UUID>(ORGANIZATION_ID), ActorType.DESTINATION)

      val fetchedOAuthParam =
        oAuthService.getDestinationOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID)

      Assertions.assertTrue(fetchedOAuthParam.isPresent)
      Assertions.assertEquals(workspaceWideParamId, fetchedOAuthParam.get().oauthParameterId)
    }

    @Test
    @Throws(IOException::class, ConfigNotFoundException::class)
    fun testWorkspaceDestinationOverridesArePrioritizedOverOrgWideOverridesWithWorkspaceOverrideCreatedLast() {
      val workspaceWideParamId = UUID.randomUUID()
      val organizationWideParamId = UUID.randomUUID()

      createActorOAuthParameter(organizationWideParamId, Optional.empty(), Optional.of<UUID>(ORGANIZATION_ID), ActorType.DESTINATION)
      createActorOAuthParameter(workspaceWideParamId, Optional.of<UUID>(WORKSPACE_ID), Optional.empty(), ActorType.DESTINATION)

      val fetchedOAuthParam =
        oAuthService.getDestinationOAuthParameterWithSecretsOptional(WORKSPACE_ID, ACTOR_DEFINITION_ID)

      Assertions.assertTrue(fetchedOAuthParam.isPresent)
      Assertions.assertEquals(workspaceWideParamId, fetchedOAuthParam.get().oauthParameterId)
    }
  }

  @Throws(IOException::class)
  private fun createActorOAuthParameter(
    id: UUID?,
    workspaceId: Optional<UUID>,
    organizationId: Optional<UUID>,
    actorType: ActorType?,
  ) {
    if (actorType == ActorType.SOURCE) {
      val param =
        SourceOAuthParameter()
          .withSourceDefinitionId(ACTOR_DEFINITION_ID)
          .withOauthParameterId(id)
          .withWorkspaceId(workspaceId.orElse(null))
          .withOrganizationId(organizationId.orElse(null))
          .withConfiguration(CONFIG)
      oAuthService.writeSourceOAuthParam(param)
    } else {
      val param =
        DestinationOAuthParameter()
          .withDestinationDefinitionId(ACTOR_DEFINITION_ID)
          .withOauthParameterId(id)
          .withWorkspaceId(workspaceId.orElse(null))
          .withOrganizationId(organizationId.orElse(null))
          .withConfiguration(CONFIG)
      oAuthService.writeDestinationOAuthParam(param)
    }
  }

  companion object {
    private val ACTOR_OAUTH_PARAMETER_TABLE = DSL.table("actor_oauth_parameter")
    private val ACTOR_DEFINITION_ID_COLUMN = DSL.field<UUID?>("actor_definition_id", SQLDataType.UUID)

    private val CONFIG = deserialize("{\"client_id\": \"client_id\", \"client_secret\": \"client_secret\"}")
    private val WORKSPACE_ID: UUID = UUID.randomUUID()
    private val A_DIFFERENT_WORKSPACE_ID: UUID = UUID.randomUUID()
    private val ORGANIZATION_ID: UUID = UUID.randomUUID()
    private val A_DIFFERENT_ORGANIZATION_ID: UUID = UUID.randomUUID()

    private val ACTOR_DEFINITION_ID: UUID = UUID.randomUUID()
    private val ID: UUID = UUID.randomUUID()
  }
}
