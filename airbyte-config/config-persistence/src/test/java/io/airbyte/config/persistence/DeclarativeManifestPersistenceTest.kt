/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.config.DataplaneGroup
import io.airbyte.config.ScopeType
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.ConnectionTimelineEventService
import io.airbyte.data.services.ConnectorBuilderService
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.impls.jooq.ActorDefinitionServiceJooqImpl
import io.airbyte.data.services.impls.jooq.ConnectorBuilderServiceJooqImpl
import io.airbyte.data.services.impls.jooq.OrganizationServiceJooqImpl
import io.airbyte.data.services.impls.jooq.SourceServiceJooqImpl
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl
import io.airbyte.data.services.shared.ActorServicePaginationHelper
import io.airbyte.featureflag.HeartbeatMaxSecondsBetweenMessages
import io.airbyte.featureflag.SourceDefinition
import io.airbyte.featureflag.TestClient
import io.airbyte.metrics.MetricClient
import io.airbyte.test.utils.BaseConfigDatabaseTest
import io.airbyte.validation.json.JsonValidationException
import org.jooq.exception.DataAccessException
import org.junit.Assert
import org.junit.function.ThrowingRunnable
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import java.io.IOException
import java.util.UUID
import org.mockito.Mockito.`when` as whenever

internal class DeclarativeManifestPersistenceTest : BaseConfigDatabaseTest() {
  private var connectorBuilderService: ConnectorBuilderService? = null
  private var sourceService: SourceService? = null
  private var actorDefinitionService: ActorDefinitionService? = null
  private var workspaceService: WorkspaceService? = null

  @BeforeEach
  @Throws(Exception::class)
  fun beforeEach() {
    truncateAllTables()

    val featureFlagClient = mock<TestClient>()
    whenever(
      featureFlagClient.stringVariation(org.mockito.kotlin.eq(HeartbeatMaxSecondsBetweenMessages), org.mockito.kotlin.any<SourceDefinition>()),
    ).thenReturn("3600")

    val secretsRepositoryReader = mock<SecretsRepositoryReader>()
    val secretsRepositoryWriter = mock<SecretsRepositoryWriter>()
    val secretPersistenceConfigService = mock<SecretPersistenceConfigService>()

    val connectionService = mock<ConnectionService>()
    val scopedConfigurationService = mock<ScopedConfigurationService>()
    val organizationService = OrganizationServiceJooqImpl(database)
    val connectionTimelineEventService = mock<ConnectionTimelineEventService>()

    actorDefinitionService = ActorDefinitionServiceJooqImpl(database)

    val actorDefinitionVersionUpdater =
      ActorDefinitionVersionUpdater(
        featureFlagClient,
        connectionService,
        actorDefinitionService!!,
        scopedConfigurationService,
        connectionTimelineEventService,
      )

    val metricClient = mock<MetricClient>()
    val dataplaneGroupService = mock<DataplaneGroupService>()
    val actorPaginationServiceHelper = mock<ActorServicePaginationHelper>()

    whenever(dataplaneGroupService.getDataplaneGroupByOrganizationIdAndName(org.mockito.kotlin.any(), org.mockito.kotlin.any()))
      .thenReturn(DataplaneGroup().withId(UUID.randomUUID()))

    sourceService =
      SourceServiceJooqImpl(
        database!!,
        featureFlagClient,
        secretPersistenceConfigService,
        connectionService,
        actorDefinitionVersionUpdater,
        metricClient,
        actorPaginationServiceHelper,
      )

    workspaceService =
      WorkspaceServiceJooqImpl(
        database!!,
        featureFlagClient,
        secretsRepositoryReader,
        secretsRepositoryWriter,
        secretPersistenceConfigService,
        metricClient,
      )

    connectorBuilderService = ConnectorBuilderServiceJooqImpl(database!!)

    organizationService.writeOrganization(MockData.defaultOrganization())
  }

  @Test
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun whenInsertDeclarativeManifestThenEntryIsInDb() {
    val manifest = MockData.declarativeManifest()!!.withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withVersion(A_VERSION)
    connectorBuilderService!!.insertDeclarativeManifest(manifest)
    Assertions.assertEquals(
      manifest,
      connectorBuilderService!!.getDeclarativeManifestByActorDefinitionIdAndVersion(AN_ACTOR_DEFINITION_ID, A_VERSION),
    )
  }

  @Test
  @Throws(IOException::class)
  fun givenActorDefinitionIdAndVersionAlreadyInDbWhenInsertDeclarativeManifestThenThrowException() {
    val manifest = MockData.declarativeManifest()!!
    connectorBuilderService!!.insertDeclarativeManifest(manifest)
    Assert.assertThrows<DataAccessException?>(
      DataAccessException::class.java,
      ThrowingRunnable { connectorBuilderService!!.insertDeclarativeManifest(manifest) },
    )
  }

  @Test
  fun givenManifestIsNullWhenInsertDeclarativeManifestThenThrowException() {
    val declarativeManifestWithoutManifest = MockData.declarativeManifest()!!.withManifest(null)
    Assert.assertThrows<DataAccessException?>(
      DataAccessException::class.java,
      ThrowingRunnable { connectorBuilderService!!.insertDeclarativeManifest(declarativeManifestWithoutManifest) },
    )
  }

  @Test
  fun givenSpecIsNullWhenInsertDeclarativeManifestThenThrowException() {
    val declarativeManifestWithoutManifest = MockData.declarativeManifest()!!.withSpec(null)
    Assert.assertThrows<DataAccessException?>(
      DataAccessException::class.java,
      ThrowingRunnable { connectorBuilderService!!.insertDeclarativeManifest(declarativeManifestWithoutManifest) },
    )
  }

  @Test
  @Throws(IOException::class)
  fun whenGetDeclarativeManifestsByActorDefinitionIdThenReturnDeclarativeManifestWithoutManifestAndSpec() {
    val declarativeManifest =
      MockData
        .declarativeManifest()!!
        .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
        .withManifest(A_MANIFEST)
        .withSpec(A_SPEC)
    connectorBuilderService!!.insertDeclarativeManifest(declarativeManifest)

    val result =
      connectorBuilderService!!.getDeclarativeManifestsByActorDefinitionId(AN_ACTOR_DEFINITION_ID).findFirst().orElse(null)

    Assertions.assertEquals(declarativeManifest.withManifest(null).withSpec(null), result)
  }

  @Test
  @Throws(IOException::class)
  fun givenManyEntriesMatchingWhenGetDeclarativeManifestsByActorDefinitionIdThenReturnAllEntries() {
    connectorBuilderService!!.insertDeclarativeManifest(
      MockData.declarativeManifest()!!.withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withVersion(1L),
    )
    connectorBuilderService!!.insertDeclarativeManifest(
      MockData.declarativeManifest()!!.withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withVersion(2L),
    )

    val manifests = connectorBuilderService!!.getDeclarativeManifestsByActorDefinitionId(AN_ACTOR_DEFINITION_ID).toList()

    Assertions.assertEquals(2, manifests.size)
  }

  @Test
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun whenGetDeclarativeManifestByActorDefinitionIdAndVersionThenReturnDeclarativeManifest() {
    val declarativeManifest =
      MockData.declarativeManifest()!!.withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withVersion(A_VERSION)
    connectorBuilderService!!.insertDeclarativeManifest(declarativeManifest)

    val result = connectorBuilderService!!.getDeclarativeManifestByActorDefinitionIdAndVersion(AN_ACTOR_DEFINITION_ID, A_VERSION)

    Assertions.assertEquals(declarativeManifest, result)
  }

  @Test
  fun givenNoDeclarativeManifestMatchingWhenGetDeclarativeManifestByActorDefinitionIdAndVersionThenThrowException() {
    Assert.assertThrows<ConfigNotFoundException?>(
      ConfigNotFoundException::class.java,
      ThrowingRunnable { connectorBuilderService!!.getDeclarativeManifestByActorDefinitionIdAndVersion(AN_ACTOR_DEFINITION_ID, A_VERSION) },
    )
  }

  @Test
  @Throws(IOException::class, ConfigNotFoundException::class)
  fun whenGetCurrentlyActiveDeclarativeManifestsByActorDefinitionIdThenReturnDeclarativeManifest() {
    val activeDeclarativeManifest =
      MockData.declarativeManifest()!!.withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withVersion(A_VERSION)
    connectorBuilderService!!
      .insertDeclarativeManifest(MockData.declarativeManifest()!!.withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withVersion(ANOTHER_VERSION))
    connectorBuilderService!!.insertActiveDeclarativeManifest(activeDeclarativeManifest)

    val result = connectorBuilderService!!.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(AN_ACTOR_DEFINITION_ID)

    Assertions.assertEquals(activeDeclarativeManifest, result)
  }

  @Test
  @Throws(IOException::class)
  fun givenNoActiveManifestWhenGetCurrentlyActiveDeclarativeManifestsByActorDefinitionIdThenReturnDeclarativeManifest() {
    connectorBuilderService!!
      .insertDeclarativeManifest(MockData.declarativeManifest()!!.withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withVersion(A_VERSION))
    Assert.assertThrows<ConfigNotFoundException?>(
      ConfigNotFoundException::class.java,
      ThrowingRunnable { connectorBuilderService!!.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(AN_ACTOR_DEFINITION_ID) },
    )
  }

  @Test
  @Throws(IOException::class, ConfigNotFoundException::class, JsonValidationException::class)
  fun whenCreateDeclarativeManifestAsActiveVersionThenUpdateSourceDefinitionAndConfigInjectionAndDeclarativeManifest() {
    givenSourceDefinition(AN_ACTOR_DEFINITION_ID)
    val declarativeManifest =
      MockData
        .declarativeManifest()!!
        .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
        .withManifest(A_MANIFEST)
        .withSpec(createSpec(A_SPEC))
    val configInjection =
      MockData
        .actorDefinitionConfigInjection()!!
        .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
        .withInjectionPath(A_MANIFEST_KEY)
        .withJsonToInject(A_MANIFEST)
    val connectorSpecification = MockData.connectorSpecification()!!.withConnectionSpecification(A_SPEC!!)

    connectorBuilderService!!.createDeclarativeManifestAsActiveVersion(
      declarativeManifest,
      listOf(configInjection),
      connectorSpecification,
      A_CDK_VERSION,
    )

    val sourceDefinition = sourceService!!.getStandardSourceDefinition(AN_ACTOR_DEFINITION_ID)
    Assertions.assertEquals(
      connectorSpecification,
      actorDefinitionService!!.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId())!!.getSpec(),
    )
    Assertions.assertEquals(
      A_CDK_VERSION,
      actorDefinitionService!!.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId())!!.getDockerImageTag(),
    )
    Assertions.assertEquals(
      listOf(configInjection),
      connectorBuilderService!!
        .getActorDefinitionConfigInjections(
          AN_ACTOR_DEFINITION_ID,
        ).toList(),
    )
    Assertions.assertEquals(
      declarativeManifest,
      connectorBuilderService!!.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(
        AN_ACTOR_DEFINITION_ID,
      ),
    )
  }

  @Test
  fun givenSourceDefinitionDoesNotExistWhenCreateDeclarativeManifestAsActiveVersionThenThrowException() {
    val configInjection =
      MockData
        .actorDefinitionConfigInjection()!!
        .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
        .withInjectionPath(A_MANIFEST_KEY)
        .withJsonToInject(A_MANIFEST)
    Assert.assertThrows<DataAccessException?>(
      DataAccessException::class.java,
      ThrowingRunnable {
        connectorBuilderService!!.createDeclarativeManifestAsActiveVersion(
          MockData
            .declarativeManifest()!!
            .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
            .withManifest(A_MANIFEST)
            .withSpec(createSpec(A_SPEC)),
          listOf(configInjection),
          MockData.connectorSpecification()!!.withConnectionSpecification(A_SPEC),
          A_CDK_VERSION,
        )
      },
    )
  }

  @Test
  fun givenActorDefinitionIdMismatchWhenCreateDeclarativeManifestAsActiveVersionThenThrowException() {
    Assert.assertThrows<IllegalArgumentException?>(
      IllegalArgumentException::class.java,
      ThrowingRunnable {
        connectorBuilderService!!.createDeclarativeManifestAsActiveVersion(
          MockData
            .declarativeManifest()!!
            .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
            .withManifest(A_MANIFEST)
            .withSpec(createSpec(A_SPEC)),
          listOf(
            MockData.actorDefinitionConfigInjection()!!.withActorDefinitionId(ANOTHER_ACTOR_DEFINITION_ID).withJsonToInject(
              A_MANIFEST,
            ),
          ),
          MockData.connectorSpecification()!!.withConnectionSpecification(A_SPEC),
          A_CDK_VERSION,
        )
      },
    )
  }

  @Test
  fun givenManifestMismatchWhenCreateDeclarativeManifestAsActiveVersionThenThrowException() {
    Assert.assertThrows<IllegalArgumentException?>(
      IllegalArgumentException::class.java,
      ThrowingRunnable {
        connectorBuilderService!!.createDeclarativeManifestAsActiveVersion(
          MockData
            .declarativeManifest()!!
            .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
            .withManifest(A_MANIFEST)
            .withSpec(createSpec(A_SPEC)),
          listOf(
            MockData.actorDefinitionConfigInjection()!!.withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withJsonToInject(
              ANOTHER_MANIFEST,
            ),
          ),
          MockData.connectorSpecification()!!.withConnectionSpecification(A_SPEC),
          A_CDK_VERSION,
        )
      },
    )
  }

  @Test
  fun givenSpecMismatchWhenCreateDeclarativeManifestAsActiveVersionThenThrowException() {
    Assert.assertThrows<IllegalArgumentException?>(
      IllegalArgumentException::class.java,
      ThrowingRunnable {
        connectorBuilderService!!.createDeclarativeManifestAsActiveVersion(
          MockData
            .declarativeManifest()!!
            .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
            .withManifest(A_MANIFEST)
            .withSpec(createSpec(A_SPEC)),
          listOf(
            MockData.actorDefinitionConfigInjection()!!.withActorDefinitionId(AN_ACTOR_DEFINITION_ID).withJsonToInject(
              A_MANIFEST,
            ),
          ),
          MockData.connectorSpecification()!!.withConnectionSpecification(ANOTHER_SPEC!!),
          A_CDK_VERSION,
        )
      },
    )
  }

  @Test
  @Throws(Exception::class)
  fun whenSetDeclarativeSourceActiveVersionThenUpdateSourceDefinitionAndConfigInjectionAndActiveDeclarativeManifest() {
    givenSourceDefinition(AN_ACTOR_DEFINITION_ID)
    connectorBuilderService!!.insertDeclarativeManifest(
      MockData
        .declarativeManifest()!!
        .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
        .withManifest(A_MANIFEST)
        .withVersion(A_VERSION),
    )
    val configInjection =
      MockData
        .actorDefinitionConfigInjection()!!
        .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
        .withInjectionPath(A_MANIFEST_KEY)
        .withJsonToInject(A_MANIFEST)
    val connectorSpecification = MockData.connectorSpecification()!!.withConnectionSpecification(A_SPEC!!)

    connectorBuilderService!!.setDeclarativeSourceActiveVersion(
      AN_ACTOR_DEFINITION_ID,
      A_VERSION,
      listOf(configInjection),
      connectorSpecification,
      A_CDK_VERSION,
    )

    val sourceDefinition = sourceService!!.getStandardSourceDefinition(AN_ACTOR_DEFINITION_ID)
    Assertions.assertEquals(
      connectorSpecification,
      actorDefinitionService!!.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId())!!.getSpec(),
    )
    Assertions.assertEquals(
      A_CDK_VERSION,
      actorDefinitionService!!.getActorDefinitionVersion(sourceDefinition.getDefaultVersionId())!!.getDockerImageTag(),
    )
    Assertions.assertEquals(
      listOf(configInjection),
      connectorBuilderService!!
        .getActorDefinitionConfigInjections(
          AN_ACTOR_DEFINITION_ID,
        ).toList(),
    )
    Assertions.assertEquals(
      A_VERSION,
      connectorBuilderService!!.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(AN_ACTOR_DEFINITION_ID).getVersion(),
    )
  }

  @Test
  fun givenSourceDefinitionDoesNotExistWhenSetDeclarativeSourceActiveVersionThenThrowException() {
    val configInjection =
      MockData
        .actorDefinitionConfigInjection()!!
        .withInjectionPath(A_MANIFEST_KEY)
        .withJsonToInject(A_MANIFEST)

    Assert.assertThrows<DataAccessException?>(
      DataAccessException::class.java,
      ThrowingRunnable {
        connectorBuilderService!!.setDeclarativeSourceActiveVersion(
          AN_ACTOR_DEFINITION_ID,
          A_VERSION,
          listOf(configInjection),
          MockData.connectorSpecification()!!,
          A_CDK_VERSION,
        )
      },
    )
  }

  @Test
  @Throws(Exception::class)
  fun givenActiveDeclarativeManifestDoesNotExistWhenSetDeclarativeSourceActiveVersionThenThrowException() {
    givenSourceDefinition(AN_ACTOR_DEFINITION_ID)
    val configInjection =
      MockData
        .actorDefinitionConfigInjection()!!
        .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
        .withInjectionPath(A_MANIFEST_KEY)
        .withJsonToInject(A_MANIFEST)
    Assert.assertThrows<DataAccessException?>(
      DataAccessException::class.java,
      ThrowingRunnable {
        connectorBuilderService!!.setDeclarativeSourceActiveVersion(
          AN_ACTOR_DEFINITION_ID,
          A_VERSION,
          listOf(configInjection),
          MockData.connectorSpecification()!!,
          A_CDK_VERSION,
        )
      },
    )
  }

  @Test
  @Throws(Exception::class)
  fun whenSetDeclarativeSourceActiveVersionMultipleTimesThenConfigInjectionsAreReplaced() {
    // Set up initial source definition
    givenSourceDefinition(AN_ACTOR_DEFINITION_ID)

    // Insert initial manifest
    connectorBuilderService!!.insertDeclarativeManifest(
      MockData
        .declarativeManifest()!!
        .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
        .withManifest(A_MANIFEST)
        .withVersion(A_VERSION),
    )

    // Create initial set of 3 config injections
    val initialConfigInjections =
      listOf(
        MockData
          .actorDefinitionConfigInjection()!!
          .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
          .withInjectionPath(A_MANIFEST_KEY)
          .withJsonToInject(A_MANIFEST),
        MockData
          .actorDefinitionConfigInjection()!!
          .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
          .withInjectionPath("path2")
          .withJsonToInject(A_MANIFEST),
        MockData
          .actorDefinitionConfigInjection()!!
          .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
          .withInjectionPath("path3")
          .withJsonToInject(A_MANIFEST),
      )

    // First call to setDeclarativeSourceActiveVersion with 3 injections
    connectorBuilderService!!.setDeclarativeSourceActiveVersion(
      AN_ACTOR_DEFINITION_ID,
      A_VERSION,
      initialConfigInjections,
      MockData.connectorSpecification()!!.withConnectionSpecification(A_SPEC!!),
      A_CDK_VERSION,
    )

    // Verify all 3 initial injections were added
    Assertions.assertEquals(3, connectorBuilderService!!.getActorDefinitionConfigInjections(AN_ACTOR_DEFINITION_ID).count())

    // Create new single config injection
    val replacementConfigInjection =
      listOf(
        MockData
          .actorDefinitionConfigInjection()!!
          .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
          .withInjectionPath(A_MANIFEST_KEY)
          .withJsonToInject(A_MANIFEST),
      )

    // Second call to setDeclarativeSourceActiveVersion with 1 injection
    connectorBuilderService!!.setDeclarativeSourceActiveVersion(
      AN_ACTOR_DEFINITION_ID,
      A_VERSION,
      replacementConfigInjection,
      MockData.connectorSpecification()!!.withConnectionSpecification(A_SPEC!!),
      A_CDK_VERSION,
    )

    // Verify only 1 injection remains
    val remainingInjections =
      connectorBuilderService!!.getActorDefinitionConfigInjections(AN_ACTOR_DEFINITION_ID).toList()
    Assertions.assertEquals(1, remainingInjections.size)
    Assertions.assertEquals(A_MANIFEST_KEY, remainingInjections[0]!!.getInjectionPath())
  }

  @Test
  @Throws(Exception::class)
  fun whenSetDeclarativeSourceActiveVersionWithMixedActorDefinitionIdsThenThrowException() {
    // Set up initial source definition
    givenSourceDefinition(AN_ACTOR_DEFINITION_ID)

    // Insert initial manifest
    connectorBuilderService!!.insertDeclarativeManifest(
      MockData
        .declarativeManifest()!!
        .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
        .withManifest(A_MANIFEST)
        .withVersion(A_VERSION),
    )

    // Create config injections with different actor definition IDs
    val mixedConfigInjections =
      listOf(
        MockData
          .actorDefinitionConfigInjection()!!
          .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
          .withInjectionPath(A_MANIFEST_KEY)
          .withJsonToInject(A_MANIFEST),
        MockData
          .actorDefinitionConfigInjection()!!
          .withActorDefinitionId(ANOTHER_ACTOR_DEFINITION_ID) // Different actor definition ID
          .withInjectionPath("path2")
          .withJsonToInject(A_MANIFEST),
      )

    // Verify that calling setDeclarativeSourceActiveVersion with mixed actor definition IDs throws
    // exception
    Assert.assertThrows<IllegalArgumentException?>(
      IllegalArgumentException::class.java,
      ThrowingRunnable {
        connectorBuilderService!!.setDeclarativeSourceActiveVersion(
          AN_ACTOR_DEFINITION_ID,
          A_VERSION,
          mixedConfigInjections,
          MockData.connectorSpecification()!!.withConnectionSpecification(A_SPEC),
          A_CDK_VERSION,
        )
      },
    )
  }

  @Test
  @Throws(Exception::class)
  fun whenSetDeclarativeSourceActiveVersionWithoutManifestInjectionThenThrowException() {
    // Set up initial source definition
    givenSourceDefinition(AN_ACTOR_DEFINITION_ID)

    // Insert initial manifest
    connectorBuilderService!!.insertDeclarativeManifest(
      MockData
        .declarativeManifest()!!
        .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
        .withManifest(A_MANIFEST)
        .withVersion(A_VERSION),
    )

    // Create config injections without manifest injection
    val configInjectionsWithoutManifest =
      listOf(
        MockData
          .actorDefinitionConfigInjection()!!
          .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
          .withInjectionPath("path1")
          .withJsonToInject(A_MANIFEST),
        MockData
          .actorDefinitionConfigInjection()!!
          .withActorDefinitionId(AN_ACTOR_DEFINITION_ID)
          .withInjectionPath("path2")
          .withJsonToInject(A_MANIFEST),
      )

    // Verify that calling setDeclarativeSourceActiveVersion without manifest injection throws exception
    Assert.assertThrows<IllegalArgumentException?>(
      IllegalArgumentException::class.java,
      ThrowingRunnable {
        connectorBuilderService!!.setDeclarativeSourceActiveVersion(
          AN_ACTOR_DEFINITION_ID,
          A_VERSION,
          configInjectionsWithoutManifest,
          MockData.connectorSpecification()!!.withConnectionSpecification(A_SPEC),
          A_CDK_VERSION,
        )
      },
    )
  }

  @Throws(IOException::class)
  fun givenActiveDeclarativeManifestWithActorDefinitionId(actorDefinitionId: UUID?) {
    val version = 4L
    connectorBuilderService!!
      .insertActiveDeclarativeManifest(MockData.declarativeManifest()!!.withActorDefinitionId(actorDefinitionId).withVersion(version))
  }

  @Throws(JsonValidationException::class, IOException::class)
  fun givenSourceDefinition(sourceDefinitionId: UUID?) {
    val workspaceId = UUID.randomUUID()
    workspaceService!!.writeStandardWorkspaceNoSecrets(MockData.standardWorkspaces().get(0)!!.withWorkspaceId(workspaceId))
    sourceService!!.writeCustomConnectorMetadata(
      MockData.customSourceDefinition()!!.withSourceDefinitionId(sourceDefinitionId),
      MockData.actorDefinitionVersion()!!.withActorDefinitionId(sourceDefinitionId),
      workspaceId,
      ScopeType.WORKSPACE,
    )
  }

  fun createSpec(connectionSpecification: JsonNode?): JsonNode? =
    ObjectMapper().createObjectNode().set<JsonNode?>("connectionSpecification", connectionSpecification)

  companion object {
    private val AN_ACTOR_DEFINITION_ID: UUID = UUID.randomUUID()
    private val ANOTHER_ACTOR_DEFINITION_ID: UUID = UUID.randomUUID()
    private const val A_MANIFEST_KEY = "__injected_declarative_manifest"
    private const val A_VERSION = 1L
    private const val ANOTHER_VERSION = 2L
    private const val A_CDK_VERSION = "0.29.0"
    private val A_MANIFEST: JsonNode?
    private val ANOTHER_MANIFEST: JsonNode?
    private val A_SPEC: JsonNode?
    private val ANOTHER_SPEC: JsonNode?

    init {
      try {
        A_MANIFEST = ObjectMapper().readTree("{\"a_manifest\": \"manifest_value\"}")
        ANOTHER_MANIFEST =
          ObjectMapper().readTree("{\"another_manifest\": \"another_manifest_value\"}")
        A_SPEC = ObjectMapper().readTree("{\"a_spec\": \"spec_value\"}")
        ANOTHER_SPEC = ObjectMapper().readTree("{\"another_spec\": \"another_spec_value\"}")
      } catch (e: JsonProcessingException) {
        throw RuntimeException(e)
      }
    }
  }
}
