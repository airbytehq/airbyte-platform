/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.jooq

import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.DataplaneGroup
import io.airbyte.config.DestinationConnection
import io.airbyte.config.Organization
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardWorkspace
import io.airbyte.config.SupportLevel
import io.airbyte.config.Tag
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.ConnectionTimelineEventService
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.data.services.impls.data.DataplaneGroupServiceTestJooqImpl
import io.airbyte.data.services.shared.ActorServicePaginationHelper
import io.airbyte.db.ContextQueryFunction
import io.airbyte.db.instance.configs.jooq.generated.Tables
import io.airbyte.featureflag.HeartbeatMaxSecondsBetweenMessages
import io.airbyte.featureflag.TestClient
import io.airbyte.metrics.MetricClient
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.test.utils.BaseConfigDatabaseTest
import io.airbyte.validation.json.JsonValidationException
import io.mockk.every
import io.mockk.mockk
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import java.io.IOException
import java.sql.SQLException
import java.util.UUID
import java.util.stream.Collectors

class JooqTestDbSetupHelper : BaseConfigDatabaseTest() {
  private val sourceServiceJooqImpl: SourceServiceJooqImpl
  private val destinationServiceJooqImpl: DestinationServiceJooqImpl
  private val workspaceServiceJooqImpl: WorkspaceServiceJooqImpl
  private val organizationServiceJooqImpl: OrganizationServiceJooqImpl
  private val dataplaneGroupServiceDataImpl: DataplaneGroupService
  private val featureFlagClient: TestClient
  private val organizationId: UUID = UUID.randomUUID()
  private val workspaceId: UUID = UUID.randomUUID()
  private val sourceDefinitionId: UUID = UUID.randomUUID()
  private val destinationDefinitionId: UUID = UUID.randomUUID()
  private val dockerImageTag = "0.0.1"
  private val dataplaneGroupId: UUID = UUID.randomUUID()
  var organization: Organization? = null
  var workspace: StandardWorkspace? = null
  var sourceDefinition: StandardSourceDefinition? = null
  var destinationDefinition: StandardDestinationDefinition? = null
  var sourceDefinitionVersion: ActorDefinitionVersion? = null
  var destinationDefinitionVersion: ActorDefinitionVersion? = null
  var source: SourceConnection? = null
    private set
  var destination: DestinationConnection? = null
    private set
  var tags: MutableList<Tag?>? = null
    private set
  var tagsFromAnotherWorkspace: MutableList<Tag?>? = null
    private set

  init {
    this.featureFlagClient =
      mockk<TestClient> {
        every { stringVariation(HeartbeatMaxSecondsBetweenMessages, any()) } returns "3600"
      }
    val metricClient = mockk<MetricClient>(relaxed = true)
    val secretsRepositoryReader = mockk<SecretsRepositoryReader>(relaxed = true)
    val secretsRepositoryWriter = mockk<SecretsRepositoryWriter>(relaxed = true)
    val secretPersistenceConfigService = mockk<SecretPersistenceConfigService>(relaxed = true)
    val connectionService = mockk<ConnectionService>(relaxed = true)
    val scopedConfigurationService = mockk<ScopedConfigurationService>(relaxed = true)
    val connectionTimelineEventService = mockk<ConnectionTimelineEventService>(relaxed = true)
    val actorPaginationServiceHelper = mockk<ActorServicePaginationHelper>(relaxed = true)

    val actorDefinitionService: ActorDefinitionService = ActorDefinitionServiceJooqImpl(database)
    val actorDefinitionVersionUpdater =
      ActorDefinitionVersionUpdater(
        featureFlagClient,
        connectionService,
        actorDefinitionService,
        scopedConfigurationService,
        connectionTimelineEventService,
      )
    this.destinationServiceJooqImpl =
      DestinationServiceJooqImpl(
        database!!,
        featureFlagClient,
        connectionService,
        actorDefinitionVersionUpdater,
        metricClient,
        actorPaginationServiceHelper,
      )
    this.sourceServiceJooqImpl =
      SourceServiceJooqImpl(
        database!!,
        featureFlagClient,
        secretPersistenceConfigService,
        connectionService,
        actorDefinitionVersionUpdater,
        metricClient,
        actorPaginationServiceHelper,
      )
    this.dataplaneGroupServiceDataImpl = DataplaneGroupServiceTestJooqImpl(database!!)
    this.workspaceServiceJooqImpl =
      WorkspaceServiceJooqImpl(
        database,
        featureFlagClient,
        secretsRepositoryReader,
        secretsRepositoryWriter,
        secretPersistenceConfigService,
        metricClient,
      )
    this.organizationServiceJooqImpl = OrganizationServiceJooqImpl(database)
  }

  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
  fun setupForVersionUpgradeTest() {
    // Create org
    organization = createBaseOrganization()
    organizationServiceJooqImpl.writeOrganization(organization!!)

    // Create dataplane group
    dataplaneGroupServiceDataImpl.writeDataplaneGroup(createBaseDataplaneGroup()!!)

    // Create workspace
    workspace = createBaseWorkspace()
    workspaceServiceJooqImpl.writeStandardWorkspaceNoSecrets(createBaseWorkspace())

    // Create source definition
    sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(sourceDefinitionId)
        .withName("Test source def")
        .withTombstone(false)
    sourceDefinitionVersion = createBaseActorDefVersion(sourceDefinition!!.sourceDefinitionId, dockerImageTag)
    createActorDefinition(sourceDefinition!!, sourceDefinitionVersion!!)

    // Create destination definition
    destinationDefinition =
      StandardDestinationDefinition()
        .withDestinationDefinitionId(destinationDefinitionId)
        .withName("Test destination def")
        .withTombstone(false)
    destinationDefinitionVersion = createBaseActorDefVersion(destinationDefinition!!.destinationDefinitionId, dockerImageTag)
    createActorDefinition(destinationDefinition!!, destinationDefinitionVersion!!)

    // Create actors
    source = createActorForActorDefinition(sourceDefinition!!)
    destination = createActorForActorDefinition(destinationDefinition!!)

    // Verify initial source version
    val initialSourceDefinitionDefaultVersionId =
      sourceServiceJooqImpl.getStandardSourceDefinition(sourceDefinitionId).defaultVersionId
    Assertions.assertNotNull(initialSourceDefinitionDefaultVersionId)

    // Verify initial destination version
    val initialDestinationDefinitionDefaultVersionId =
      destinationServiceJooqImpl.getStandardDestinationDefinition(destinationDefinitionId).defaultVersionId
    Assertions.assertNotNull(initialDestinationDefinitionDefaultVersionId)
  }

  @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class, SQLException::class)
  fun setUpDependencies() {
    // Create org
    organization = createBaseOrganization()
    organizationServiceJooqImpl.writeOrganization(organization!!)

    // Create dataplane group
    dataplaneGroupServiceDataImpl.writeDataplaneGroup(createBaseDataplaneGroup()!!)

    // Create workspace
    workspace = createBaseWorkspace()
    workspaceServiceJooqImpl.writeStandardWorkspaceNoSecrets(createBaseWorkspace())

    // Create source definition
    sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(sourceDefinitionId)
        .withName("Test source def")
        .withTombstone(false)
    sourceDefinitionVersion = createBaseActorDefVersion(sourceDefinition!!.sourceDefinitionId, dockerImageTag)
    createActorDefinition(sourceDefinition!!, sourceDefinitionVersion!!)

    // Create destination definition
    destinationDefinition =
      StandardDestinationDefinition()
        .withDestinationDefinitionId(destinationDefinitionId)
        .withName("Test destination def")
        .withTombstone(false)
    destinationDefinitionVersion = createBaseActorDefVersion(destinationDefinition!!.destinationDefinitionId, dockerImageTag)
    createActorDefinition(destinationDefinition!!, destinationDefinitionVersion!!)

    // Create actors
    source = createActorForActorDefinition(sourceDefinition!!)
    destination = createActorForActorDefinition(destinationDefinition!!)

    // Verify initial source version
    val initialSourceDefinitionDefaultVersionId =
      sourceServiceJooqImpl.getStandardSourceDefinition(sourceDefinitionId).defaultVersionId
    Assertions.assertNotNull(initialSourceDefinitionDefaultVersionId)

    // Verify initial destination version
    val initialDestinationDefinitionDefaultVersionId =
      destinationServiceJooqImpl.getStandardDestinationDefinition(destinationDefinitionId).defaultVersionId
    Assertions.assertNotNull(initialDestinationDefinitionDefaultVersionId)

    // Create connection tags
    tags = createTags(workspace!!.workspaceId)
    val secondWorkspace = createSecondWorkspace()
    workspaceServiceJooqImpl.writeStandardWorkspaceNoSecrets(secondWorkspace)
    tagsFromAnotherWorkspace = createTags(secondWorkspace.workspaceId)
  }

  @Throws(IOException::class)
  fun setupForGetActorDefinitionVersionByDockerRepositoryAndDockerImageTagTests(
    sourceDefinitionId: UUID?,
    name: String?,
    version: String?,
  ) {
    // Add another version of the source definition
    sourceDefinition =
      StandardSourceDefinition()
        .withSourceDefinitionId(sourceDefinitionId)
        .withName(name)
        .withTombstone(false)
    sourceDefinitionVersion = createBaseActorDefVersion(sourceDefinition!!.sourceDefinitionId, version)
    sourceDefinitionVersion!!.withDockerRepository(name).withDockerImageTag(version)
    createActorDefinition(sourceDefinition!!, sourceDefinitionVersion!!)
  }

  // It's kind of ugly and brittle to create the tags in this way, but since TagService is a micronaut
  // data service, we cannot instantiate it here and use it to create the tags
  @Throws(SQLException::class)
  fun createTags(workspaceId: UUID?): MutableList<Tag?> {
    val tagOne =
      Tag()
        .withName("tag_one")
        .withTagId(UUID.randomUUID())
        .withWorkspaceId(workspaceId)
        .withColor("111111")
    val tagTwo =
      Tag()
        .withName("tag_two")
        .withTagId(UUID.randomUUID())
        .withWorkspaceId(workspaceId)
        .withColor("222222")
    val tagThree =
      Tag()
        .withName("tag_three")
        .withTagId(UUID.randomUUID())
        .withWorkspaceId(workspaceId)
        .withColor("333333")
    val tags = mutableListOf<Tag?>(tagOne, tagTwo, tagThree)

    database!!.query<IntArray?>(
      ContextQueryFunction { ctx: DSLContext? ->
        val records =
          tags
            .stream()
            .map { tag: Tag? ->
              val record = DSL.using(ctx!!.configuration()).newRecord(Tables.TAG)
              record.id = tag!!.tagId
              record.workspaceId = workspaceId
              record.name = tag.name
              record.color = tag.color
              record
            }.collect(Collectors.toList())
        ctx!!.batchInsert(records).execute()
      },
    )

    return tags
  }

  @Throws(IOException::class)
  fun createActorDefinition(
    sourceDefinition: StandardSourceDefinition,
    actorDefinitionVersion: ActorDefinitionVersion,
  ) {
    sourceServiceJooqImpl.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion, mutableListOf())
  }

  @Throws(IOException::class)
  fun createActorDefinition(
    destinationDefinition: StandardDestinationDefinition,
    actorDefinitionVersion: ActorDefinitionVersion,
  ) {
    destinationServiceJooqImpl.writeConnectorMetadata(
      destinationDefinition,
      actorDefinitionVersion,
      mutableListOf(),
    )
  }

  @Throws(IOException::class)
  fun createActorForActorDefinition(sourceDefinition: StandardSourceDefinition): SourceConnection {
    val source = createBaseSourceActor()!!.withSourceDefinitionId(sourceDefinition.sourceDefinitionId)
    sourceServiceJooqImpl.writeSourceConnectionNoSecrets(source)
    return source
  }

  @Throws(IOException::class)
  fun createActorForActorDefinition(destinationDefinition: StandardDestinationDefinition): DestinationConnection {
    val destination =
      createBaseDestinationActor()!!.withDestinationDefinitionId(destinationDefinition.destinationDefinitionId)
    destinationServiceJooqImpl.writeDestinationConnectionNoSecrets(destination)
    return destination
  }

  private fun createBaseDestinationActor(): DestinationConnection? =
    DestinationConnection()
      .withDestinationId(UUID.randomUUID())
      .withWorkspaceId(workspaceId)
      .withName("destination")

  private fun createBaseSourceActor(): SourceConnection? =
    SourceConnection()
      .withSourceId(UUID.randomUUID())
      .withWorkspaceId(workspaceId)
      .withName("source")

  private fun createBaseOrganization(): Organization =
    Organization()
      .withOrganizationId(organizationId)
      .withName("organization")
      .withEmail("org@airbyte.io")

  private fun createBaseDataplaneGroup(): DataplaneGroup? =
    DataplaneGroup()
      .withId(dataplaneGroupId)
      .withOrganizationId(organizationId)
      .withName("test")
      .withEnabled(true)
      .withTombstone(false)

  private fun createBaseWorkspace(): StandardWorkspace =
    StandardWorkspace()
      .withWorkspaceId(workspaceId)
      .withOrganizationId(organizationId)
      .withName("default")
      .withSlug("workspace-slug")
      .withInitialSetupComplete(false)
      .withTombstone(false)
      .withDataplaneGroupId(dataplaneGroupId)

  private fun createSecondWorkspace(): StandardWorkspace =
    StandardWorkspace()
      .withWorkspaceId(UUID.randomUUID())
      .withOrganizationId(organizationId)
      .withName("second")
      .withSlug("second-workspace-slug")
      .withInitialSetupComplete(false)
      .withTombstone(false)
      .withDataplaneGroupId(dataplaneGroupId)

  companion object {
    private fun createBaseActorDefVersion(
      actorDefId: UUID?,
      dockerImageTag: String?,
    ): ActorDefinitionVersion =
      ActorDefinitionVersion()
        .withActorDefinitionId(actorDefId)
        .withDockerRepository("destination-image-" + actorDefId)
        .withDockerImageTag(dockerImageTag)
        .withProtocolVersion("1.0.0")
        .withSupportLevel(SupportLevel.CERTIFIED)
        .withInternalSupportLevel(200L)
        .withSpec(
          ConnectorSpecification()
            .withConnectionSpecification(jsonNode(mutableMapOf("key" to "value1")))
            .withProtocolVersion("1.0.0"),
        )
  }
}
