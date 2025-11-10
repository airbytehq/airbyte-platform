/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import io.airbyte.commons.enums.isCompatible
import io.airbyte.commons.json.Jsons.clone
import io.airbyte.commons.json.Jsons.deserialize
import io.airbyte.commons.json.Jsons.serialize
import io.airbyte.config.DataplaneGroup
import io.airbyte.config.State
import io.airbyte.config.StateType
import io.airbyte.config.StateWrapper
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.ConnectionTimelineEventService
import io.airbyte.data.services.DataplaneGroupService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.SecretPersistenceConfigService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.impls.data.DataplaneGroupServiceTestJooqImpl
import io.airbyte.data.services.impls.jooq.ActorDefinitionServiceJooqImpl
import io.airbyte.data.services.impls.jooq.ConnectionServiceJooqImpl
import io.airbyte.data.services.impls.jooq.DestinationServiceJooqImpl
import io.airbyte.data.services.impls.jooq.OrganizationServiceJooqImpl
import io.airbyte.data.services.impls.jooq.SourceServiceJooqImpl
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl
import io.airbyte.data.services.shared.ActorServicePaginationHelper
import io.airbyte.featureflag.TestClient
import io.airbyte.metrics.MetricClient
import io.airbyte.protocol.models.v0.AirbyteGlobalState
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage.AirbyteStateType
import io.airbyte.protocol.models.v0.AirbyteStreamState
import io.airbyte.protocol.models.v0.StreamDescriptor
import io.airbyte.test.utils.BaseConfigDatabaseTest
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.impl.DSL
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import java.util.UUID

internal class StatePersistenceTest : BaseConfigDatabaseTest() {
  private var statePersistence: StatePersistence? = null
  private var connectionId: UUID? = null
  private var connectionService: ConnectionService? = null
  private var sourceService: SourceService? = null
  private var destinationService: DestinationService? = null
  private var workspaceService: WorkspaceService? = null

  @BeforeEach
  fun beforeEach() {
    truncateAllTables()

    statePersistence = StatePersistence(database, ConnectionServiceJooqImpl(database))

    val featureFlagClient = Mockito.mock(TestClient::class.java)
    val secretsRepositoryReader = Mockito.mock(SecretsRepositoryReader::class.java)
    val secretsRepositoryWriter = Mockito.mock(SecretsRepositoryWriter::class.java)
    val secretPersistenceConfigService = Mockito.mock(SecretPersistenceConfigService::class.java)
    val connectionTimelineEventService = Mockito.mock(ConnectionTimelineEventService::class.java)
    val metricClient = Mockito.mock(MetricClient::class.java)
    val actorPaginationServiceHelper = Mockito.mock(ActorServicePaginationHelper::class.java)

    val organizationService: OrganizationService = OrganizationServiceJooqImpl(database)
    organizationService.writeOrganization(MockData.defaultOrganization())

    val dataplaneGroupService: DataplaneGroupService = DataplaneGroupServiceTestJooqImpl(database!!)
    dataplaneGroupService.writeDataplaneGroup(
      DataplaneGroup()
        .withId(DATAPLANE_GROUP_ID)
        .withOrganizationId(MockData.defaultOrganization().organizationId)
        .withName("test")
        .withEnabled(true)
        .withTombstone(false),
    )

    connectionService = Mockito.mock(ConnectionService::class.java)
    val actorDefinitionVersionUpdater =
      ActorDefinitionVersionUpdater(
        featureFlagClient,
        connectionService!!,
        ActorDefinitionServiceJooqImpl(database),
        Mockito.mock(ScopedConfigurationService::class.java),
        connectionTimelineEventService,
      )
    connectionService = ConnectionServiceJooqImpl(database)
    sourceService =
      SourceServiceJooqImpl(
        database!!,
        featureFlagClient,
        secretPersistenceConfigService,
        connectionService!!,
        actorDefinitionVersionUpdater,
        metricClient,
        actorPaginationServiceHelper,
      )
    destinationService =
      DestinationServiceJooqImpl(
        database!!,
        featureFlagClient,
        connectionService!!,
        actorDefinitionVersionUpdater,
        metricClient,
        actorPaginationServiceHelper,
      )

    workspaceService =
      WorkspaceServiceJooqImpl(
        database,
        featureFlagClient,
        secretsRepositoryReader,
        secretsRepositoryWriter,
        secretPersistenceConfigService,
        metricClient,
      )

    connectionId = setupTestData()
  }

  private fun setupTestData(): UUID {
    val organizationService: OrganizationService = OrganizationServiceJooqImpl(database)
    organizationService.writeOrganization(MockData.defaultOrganization())

    val workspace = MockData.standardWorkspaces()[0]!!
    val sourceDefinition = MockData.publicSourceDefinition()!!
    val sourceConnection = MockData.sourceConnections()[0]!!
    val actorDefinitionVersion =
      MockData
        .actorDefinitionVersion()!!
        .withActorDefinitionId(sourceDefinition.sourceDefinitionId)
        .withVersionId(sourceDefinition.defaultVersionId)
    val destinationDefinition = MockData.publicDestinationDefinition()!!
    val actorDefinitionVersion2 =
      MockData
        .actorDefinitionVersion()!!
        .withActorDefinitionId(destinationDefinition.destinationDefinitionId)
        .withVersionId(destinationDefinition.defaultVersionId)
    val destinationConnection = MockData.destinationConnections()[0]!!
    // we don't need sync operations in this test suite, zero them out.
    val sync = clone(MockData.standardSyncs()[0]!!).withOperationIds(mutableListOf<UUID?>())

    workspaceService!!.writeStandardWorkspaceNoSecrets(workspace)
    sourceService!!.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion, mutableListOf())
    sourceService!!.writeSourceConnectionNoSecrets(sourceConnection)
    destinationService!!.writeConnectorMetadata(destinationDefinition, actorDefinitionVersion2, mutableListOf())
    destinationService!!.writeDestinationConnectionNoSecrets(destinationConnection)
    connectionService!!.writeStandardSync(sync)

    return sync.connectionId
  }

  @Test
  fun testReadingNonExistingState() {
    Assertions.assertTrue(statePersistence!!.getCurrentState(UUID.randomUUID()).isEmpty)
  }

  @Test
  fun testLegacyReadWrite() {
    val state0 =
      StateWrapper()
        .withStateType(StateType.LEGACY)
        .withLegacyState(deserialize("{\"woot\": \"legacy states is passthrough\"}"))

    // Initial write/read loop, making sure we read what we wrote
    statePersistence!!.updateOrCreateState(connectionId!!, state0)
    val state1 = statePersistence!!.getCurrentState(connectionId!!)

    Assertions.assertTrue(state1.isPresent)
    Assertions.assertEquals(StateType.LEGACY, state1.get().stateType)
    Assertions.assertEquals(state0.legacyState, state1.get().legacyState)

    // Updating a state
    val newStateJson = deserialize("{\"woot\": \"new state\"}")
    val state2 = clone(state1.get()).withLegacyState(newStateJson)
    statePersistence!!.updateOrCreateState(connectionId!!, state2)
    val state3 = statePersistence!!.getCurrentState(connectionId!!)

    Assertions.assertTrue(state3.isPresent)
    Assertions.assertEquals(StateType.LEGACY, state3.get().stateType)
    Assertions.assertEquals(newStateJson, state3.get().legacyState)

    // Deleting a state
    val state4 = clone(state3.get()).withLegacyState(null)
    statePersistence!!.updateOrCreateState(connectionId!!, state4)
    Assertions.assertTrue(statePersistence!!.getCurrentState(connectionId!!).isEmpty)
  }

  @Test
  fun testLegacyMigrationToGlobal() {
    val state0 =
      StateWrapper()
        .withStateType(StateType.LEGACY)
        .withLegacyState(deserialize("{\"woot\": \"legacy states is passthrough\"}"))

    statePersistence!!.updateOrCreateState(connectionId!!, state0)

    val newGlobalState =
      StateWrapper()
        .withStateType(StateType.GLOBAL)
        .withGlobal(
          AirbyteStateMessage()
            .withType(AirbyteStateType.GLOBAL)
            .withGlobal(
              AirbyteGlobalState()
                .withSharedState(deserialize("\"woot\""))
                .withStreamStates(
                  listOf(
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("s1").withNamespace("n2"))
                      .withStreamState(deserialize(STATE_ONE)),
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("s1"))
                      .withStreamState(deserialize(STATE_TWO)),
                  ),
                ),
            ),
        )
    statePersistence!!.updateOrCreateState(connectionId!!, newGlobalState)
    val storedGlobalState = statePersistence!!.getCurrentState(connectionId!!).orElseThrow()
    assertEquals(newGlobalState, storedGlobalState)
  }

  @Test
  fun testLegacyMigrationToStream() {
    val state0 =
      StateWrapper()
        .withStateType(StateType.LEGACY)
        .withLegacyState(deserialize("{\"woot\": \"legacy states is passthrough\"}"))

    statePersistence!!.updateOrCreateState(connectionId!!, state0)

    val newStreamState =
      StateWrapper()
        .withStateType(StateType.STREAM)
        .withStateMessages(
          listOf(
            AirbyteStateMessage()
              .withType(AirbyteStateType.STREAM)
              .withStream(
                AirbyteStreamState()
                  .withStreamDescriptor(StreamDescriptor().withName("s1").withNamespace("n1"))
                  .withStreamState(deserialize(STATE_WITH_NAMESPACE)),
              ),
            AirbyteStateMessage()
              .withType(AirbyteStateType.STREAM)
              .withStream(
                AirbyteStreamState()
                  .withStreamDescriptor(StreamDescriptor().withName("s2"))
                  .withStreamState(deserialize(STREAM_STATE_2)),
              ),
          ),
        )
    statePersistence!!.updateOrCreateState(connectionId!!, newStreamState)
    val storedStreamState = statePersistence!!.getCurrentState(connectionId!!).orElseThrow()
    assertEquals(newStreamState, storedStreamState)
  }

  @Test
  fun testGlobalReadWrite() {
    val state0 =
      StateWrapper()
        .withStateType(StateType.GLOBAL)
        .withGlobal(
          AirbyteStateMessage()
            .withType(AirbyteStateType.GLOBAL)
            .withGlobal(
              AirbyteGlobalState()
                .withSharedState(deserialize(GLOBAL_STATE))
                .withStreamStates(
                  listOf(
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("s1").withNamespace("n2"))
                      .withStreamState(deserialize(STATE_ONE)),
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("s1"))
                      .withStreamState(deserialize(STATE_TWO)),
                  ),
                ),
            ),
        )

    // Initial write/read loop, making sure we read what we wrote
    statePersistence!!.updateOrCreateState(connectionId!!, state0)
    val state1 = statePersistence!!.getCurrentState(connectionId!!)
    Assertions.assertTrue(state1.isPresent)
    assertEquals(state0, state1.get())

    // Updating a state
    val state2 = clone(state1.get())
    state2
      .global
      .global
      .withSharedState(deserialize("\"updated shared state\""))
      .streamStates[1]
      .withStreamState(deserialize("\"updated state2\""))
    statePersistence!!.updateOrCreateState(connectionId!!, state2)
    val state3 = statePersistence!!.getCurrentState(connectionId!!)

    Assertions.assertTrue(state3.isPresent)
    assertEquals(state2, state3.get())

    // Updating a state with name and namespace
    val state4 = clone(state1.get())
    state4
      .global
      .global
      .streamStates[0]
      .withStreamState(deserialize("\"updated state1\""))
    statePersistence!!.updateOrCreateState(connectionId!!, state4)
    val state5 = statePersistence!!.getCurrentState(connectionId!!)

    Assertions.assertTrue(state5.isPresent)
    assertEquals(state4, state5.get())
  }

  @Test
  fun testGlobalPartialReset() {
    val state0 =
      StateWrapper()
        .withStateType(StateType.GLOBAL)
        .withGlobal(
          AirbyteStateMessage()
            .withType(AirbyteStateType.GLOBAL)
            .withGlobal(
              AirbyteGlobalState()
                .withSharedState(deserialize(GLOBAL_STATE))
                .withStreamStates(
                  listOf(
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("s1").withNamespace("n2"))
                      .withStreamState(deserialize(STATE_ONE)),
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("s1"))
                      .withStreamState(deserialize(STATE_TWO)),
                  ),
                ),
            ),
        )

    // Set the initial state
    statePersistence!!.updateOrCreateState(connectionId!!, state0)

    // incomplete reset does not remove the state
    val incompletePartialReset =
      StateWrapper()
        .withStateType(StateType.GLOBAL)
        .withGlobal(
          AirbyteStateMessage()
            .withType(AirbyteStateType.GLOBAL)
            .withGlobal(
              AirbyteGlobalState()
                .withSharedState(deserialize(GLOBAL_STATE))
                .withStreamStates(
                  listOf(
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("s1"))
                      .withStreamState(deserialize(STATE_TWO)),
                  ),
                ),
            ),
        )
    statePersistence!!.updateOrCreateState(connectionId!!, incompletePartialReset)
    val incompletePartialResetResult = statePersistence!!.getCurrentState(connectionId!!).orElseThrow()
    Assertions.assertEquals(state0, incompletePartialResetResult)

    // The good partial reset
    val partialReset =
      StateWrapper()
        .withStateType(StateType.GLOBAL)
        .withGlobal(
          AirbyteStateMessage()
            .withType(AirbyteStateType.GLOBAL)
            .withGlobal(
              AirbyteGlobalState()
                .withSharedState(deserialize(GLOBAL_STATE))
                .withStreamStates(
                  listOf(
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("s1").withNamespace("n2"))
                      .withStreamState(deserialize(STATE_ONE)),
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("s1"))
                      .withStreamState(null),
                  ),
                ),
            ),
        )
    statePersistence!!.updateOrCreateState(connectionId!!, partialReset)
    val partialResetResult = statePersistence!!.getCurrentState(connectionId!!).orElseThrow()

    Assertions.assertEquals(
      partialReset.global.global.sharedState,
      partialResetResult.global.global.sharedState,
    )
    // {"name": "s1"} should have been removed from the stream states
    Assertions.assertEquals(
      1,
      partialResetResult
        .global
        .global
        .streamStates
        .size,
    )
    Assertions.assertEquals(
      partialReset
        .global
        .global
        .streamStates[0],
      partialResetResult
        .global
        .global
        .streamStates[0],
    )
  }

  @Test
  fun testGlobalFullReset() {
    val state0 =
      StateWrapper()
        .withStateType(StateType.GLOBAL)
        .withGlobal(
          AirbyteStateMessage()
            .withType(AirbyteStateType.GLOBAL)
            .withGlobal(
              AirbyteGlobalState()
                .withSharedState(deserialize(GLOBAL_STATE))
                .withStreamStates(
                  listOf(
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("s1").withNamespace("n2"))
                      .withStreamState(deserialize(STATE_ONE)),
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("s1"))
                      .withStreamState(deserialize(STATE_TWO)),
                  ),
                ),
            ),
        )

    val fullReset =
      StateWrapper()
        .withStateType(StateType.GLOBAL)
        .withGlobal(
          AirbyteStateMessage()
            .withType(AirbyteStateType.GLOBAL)
            .withGlobal(
              AirbyteGlobalState()
                .withSharedState(null)
                .withStreamStates(
                  listOf(
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("s1").withNamespace("n2"))
                      .withStreamState(null),
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("s1"))
                      .withStreamState(null),
                  ),
                ),
            ),
        )

    statePersistence!!.updateOrCreateState(connectionId!!, state0)
    statePersistence!!.updateOrCreateState(connectionId!!, fullReset)
    val fullResetResult = statePersistence!!.getCurrentState(connectionId!!)
    Assertions.assertTrue(fullResetResult.isEmpty)
  }

  @Test
  fun testGlobalStateAllowsEmptyNameAndNamespace() {
    val state0 =
      StateWrapper()
        .withStateType(StateType.GLOBAL)
        .withGlobal(
          AirbyteStateMessage()
            .withType(AirbyteStateType.GLOBAL)
            .withGlobal(
              AirbyteGlobalState()
                .withSharedState(deserialize(GLOBAL_STATE))
                .withStreamStates(
                  listOf(
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName(""))
                      .withStreamState(deserialize("\"empty name state\"")),
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("").withNamespace(""))
                      .withStreamState(deserialize("\"empty name and namespace state\"")),
                  ),
                ),
            ),
        )

    statePersistence!!.updateOrCreateState(connectionId!!, state0)
    val state1 = statePersistence!!.getCurrentState(connectionId!!).orElseThrow()
    assertEquals(state0, state1)
  }

  @Test
  fun testStreamReadWrite() {
    val state0 =
      StateWrapper()
        .withStateType(StateType.STREAM)
        .withStateMessages(
          listOf(
            AirbyteStateMessage()
              .withType(AirbyteStateType.STREAM)
              .withStream(
                AirbyteStreamState()
                  .withStreamDescriptor(StreamDescriptor().withName("s1").withNamespace("n1"))
                  .withStreamState(deserialize(STATE_WITH_NAMESPACE)),
              ),
            AirbyteStateMessage()
              .withType(AirbyteStateType.STREAM)
              .withStream(
                AirbyteStreamState()
                  .withStreamDescriptor(StreamDescriptor().withName("s2"))
                  .withStreamState(deserialize(STREAM_STATE_2)),
              ),
          ),
        )

    // Initial write/read loop, making sure we read what we wrote
    statePersistence!!.updateOrCreateState(connectionId!!, state0)
    val state1 = statePersistence!!.getCurrentState(connectionId!!).orElseThrow()
    assertEquals(state0, state1)

    // Updating a state
    val state2 = clone(state1)
    state2
      .stateMessages[1]
      .stream
      .withStreamState(deserialize("\"updated state s2\""))
    statePersistence!!.updateOrCreateState(connectionId!!, state2)
    val state3 = statePersistence!!.getCurrentState(connectionId!!).orElseThrow()
    assertEquals(state2, state3)

    // Updating a state with name and namespace
    val state4 = clone(state1)
    state4
      .stateMessages[0]
      .stream
      .withStreamState(deserialize("\"updated state s1\""))
    statePersistence!!.updateOrCreateState(connectionId!!, state4)
    val state5 = statePersistence!!.getCurrentState(connectionId!!).orElseThrow()
    assertEquals(state4, state5)
  }

  @Test
  fun testStreamPartialUpdates() {
    val state0 =
      StateWrapper()
        .withStateType(StateType.STREAM)
        .withStateMessages(
          listOf(
            AirbyteStateMessage()
              .withType(AirbyteStateType.STREAM)
              .withStream(
                AirbyteStreamState()
                  .withStreamDescriptor(StreamDescriptor().withName("s1").withNamespace("n1"))
                  .withStreamState(deserialize(STATE_WITH_NAMESPACE)),
              ),
            AirbyteStateMessage()
              .withType(AirbyteStateType.STREAM)
              .withStream(
                AirbyteStreamState()
                  .withStreamDescriptor(StreamDescriptor().withName("s2"))
                  .withStreamState(deserialize(STREAM_STATE_2)),
              ),
          ),
        )

    statePersistence!!.updateOrCreateState(connectionId!!, state0)

    // Partial update
    val partialUpdate =
      StateWrapper()
        .withStateType(StateType.STREAM)
        .withStateMessages(
          mutableListOf<AirbyteStateMessage?>(
            AirbyteStateMessage()
              .withType(AirbyteStateType.STREAM)
              .withStream(
                AirbyteStreamState()
                  .withStreamDescriptor(StreamDescriptor().withName("s1").withNamespace("n1"))
                  .withStreamState(deserialize("\"updated\"")),
              ),
          ),
        )
    statePersistence!!.updateOrCreateState(connectionId!!, partialUpdate)
    val partialUpdateResult = statePersistence!!.getCurrentState(connectionId!!).orElseThrow()
    assertEquals(
      StateWrapper()
        .withStateType(StateType.STREAM)
        .withStateMessages(
          listOf<AirbyteStateMessage?>(
            AirbyteStateMessage()
              .withType(AirbyteStateType.STREAM)
              .withStream(
                AirbyteStreamState()
                  .withStreamDescriptor(StreamDescriptor().withName("s1").withNamespace("n1"))
                  .withStreamState(deserialize("\"updated\"")),
              ),
            AirbyteStateMessage()
              .withType(AirbyteStateType.STREAM)
              .withStream(
                AirbyteStreamState()
                  .withStreamDescriptor(StreamDescriptor().withName("s2"))
                  .withStreamState(deserialize(STREAM_STATE_2)),
              ),
          ),
        ),
      partialUpdateResult,
    )

    // Partial Reset
    val partialReset =
      StateWrapper()
        .withStateType(StateType.STREAM)
        .withStateMessages(
          mutableListOf<AirbyteStateMessage?>(
            AirbyteStateMessage()
              .withType(AirbyteStateType.STREAM)
              .withStream(
                AirbyteStreamState()
                  .withStreamDescriptor(StreamDescriptor().withName("s2"))
                  .withStreamState(null),
              ),
          ),
        )
    statePersistence!!.updateOrCreateState(connectionId!!, partialReset)
    val partialResetResult = statePersistence!!.getCurrentState(connectionId!!).orElseThrow()
    assertEquals(
      StateWrapper()
        .withStateType(StateType.STREAM)
        .withStateMessages(
          listOf(
            AirbyteStateMessage()
              .withType(AirbyteStateType.STREAM)
              .withStream(
                AirbyteStreamState()
                  .withStreamDescriptor(StreamDescriptor().withName("s1").withNamespace("n1"))
                  .withStreamState(deserialize("\"updated\"")),
              ),
          ),
        ),
      partialResetResult,
    )
  }

  @Test
  fun testStreamFullReset() {
    val state0 =
      StateWrapper()
        .withStateType(StateType.STREAM)
        .withStateMessages(
          listOf(
            AirbyteStateMessage()
              .withType(AirbyteStateType.STREAM)
              .withStream(
                AirbyteStreamState()
                  .withStreamDescriptor(StreamDescriptor().withName("s1").withNamespace("n1"))
                  .withStreamState(deserialize(STATE_WITH_NAMESPACE)),
              ),
            AirbyteStateMessage()
              .withType(AirbyteStateType.STREAM)
              .withStream(
                AirbyteStreamState()
                  .withStreamDescriptor(StreamDescriptor().withName("s2"))
                  .withStreamState(deserialize(STREAM_STATE_2)),
              ),
          ),
        )

    statePersistence!!.updateOrCreateState(connectionId!!, state0)

    // Partial update
    val fullReset =
      StateWrapper()
        .withStateType(StateType.STREAM)
        .withStateMessages(
          listOf(
            AirbyteStateMessage()
              .withType(AirbyteStateType.STREAM)
              .withStream(
                AirbyteStreamState()
                  .withStreamDescriptor(StreamDescriptor().withName("s1").withNamespace("n1"))
                  .withStreamState(null),
              ),
            AirbyteStateMessage()
              .withType(AirbyteStateType.STREAM)
              .withStream(
                AirbyteStreamState()
                  .withStreamDescriptor(StreamDescriptor().withName("s2"))
                  .withStreamState(null),
              ),
          ),
        )
    statePersistence!!.updateOrCreateState(connectionId!!, fullReset)
    val fullResetResult = statePersistence!!.getCurrentState(connectionId!!)
    Assertions.assertTrue(fullResetResult.isEmpty)
  }

  @Test
  fun testInconsistentTypeUpdates() {
    val streamState =
      StateWrapper()
        .withStateType(StateType.STREAM)
        .withStateMessages(
          listOf(
            AirbyteStateMessage()
              .withType(AirbyteStateType.STREAM)
              .withStream(
                AirbyteStreamState()
                  .withStreamDescriptor(StreamDescriptor().withName("s1").withNamespace("n1"))
                  .withStreamState(deserialize(STATE_WITH_NAMESPACE)),
              ),
            AirbyteStateMessage()
              .withType(AirbyteStateType.STREAM)
              .withStream(
                AirbyteStreamState()
                  .withStreamDescriptor(StreamDescriptor().withName("s2"))
                  .withStreamState(deserialize(STREAM_STATE_2)),
              ),
          ),
        )
    statePersistence!!.updateOrCreateState(connectionId!!, streamState)

    Assertions.assertThrows(
      IllegalStateException::class.java,
    ) {
      val globalState =
        StateWrapper()
          .withStateType(StateType.GLOBAL)
          .withGlobal(
            AirbyteStateMessage()
              .withType(AirbyteStateType.GLOBAL)
              .withGlobal(
                AirbyteGlobalState()
                  .withSharedState(deserialize(GLOBAL_STATE))
                  .withStreamStates(
                    listOf(
                      AirbyteStreamState()
                        .withStreamDescriptor(StreamDescriptor().withName(""))
                        .withStreamState(deserialize("\"empty name state\"")),
                      AirbyteStreamState()
                        .withStreamDescriptor(StreamDescriptor().withName("").withNamespace(""))
                        .withStreamState(deserialize("\"empty name and namespace state\"")),
                    ),
                  ),
              ),
          )
      statePersistence!!.updateOrCreateState(connectionId!!, globalState)
    }

    // We should be guarded against those cases let's make sure we don't make things worse if we're in
    // an inconsistent state
    database!!.transaction<Any?> { ctx: DSLContext? ->
      ctx!!
        .insertInto(DSL.table(STATE))
        .columns(DSL.field("id"), DSL.field("connection_id"), DSL.field("type"), DSL.field(STATE))
        .values(UUID.randomUUID(), connectionId, io.airbyte.db.instance.configs.jooq.generated.enums.StateType.GLOBAL, JSONB.valueOf("{}"))
        .execute()
      null
    }
    Assertions.assertThrows(
      IllegalStateException::class.java,
    ) {
      statePersistence!!.updateOrCreateState(
        connectionId!!,
        streamState,
      )
    }
    Assertions.assertThrows(
      IllegalStateException::class.java,
    ) {
      statePersistence!!.getCurrentState(
        connectionId!!,
      )
    }
  }

  @Test
  fun testEnumCompatibility() {
    Assertions.assertTrue(
      isCompatible<io.airbyte.db.instance.configs.jooq.generated.enums.StateType, StateType>(),
    )
  }

  @Test
  fun testStatePersistenceLegacyWriteConsistency() {
    val jsonState = deserialize("{\"my\": \"state\"}")
    val stateWrapper = StateWrapper().withStateType(StateType.LEGACY).withLegacyState(jsonState)
    statePersistence!!.updateOrCreateState(connectionId!!, stateWrapper)

    // Making sure we still follow the legacy format
    val readStates: MutableList<State?> =
      database!!.transaction<MutableList<State?>?> { ctx: DSLContext? ->
        ctx!!
          .selectFrom(STATE)
          .where(
            DSL
              .field("connection_id")
              .eq(connectionId),
          ).fetch()
          .map { r: org.jooq.Record? ->
            deserialize(
              r!!
                .get(
                  DSL.field(
                    STATE,
                    JSONB::class.java,
                  ),
                )!!
                .data(),
              State::class.java,
            )
          }.stream()
          .toList()
      }!!
    Assertions.assertEquals(1, readStates.size)

    Assertions.assertEquals(readStates[0]!!.state, stateWrapper.legacyState)
  }

  @Test
  fun testBulkDeletePerStream() {
    val perStreamToModify =
      StateWrapper()
        .withStateType(StateType.STREAM)
        .withStateMessages(
          listOf(
            AirbyteStateMessage()
              .withType(AirbyteStateType.STREAM)
              .withStream(
                AirbyteStreamState()
                  .withStreamDescriptor(StreamDescriptor().withName("del-1").withNamespace("del-n1"))
                  .withStreamState(deserialize("")),
              ),
            AirbyteStateMessage()
              .withType(AirbyteStateType.STREAM)
              .withStream(
                AirbyteStreamState()
                  .withStreamDescriptor(StreamDescriptor().withName("keep-1"))
                  .withStreamState(deserialize("")),
              ),
            AirbyteStateMessage()
              .withType(AirbyteStateType.STREAM)
              .withStream(
                AirbyteStreamState()
                  .withStreamDescriptor(StreamDescriptor().withName("del-2"))
                  .withStreamState(deserialize("")),
              ),
            AirbyteStateMessage()
              .withType(AirbyteStateType.STREAM)
              .withStream(
                AirbyteStreamState()
                  .withStreamDescriptor(StreamDescriptor().withName("del-1").withNamespace("del-n2"))
                  .withStreamState(deserialize("")),
              ),
            AirbyteStateMessage()
              .withType(AirbyteStateType.STREAM)
              .withStream(
                AirbyteStreamState()
                  .withStreamDescriptor(StreamDescriptor().withName("keep-1").withNamespace("keep-n1"))
                  .withStreamState(deserialize("")),
              ),
          ),
        )
    statePersistence!!.updateOrCreateState(connectionId!!, clone(perStreamToModify))

    val toDelete =
      setOf(
        io.airbyte.config
          .StreamDescriptor()
          .withName("del-1")
          .withNamespace("del-n1"),
        io.airbyte.config
          .StreamDescriptor()
          .withName("del-2"),
        io.airbyte.config
          .StreamDescriptor()
          .withName("del-1")
          .withNamespace("del-n2"),
      )
    statePersistence!!.bulkDelete(connectionId!!, toDelete)

    val curr = statePersistence!!.getCurrentState(connectionId!!)
    val exp =
      StateWrapper()
        .withStateType(StateType.STREAM)
        .withStateMessages(
          listOf(
            AirbyteStateMessage()
              .withType(AirbyteStateType.STREAM)
              .withStream(
                AirbyteStreamState()
                  .withStreamDescriptor(StreamDescriptor().withName("keep-1"))
                  .withStreamState(deserialize("")),
              ),
            AirbyteStateMessage()
              .withType(AirbyteStateType.STREAM)
              .withStream(
                AirbyteStreamState()
                  .withStreamDescriptor(StreamDescriptor().withName("keep-1").withNamespace("keep-n1"))
                  .withStreamState(deserialize("")),
              ),
          ),
        )
    assertEquals(exp, curr.get())
  }

  @Test
  fun testBulkDeleteGlobal() {
    val globalToModify =
      StateWrapper()
        .withStateType(StateType.GLOBAL)
        .withGlobal(
          AirbyteStateMessage()
            .withType(AirbyteStateType.GLOBAL)
            .withGlobal(
              AirbyteGlobalState()
                .withSharedState(deserialize("\"woot\""))
                .withStreamStates(
                  listOf(
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("del-1").withNamespace("del-n1"))
                      .withStreamState(deserialize("")),
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("keep-1"))
                      .withStreamState(deserialize("")),
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("del-2"))
                      .withStreamState(deserialize("")),
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("del-1").withNamespace("del-n2"))
                      .withStreamState(deserialize("")),
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("keep-1").withNamespace("keep-n1"))
                      .withStreamState(deserialize("")),
                  ),
                ),
            ),
        )

    statePersistence!!.updateOrCreateState(connectionId!!, clone(globalToModify))

    val toDelete =
      setOf(
        io.airbyte.config
          .StreamDescriptor()
          .withName("del-1")
          .withNamespace("del-n1"),
        io.airbyte.config
          .StreamDescriptor()
          .withName("del-2"),
        io.airbyte.config
          .StreamDescriptor()
          .withName("del-1")
          .withNamespace("del-n2"),
      )
    statePersistence!!.bulkDelete(connectionId!!, toDelete)

    val curr = statePersistence!!.getCurrentState(connectionId!!)
    val exp =
      StateWrapper()
        .withStateType(StateType.GLOBAL)
        .withGlobal(
          AirbyteStateMessage()
            .withType(AirbyteStateType.GLOBAL)
            .withGlobal(
              AirbyteGlobalState()
                .withSharedState(deserialize("\"woot\""))
                .withStreamStates(
                  listOf(
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("keep-1"))
                      .withStreamState(deserialize("")),
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("keep-1").withNamespace("keep-n1"))
                      .withStreamState(deserialize("")),
                  ),
                ),
            ),
        )
    assertEquals(exp, curr.get())
  }

  @Test
  fun testBulkDeleteGlobalAllStreams() {
    val globalToModify =
      StateWrapper()
        .withStateType(StateType.GLOBAL)
        .withGlobal(
          AirbyteStateMessage()
            .withType(AirbyteStateType.GLOBAL)
            .withGlobal(
              AirbyteGlobalState()
                .withSharedState(deserialize("\"woot\""))
                .withStreamStates(
                  listOf(
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("del-1").withNamespace("del-n1"))
                      .withStreamState(deserialize("")),
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("del-2"))
                      .withStreamState(deserialize("")),
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("del-1").withNamespace("del-n2"))
                      .withStreamState(deserialize("")),
                  ),
                ),
            ),
        )

    statePersistence!!.updateOrCreateState(connectionId!!, clone(globalToModify))

    val toDelete =
      setOf(
        io.airbyte.config
          .StreamDescriptor()
          .withName("del-1")
          .withNamespace("del-n1"),
        io.airbyte.config
          .StreamDescriptor()
          .withName("del-2"),
        io.airbyte.config
          .StreamDescriptor()
          .withName("del-1")
          .withNamespace("del-n2"),
      )
    statePersistence!!.bulkDelete(connectionId!!, toDelete)

    val curr = statePersistence!!.getCurrentState(connectionId!!)

    Assertions.assertTrue(curr.isEmpty)
  }

  @Test
  fun testBulkDeleteCorrectConnection() {
    val globalToModify =
      StateWrapper()
        .withStateType(StateType.GLOBAL)
        .withGlobal(
          AirbyteStateMessage()
            .withType(AirbyteStateType.GLOBAL)
            .withGlobal(
              AirbyteGlobalState()
                .withSharedState(deserialize("\"woot\""))
                .withStreamStates(
                  listOf(
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("del-1").withNamespace("del-n1"))
                      .withStreamState(deserialize("")),
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("keep-1"))
                      .withStreamState(deserialize("")),
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("del-2"))
                      .withStreamState(deserialize("")),
                  ),
                ),
            ),
        )

    statePersistence!!.updateOrCreateState(connectionId!!, clone(globalToModify))

    val secondConn = setupSecondConnection()
    statePersistence!!.updateOrCreateState(secondConn, clone(globalToModify))

    val toDelete =
      setOf(
        io.airbyte.config
          .StreamDescriptor()
          .withName("del-1")
          .withNamespace("del-n1"),
        io.airbyte.config
          .StreamDescriptor()
          .withName("del-2"),
      )
    statePersistence!!.bulkDelete(connectionId!!, toDelete)

    val curr = statePersistence!!.getCurrentState(connectionId!!)
    val exp =
      StateWrapper()
        .withStateType(StateType.GLOBAL)
        .withGlobal(
          AirbyteStateMessage()
            .withType(AirbyteStateType.GLOBAL)
            .withGlobal(
              AirbyteGlobalState()
                .withSharedState(deserialize("\"woot\""))
                .withStreamStates(
                  mutableListOf<AirbyteStreamState?>(
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("keep-1"))
                      .withStreamState(deserialize("")),
                  ),
                ),
            ),
        )
    assertEquals(exp, curr.get())

    val untouched = statePersistence!!.getCurrentState(secondConn)
    assertEquals(globalToModify, untouched.get())
  }

  @Test
  fun testBulkDeleteNoStreamsNoDelete() {
    val globalToModify =
      StateWrapper()
        .withStateType(StateType.GLOBAL)
        .withGlobal(
          AirbyteStateMessage()
            .withType(AirbyteStateType.GLOBAL)
            .withGlobal(
              AirbyteGlobalState()
                .withSharedState(deserialize("\"woot\""))
                .withStreamStates(
                  listOf(
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("del-1").withNamespace("del-n1"))
                      .withStreamState(deserialize("")),
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("keep-1"))
                      .withStreamState(deserialize("")),
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("del-2"))
                      .withStreamState(deserialize("")),
                  ),
                ),
            ),
        )

    statePersistence!!.updateOrCreateState(connectionId!!, clone(globalToModify))

    val secondConn = setupSecondConnection()
    statePersistence!!.updateOrCreateState(secondConn, clone(globalToModify))

    statePersistence!!.bulkDelete(connectionId!!, mutableSetOf())

    val curr = statePersistence!!.getCurrentState(connectionId!!)
    assertEquals(clone(globalToModify), curr.get())

    val untouched = statePersistence!!.getCurrentState(secondConn)
    assertEquals(globalToModify, untouched.get())
  }

  @Test
  fun testEraseGlobalState() {
    val connectionState =
      StateWrapper()
        .withStateType(StateType.GLOBAL)
        .withGlobal(
          AirbyteStateMessage()
            .withType(AirbyteStateType.GLOBAL)
            .withGlobal(
              AirbyteGlobalState()
                .withSharedState(deserialize("0"))
                .withStreamStates(
                  listOf(
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("user"))
                      .withStreamState(deserialize("99")),
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("order").withNamespace("business"))
                      .withStreamState(deserialize("99")),
                  ),
                ),
            ),
        )
    val otherState =
      StateWrapper()
        .withStateType(StateType.GLOBAL)
        .withGlobal(
          AirbyteStateMessage()
            .withType(AirbyteStateType.GLOBAL)
            .withGlobal(
              AirbyteGlobalState()
                .withSharedState(deserialize("\"2024-01-01\""))
                .withStreamStates(
                  listOf(
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("shop"))
                      .withStreamState(deserialize("\"test\"")),
                    AirbyteStreamState()
                      .withStreamDescriptor(StreamDescriptor().withName("seller"))
                      .withStreamState(deserialize("\"joe\"")),
                  ),
                ),
            ),
        )

    val otherConnectionId = setupSecondConnection()
    statePersistence!!.updateOrCreateState(connectionId!!, connectionState)
    Assertions.assertTrue(statePersistence!!.getCurrentState(connectionId!!).isPresent, "The main connection state is not present in database")
    statePersistence!!.updateOrCreateState(otherConnectionId, otherState)
    Assertions.assertTrue(
      statePersistence!!.getCurrentState(otherConnectionId).isPresent,
      "The other connection state is not present in database",
    )
    assertEquals(otherState, statePersistence!!.getCurrentState(otherConnectionId).get(), "The other connection state is incorrect")

    statePersistence!!.eraseState(connectionId!!)
    Assertions.assertTrue(statePersistence!!.getCurrentState(connectionId!!).isEmpty, "The main connection state is still present")
    statePersistence!!.updateOrCreateState(otherConnectionId, otherState)
    Assertions.assertTrue(
      statePersistence!!.getCurrentState(otherConnectionId).isPresent,
      "The other connection state is no longer present in database",
    )
    assertEquals(otherState, statePersistence!!.getCurrentState(otherConnectionId).get(), "the other connection state has been altered")
  }

  @Test
  fun testEraseStreamState() {
    val connectionState =
      StateWrapper()
        .withStateType(StateType.STREAM)
        .withStateMessages(
          listOf(
            AirbyteStateMessage()
              .withType(AirbyteStateType.STREAM)
              .withStream(
                AirbyteStreamState()
                  .withStreamDescriptor(StreamDescriptor().withName("user"))
                  .withStreamState(deserialize("0")),
              ),
            AirbyteStateMessage()
              .withType(AirbyteStateType.STREAM)
              .withStream(
                AirbyteStreamState()
                  .withStreamDescriptor(StreamDescriptor().withName("order").withNamespace("business"))
                  .withStreamState(deserialize("10")),
              ),
          ),
        )
    val otherState =
      StateWrapper()
        .withStateType(StateType.STREAM)
        .withStateMessages(
          listOf(
            AirbyteStateMessage()
              .withType(AirbyteStateType.STREAM)
              .withStream(
                AirbyteStreamState()
                  .withStreamDescriptor(StreamDescriptor().withName("shop"))
                  .withStreamState(deserialize("\"test\"")),
              ),
            AirbyteStateMessage()
              .withType(AirbyteStateType.STREAM)
              .withStream(
                AirbyteStreamState()
                  .withStreamDescriptor(StreamDescriptor().withName("seller"))
                  .withStreamState(deserialize("\"joe\"")),
              ),
          ),
        )
    val otherConnectionId = setupSecondConnection()
    statePersistence!!.updateOrCreateState(connectionId!!, connectionState)
    Assertions.assertTrue(statePersistence!!.getCurrentState(connectionId!!).isPresent, "The main connection state is not present in database")
    statePersistence!!.updateOrCreateState(otherConnectionId, otherState)
    Assertions.assertTrue(
      statePersistence!!.getCurrentState(otherConnectionId).isPresent,
      "The other connection state is not present in database",
    )
    assertEquals(otherState, statePersistence!!.getCurrentState(otherConnectionId).get(), "The other connection state is incorrect")

    statePersistence!!.eraseState(connectionId!!)
    Assertions.assertTrue(statePersistence!!.getCurrentState(connectionId!!).isEmpty, "The main connection state is still present")
    statePersistence!!.updateOrCreateState(otherConnectionId, otherState)
    Assertions.assertTrue(
      statePersistence!!.getCurrentState(otherConnectionId).isPresent,
      "The other connection state is no longer present in database",
    )
    assertEquals(otherState, statePersistence!!.getCurrentState(otherConnectionId).get(), "the other connection state has been altered")
  }

  private fun setupSecondConnection(): UUID {
    val workspace = MockData.standardWorkspaces()[0]!!
    val sourceDefinition = MockData.publicSourceDefinition()!!
    val sourceConnection = MockData.sourceConnections()[0]!!
    val actorDefinitionVersion =
      MockData
        .actorDefinitionVersion()!!
        .withActorDefinitionId(sourceDefinition.sourceDefinitionId)
        .withVersionId(sourceDefinition.defaultVersionId)
    val destinationDefinition = MockData.grantableDestinationDefinition1()!!
    val actorDefinitionVersion2 =
      MockData
        .actorDefinitionVersion()!!
        .withActorDefinitionId(destinationDefinition.destinationDefinitionId)
        .withVersionId(destinationDefinition.defaultVersionId)
    val destinationConnection = MockData.destinationConnections()[1]!!
    // we don't need sync operations in this test suite, zero them out.
    val sync = clone(MockData.standardSyncs()[1]!!).withOperationIds(mutableListOf<UUID?>())

    workspaceService!!.writeStandardWorkspaceNoSecrets(workspace)
    sourceService!!.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion, mutableListOf())
    sourceService!!.writeSourceConnectionNoSecrets(sourceConnection)
    destinationService!!.writeConnectorMetadata(destinationDefinition, actorDefinitionVersion2, mutableListOf())
    destinationService!!.writeDestinationConnectionNoSecrets(destinationConnection)
    connectionService!!.writeStandardSync(sync)
    return sync.connectionId
  }

  private fun clone(state: StateWrapper): StateWrapper =
    when (state.stateType) {
      StateType.LEGACY ->
        StateWrapper()
          .withLegacyState(deserialize(serialize(state.legacyState)))
          .withStateType(state.stateType)

      StateType.STREAM ->
        StateWrapper()
          .withStateMessages(
            state
              .stateMessages
              .stream()
              .map { msg: AirbyteStateMessage? ->
                deserialize(
                  serialize(msg),
                  AirbyteStateMessage::class.java,
                )
              }.toList(),
          ).withStateType(state.stateType)

      StateType.GLOBAL ->
        StateWrapper()
          .withGlobal(
            deserialize(
              serialize(state.global),
              AirbyteStateMessage::class.java,
            ),
          ).withStateType(state.stateType)
    }

  private fun assertEquals(
    lhs: StateWrapper?,
    rhs: StateWrapper?,
  ) {
    Assertions.assertEquals(serialize(lhs), serialize(rhs))
  }

  private fun assertEquals(
    lhs: StateWrapper?,
    rhs: StateWrapper?,
    message: String?,
  ) {
    Assertions.assertEquals(serialize(lhs), serialize(rhs), message)
  }

  companion object {
    private const val STATE_ONE = "\"state1\""
    private const val STATE_TWO = "\"state2\""
    private const val STATE_WITH_NAMESPACE = "\"state s1.n1\""
    private const val STREAM_STATE_2 = "\"state s2\""
    private const val GLOBAL_STATE = "\"my global state\""
    private const val STATE = "state"
    private val DATAPLANE_GROUP_ID: UUID = UUID.randomUUID()
  }
}
