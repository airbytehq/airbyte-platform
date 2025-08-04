/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence

import io.airbyte.commons.enums.isCompatible
import io.airbyte.commons.json.Jsons
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
import io.airbyte.db.ContextQueryFunction
import io.airbyte.db.init.DatabaseInitializationException
import io.airbyte.featureflag.TestClient
import io.airbyte.metrics.MetricClient
import io.airbyte.protocol.models.v0.AirbyteGlobalState
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage.AirbyteStateType
import io.airbyte.protocol.models.v0.AirbyteStreamState
import io.airbyte.protocol.models.v0.StreamDescriptor
import io.airbyte.test.utils.BaseConfigDatabaseTest
import io.airbyte.validation.json.JsonValidationException
import org.jooq.DSLContext
import org.jooq.JSONB
import org.jooq.Record
import org.jooq.impl.DSL
import org.junit.Assert
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import org.mockito.Mockito
import java.io.IOException
import java.sql.SQLException
import java.util.Arrays
import java.util.List
import java.util.Set
import java.util.UUID

internal class StatePersistenceTest : BaseConfigDatabaseTest() {
  private var statePersistence: StatePersistence? = null
  private var connectionId: UUID? = null
  private var connectionService: ConnectionService? = null
  private var sourceService: SourceService? = null
  private var destinationService: DestinationService? = null
  private var workspaceService: WorkspaceService? = null

  @BeforeEach
  @Throws(DatabaseInitializationException::class, IOException::class, JsonValidationException::class, SQLException::class)
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
        .withOrganizationId(MockData.defaultOrganization()!!.getOrganizationId())
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

  @Throws(JsonValidationException::class, IOException::class)
  private fun setupTestData(): UUID {
    val organizationService: OrganizationService = OrganizationServiceJooqImpl(database)
    organizationService.writeOrganization(MockData.defaultOrganization())

    val workspace = MockData.standardWorkspaces().get(0)!!
    val sourceDefinition = MockData.publicSourceDefinition()!!
    val sourceConnection = MockData.sourceConnections().get(0)!!
    val actorDefinitionVersion =
      MockData
        .actorDefinitionVersion()!!
        .withActorDefinitionId(sourceDefinition.getSourceDefinitionId())
        .withVersionId(sourceDefinition.getDefaultVersionId())
    val destinationDefinition = MockData.publicDestinationDefinition()!!
    val actorDefinitionVersion2 =
      MockData
        .actorDefinitionVersion()!!
        .withActorDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .withVersionId(destinationDefinition.getDefaultVersionId())
    val destinationConnection = MockData.destinationConnections().get(0)!!
    // we don't need sync operations in this test suite, zero them out.
    val sync = clone(MockData.standardSyncs().get(0)!!).withOperationIds(mutableListOf<UUID?>())

    workspaceService!!.writeStandardWorkspaceNoSecrets(workspace)
    sourceService!!.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion, mutableListOf())
    sourceService!!.writeSourceConnectionNoSecrets(sourceConnection)
    destinationService!!.writeConnectorMetadata(destinationDefinition, actorDefinitionVersion2, mutableListOf())
    destinationService!!.writeDestinationConnectionNoSecrets(destinationConnection)
    connectionService!!.writeStandardSync(sync)

    return sync.getConnectionId()
  }

  @Test
  @Throws(IOException::class)
  fun testReadingNonExistingState() {
    Assertions.assertTrue(statePersistence!!.getCurrentState(UUID.randomUUID()).isEmpty())
  }

  @Test
  @Throws(IOException::class)
  fun testLegacyReadWrite() {
    val state0 =
      StateWrapper()
        .withStateType(StateType.LEGACY)
        .withLegacyState(deserialize("{\"woot\": \"legacy states is passthrough\"}"))

    // Initial write/read loop, making sure we read what we wrote
    statePersistence!!.updateOrCreateState(connectionId!!, state0)
    val state1 = statePersistence!!.getCurrentState(connectionId!!)

    Assertions.assertTrue(state1.isPresent())
    Assertions.assertEquals(StateType.LEGACY, state1.get().getStateType())
    Assertions.assertEquals(state0.getLegacyState(), state1.get().getLegacyState())

    // Updating a state
    val newStateJson = deserialize("{\"woot\": \"new state\"}")
    val state2 = clone(state1.get()).withLegacyState(newStateJson)
    statePersistence!!.updateOrCreateState(connectionId!!, state2)
    val state3 = statePersistence!!.getCurrentState(connectionId!!)

    Assertions.assertTrue(state3.isPresent())
    Assertions.assertEquals(StateType.LEGACY, state3.get().getStateType())
    Assertions.assertEquals(newStateJson, state3.get().getLegacyState())

    // Deleting a state
    val state4 = clone(state3.get()).withLegacyState(null)
    statePersistence!!.updateOrCreateState(connectionId!!, state4)
    Assertions.assertTrue(statePersistence!!.getCurrentState(connectionId!!).isEmpty())
  }

  @Test
  @Throws(IOException::class)
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
                  Arrays.asList(
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
  @Throws(IOException::class)
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
          Arrays.asList(
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
  @Throws(IOException::class)
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
                  Arrays.asList(
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
    Assertions.assertTrue(state1.isPresent())
    assertEquals(state0, state1.get())

    // Updating a state
    val state2 = clone(state1.get())
    state2
      .getGlobal()
      .getGlobal()
      .withSharedState(deserialize("\"updated shared state\""))
      .getStreamStates()
      .get(1)
      .withStreamState(deserialize("\"updated state2\""))
    statePersistence!!.updateOrCreateState(connectionId!!, state2)
    val state3 = statePersistence!!.getCurrentState(connectionId!!)

    Assertions.assertTrue(state3.isPresent())
    assertEquals(state2, state3.get())

    // Updating a state with name and namespace
    val state4 = clone(state1.get())
    state4
      .getGlobal()
      .getGlobal()
      .getStreamStates()
      .get(0)
      .withStreamState(deserialize("\"updated state1\""))
    statePersistence!!.updateOrCreateState(connectionId!!, state4)
    val state5 = statePersistence!!.getCurrentState(connectionId!!)

    Assertions.assertTrue(state5.isPresent())
    assertEquals(state4, state5.get())
  }

  @Test
  @Throws(IOException::class)
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
                  Arrays.asList(
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
                  List.of<AirbyteStreamState?>(
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
                  Arrays.asList(
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
      partialReset.getGlobal().getGlobal().getSharedState(),
      partialResetResult.getGlobal().getGlobal().getSharedState(),
    )
    // {"name": "s1"} should have been removed from the stream states
    Assertions.assertEquals(
      1,
      partialResetResult
        .getGlobal()
        .getGlobal()
        .getStreamStates()
        .size,
    )
    Assertions.assertEquals(
      partialReset
        .getGlobal()
        .getGlobal()
        .getStreamStates()
        .get(0),
      partialResetResult
        .getGlobal()
        .getGlobal()
        .getStreamStates()
        .get(0),
    )
  }

  @Test
  @Throws(IOException::class)
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
                  Arrays.asList(
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
                  Arrays.asList(
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
    Assertions.assertTrue(fullResetResult.isEmpty())
  }

  @Test
  @Throws(IOException::class)
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
                  Arrays.asList(
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
  @Throws(IOException::class)
  fun testStreamReadWrite() {
    val state0 =
      StateWrapper()
        .withStateType(StateType.STREAM)
        .withStateMessages(
          Arrays.asList(
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
      .getStateMessages()
      .get(1)
      .getStream()
      .withStreamState(deserialize("\"updated state s2\""))
    statePersistence!!.updateOrCreateState(connectionId!!, state2)
    val state3 = statePersistence!!.getCurrentState(connectionId!!).orElseThrow()
    assertEquals(state2, state3)

    // Updating a state with name and namespace
    val state4 = clone(state1)
    state4
      .getStateMessages()
      .get(0)
      .getStream()
      .withStreamState(deserialize("\"updated state s1\""))
    statePersistence!!.updateOrCreateState(connectionId!!, state4)
    val state5 = statePersistence!!.getCurrentState(connectionId!!).orElseThrow()
    assertEquals(state4, state5)
  }

  @Test
  @Throws(IOException::class)
  fun testStreamPartialUpdates() {
    val state0 =
      StateWrapper()
        .withStateType(StateType.STREAM)
        .withStateMessages(
          Arrays.asList(
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
          Arrays.asList<AirbyteStateMessage?>(
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
          List.of<AirbyteStateMessage?>(
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
  @Throws(IOException::class)
  fun testStreamFullReset() {
    val state0 =
      StateWrapper()
        .withStateType(StateType.STREAM)
        .withStateMessages(
          Arrays.asList(
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
          Arrays.asList(
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
    Assertions.assertTrue(fullResetResult.isEmpty())
  }

  @Test
  @Throws(IOException::class, SQLException::class)
  fun testInconsistentTypeUpdates() {
    val streamState =
      StateWrapper()
        .withStateType(StateType.STREAM)
        .withStateMessages(
          Arrays.asList(
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
      Executable {
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
                      Arrays.asList(
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
      },
    )

    // We should be guarded against those cases let's make sure we don't make things worse if we're in
    // an inconsistent state
    database!!.transaction<Any?>(
      ContextQueryFunction { ctx: DSLContext? ->
        ctx!!
          .insertInto(DSL.table(STATE))
          .columns(DSL.field("id"), DSL.field("connection_id"), DSL.field("type"), DSL.field(STATE))
          .values(UUID.randomUUID(), connectionId, io.airbyte.db.instance.configs.jooq.generated.enums.StateType.GLOBAL, JSONB.valueOf("{}"))
          .execute()
        null
      },
    )
    Assertions.assertThrows(
      IllegalStateException::class.java,
      Executable {
        statePersistence!!.updateOrCreateState(
          connectionId!!,
          streamState,
        )
      },
    )
    Assertions.assertThrows(
      IllegalStateException::class.java,
      Executable {
        statePersistence!!.getCurrentState(
          connectionId!!,
        )
      },
    )
  }

  @Test
  fun testEnumCompatibility() {
    Assertions.assertTrue(
      isCompatible<io.airbyte.db.instance.configs.jooq.generated.enums.StateType, io.airbyte.config.StateType>(),
    )
  }

  @Test
  @Throws(IOException::class, SQLException::class)
  fun testStatePersistenceLegacyWriteConsistency() {
    val jsonState = deserialize("{\"my\": \"state\"}")
    val stateWrapper = StateWrapper().withStateType(StateType.LEGACY).withLegacyState(jsonState)
    statePersistence!!.updateOrCreateState(connectionId!!, stateWrapper)

    // Making sure we still follow the legacy format
    val readStates: MutableList<State?> =
      database!!.transaction<MutableList<State?>?>(
        ContextQueryFunction { ctx: org.jooq.DSLContext? ->
          ctx!!
            .selectFrom(StatePersistenceTest.Companion.STATE)
            .where(
              org.jooq.impl.DSL
                .field("connection_id")
                .eq(connectionId),
            ).fetch()
            .map(
              org.jooq.RecordMapper { r: org.jooq.Record? ->
                io.airbyte.commons.json.Jsons.deserialize(
                  r!!
                    .get(
                      org.jooq.impl.DSL.field(
                        StatePersistenceTest.Companion.STATE,
                        org.jooq.JSONB::class.java,
                      ),
                    )!!
                    .data(),
                  io.airbyte.config.State::class.java,
                )
              },
            ).stream()
            .toList()
        },
      )!!
    Assertions.assertEquals(1, readStates.size)

    Assertions.assertEquals(readStates.get(0)!!.getState(), stateWrapper.getLegacyState())
  }

  @Test
  @Throws(IOException::class)
  fun testBulkDeletePerStream() {
    val perStreamToModify =
      StateWrapper()
        .withStateType(StateType.STREAM)
        .withStateMessages(
          Arrays.asList(
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
      Set.of<io.airbyte.config.StreamDescriptor?>(
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
          Arrays.asList(
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
  @Throws(IOException::class)
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
                  Arrays.asList(
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
      Set.of<io.airbyte.config.StreamDescriptor?>(
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
                  Arrays.asList(
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
  @Throws(IOException::class)
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
                  Arrays.asList(
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
      Set.of<io.airbyte.config.StreamDescriptor?>(
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

    Assert.assertTrue(curr.isEmpty())
  }

  @Test
  @Throws(IOException::class, JsonValidationException::class)
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
                  Arrays.asList(
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
      Set.of<io.airbyte.config.StreamDescriptor?>(
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
  @Throws(IOException::class, JsonValidationException::class)
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
                  Arrays.asList(
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
  @Throws(IOException::class, JsonValidationException::class)
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
                  List.of<AirbyteStreamState?>(
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
                  List.of<AirbyteStreamState?>(
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
    Assertions.assertTrue(statePersistence!!.getCurrentState(connectionId!!).isPresent(), "The main connection state is not present in database")
    statePersistence!!.updateOrCreateState(otherConnectionId, otherState)
    Assertions.assertTrue(
      statePersistence!!.getCurrentState(otherConnectionId).isPresent(),
      "The other connection state is not present in database",
    )
    assertEquals(otherState, statePersistence!!.getCurrentState(otherConnectionId).get(), "The other connection state is incorrect")

    statePersistence!!.eraseState(connectionId!!)
    Assertions.assertTrue(statePersistence!!.getCurrentState(connectionId!!).isEmpty(), "The main connection state is still present")
    statePersistence!!.updateOrCreateState(otherConnectionId, otherState)
    Assertions.assertTrue(
      statePersistence!!.getCurrentState(otherConnectionId).isPresent(),
      "The other connection state is no longer present in database",
    )
    assertEquals(otherState, statePersistence!!.getCurrentState(otherConnectionId).get(), "the other connection state has been altered")
  }

  @Test
  @Throws(JsonValidationException::class, IOException::class)
  fun testEraseStreamState() {
    val connectionState =
      StateWrapper()
        .withStateType(StateType.STREAM)
        .withStateMessages(
          List.of<AirbyteStateMessage?>(
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
          List.of<AirbyteStateMessage?>(
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
    Assertions.assertTrue(statePersistence!!.getCurrentState(connectionId!!).isPresent(), "The main connection state is not present in database")
    statePersistence!!.updateOrCreateState(otherConnectionId, otherState)
    Assertions.assertTrue(
      statePersistence!!.getCurrentState(otherConnectionId).isPresent(),
      "The other connection state is not present in database",
    )
    assertEquals(otherState, statePersistence!!.getCurrentState(otherConnectionId).get(), "The other connection state is incorrect")

    statePersistence!!.eraseState(connectionId!!)
    Assertions.assertTrue(statePersistence!!.getCurrentState(connectionId!!).isEmpty(), "The main connection state is still present")
    statePersistence!!.updateOrCreateState(otherConnectionId, otherState)
    Assertions.assertTrue(
      statePersistence!!.getCurrentState(otherConnectionId).isPresent(),
      "The other connection state is no longer present in database",
    )
    assertEquals(otherState, statePersistence!!.getCurrentState(otherConnectionId).get(), "the other connection state has been altered")
  }

  @Throws(JsonValidationException::class, IOException::class)
  private fun setupSecondConnection(): UUID {
    val workspace = MockData.standardWorkspaces().get(0)!!
    val sourceDefinition = MockData.publicSourceDefinition()!!
    val sourceConnection = MockData.sourceConnections().get(0)!!
    val actorDefinitionVersion =
      MockData
        .actorDefinitionVersion()!!
        .withActorDefinitionId(sourceDefinition.getSourceDefinitionId())
        .withVersionId(sourceDefinition.getDefaultVersionId())
    val destinationDefinition = MockData.grantableDestinationDefinition1()!!
    val actorDefinitionVersion2 =
      MockData
        .actorDefinitionVersion()!!
        .withActorDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .withVersionId(destinationDefinition.getDefaultVersionId())
    val destinationConnection = MockData.destinationConnections().get(1)!!
    // we don't need sync operations in this test suite, zero them out.
    val sync = clone(MockData.standardSyncs().get(1)!!).withOperationIds(mutableListOf<UUID?>())

    workspaceService!!.writeStandardWorkspaceNoSecrets(workspace)
    sourceService!!.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion, mutableListOf())
    sourceService!!.writeSourceConnectionNoSecrets(sourceConnection)
    destinationService!!.writeConnectorMetadata(destinationDefinition, actorDefinitionVersion2, mutableListOf())
    destinationService!!.writeDestinationConnectionNoSecrets(destinationConnection)
    connectionService!!.writeStandardSync(sync)
    return sync.getConnectionId()
  }

  private fun clone(state: StateWrapper): StateWrapper =
    when (state.getStateType()) {
      StateType.LEGACY ->
        StateWrapper()
          .withLegacyState(deserialize(serialize(state.getLegacyState())))
          .withStateType(state.getStateType())

      StateType.STREAM ->
        StateWrapper()
          .withStateMessages(
            state
              .getStateMessages()
              .stream()
              .map { msg: AirbyteStateMessage? ->
                Jsons.deserialize(
                  serialize(msg),
                  AirbyteStateMessage::class.java,
                )
              }.toList(),
          ).withStateType(state.getStateType())

      StateType.GLOBAL ->
        StateWrapper()
          .withGlobal(
            Jsons.deserialize(
              serialize(state.getGlobal()),
              AirbyteStateMessage::class.java,
            ),
          ).withStateType(state.getStateType())
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
