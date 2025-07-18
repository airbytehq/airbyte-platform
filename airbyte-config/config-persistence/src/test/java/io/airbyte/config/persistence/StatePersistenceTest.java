/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence;

import static io.airbyte.commons.ConstantsKt.DEFAULT_ORGANIZATION_ID;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.DataplaneGroup;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.SourceConnection;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSourceDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardWorkspace;
import io.airbyte.config.State;
import io.airbyte.config.StateType;
import io.airbyte.config.StateWrapper;
import io.airbyte.config.secrets.SecretsRepositoryReader;
import io.airbyte.config.secrets.SecretsRepositoryWriter;
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.ConnectionTimelineEventService;
import io.airbyte.data.services.DataplaneGroupService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.OrganizationService;
import io.airbyte.data.services.ScopedConfigurationService;
import io.airbyte.data.services.SecretPersistenceConfigService;
import io.airbyte.data.services.SourceService;
import io.airbyte.data.services.WorkspaceService;
import io.airbyte.data.services.impls.data.DataplaneGroupServiceTestJooqImpl;
import io.airbyte.data.services.impls.jooq.ActorDefinitionServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.ConnectionServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.DestinationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.OrganizationServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.SourceServiceJooqImpl;
import io.airbyte.data.services.impls.jooq.WorkspaceServiceJooqImpl;
import io.airbyte.data.services.shared.ActorServicePaginationHelper;
import io.airbyte.db.init.DatabaseInitializationException;
import io.airbyte.featureflag.TestClient;
import io.airbyte.metrics.MetricClient;
import io.airbyte.protocol.models.v0.AirbyteGlobalState;
import io.airbyte.protocol.models.v0.AirbyteStateMessage;
import io.airbyte.protocol.models.v0.AirbyteStateMessage.AirbyteStateType;
import io.airbyte.protocol.models.v0.AirbyteStreamState;
import io.airbyte.protocol.models.v0.StreamDescriptor;
import io.airbyte.test.utils.BaseConfigDatabaseTest;
import io.airbyte.validation.json.JsonValidationException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.jooq.JSONB;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class StatePersistenceTest extends BaseConfigDatabaseTest {

  private StatePersistence statePersistence;
  private UUID connectionId;
  private static final String STATE_ONE = "\"state1\"";
  private static final String STATE_TWO = "\"state2\"";
  private static final String STATE_WITH_NAMESPACE = "\"state s1.n1\"";
  private static final String STREAM_STATE_2 = "\"state s2\"";
  private static final String GLOBAL_STATE = "\"my global state\"";
  private static final String STATE = "state";
  private static final UUID DATAPLANE_GROUP_ID = UUID.randomUUID();

  private ConnectionService connectionService;
  private SourceService sourceService;
  private DestinationService destinationService;
  private WorkspaceService workspaceService;

  @BeforeEach
  void beforeEach() throws DatabaseInitializationException, IOException, JsonValidationException, SQLException {
    truncateAllTables();

    statePersistence = new StatePersistence(database, new ConnectionServiceJooqImpl(database));

    final var featureFlagClient = mock(TestClient.class);
    final var secretsRepositoryReader = mock(SecretsRepositoryReader.class);
    final var secretsRepositoryWriter = mock(SecretsRepositoryWriter.class);
    final var secretPersistenceConfigService = mock(SecretPersistenceConfigService.class);
    final var connectionTimelineEventService = mock(ConnectionTimelineEventService.class);
    final var metricClient = mock(MetricClient.class);
    final var actorPaginationServiceHelper = mock(ActorServicePaginationHelper.class);

    final OrganizationService organizationService = new OrganizationServiceJooqImpl(database);
    organizationService.writeOrganization(MockData.defaultOrganization());

    final DataplaneGroupService dataplaneGroupService = new DataplaneGroupServiceTestJooqImpl(database);
    dataplaneGroupService.writeDataplaneGroup(new DataplaneGroup()
        .withId(DATAPLANE_GROUP_ID)
        .withOrganizationId(DEFAULT_ORGANIZATION_ID)
        .withName("test")
        .withEnabled(true)
        .withTombstone(false));

    connectionService = mock(ConnectionService.class);
    final var actorDefinitionVersionUpdater = new ActorDefinitionVersionUpdater(
        featureFlagClient,
        connectionService,
        new ActorDefinitionServiceJooqImpl(database),
        mock(ScopedConfigurationService.class),
        connectionTimelineEventService);
    connectionService = new ConnectionServiceJooqImpl(database);
    sourceService = new SourceServiceJooqImpl(
        database,
        featureFlagClient,
        secretPersistenceConfigService,
        connectionService,
        actorDefinitionVersionUpdater,
        metricClient,
        actorPaginationServiceHelper);
    destinationService = new DestinationServiceJooqImpl(
        database,
        featureFlagClient,
        connectionService,
        actorDefinitionVersionUpdater,
        metricClient,
        actorPaginationServiceHelper);

    workspaceService = new WorkspaceServiceJooqImpl(
        database,
        featureFlagClient,
        secretsRepositoryReader,
        secretsRepositoryWriter,
        secretPersistenceConfigService,
        metricClient);

    connectionId = setupTestData();

  }

  private UUID setupTestData() throws JsonValidationException, IOException {

    final OrganizationService organizationService = new OrganizationServiceJooqImpl(database);
    organizationService.writeOrganization(MockData.defaultOrganization());

    final StandardWorkspace workspace = MockData.standardWorkspaces().get(0);
    final StandardSourceDefinition sourceDefinition = MockData.publicSourceDefinition();
    final SourceConnection sourceConnection = MockData.sourceConnections().get(0);
    final ActorDefinitionVersion actorDefinitionVersion = MockData.actorDefinitionVersion()
        .withActorDefinitionId(sourceDefinition.getSourceDefinitionId())
        .withVersionId(sourceDefinition.getDefaultVersionId());
    final StandardDestinationDefinition destinationDefinition = MockData.publicDestinationDefinition();
    final ActorDefinitionVersion actorDefinitionVersion2 = MockData.actorDefinitionVersion()
        .withActorDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .withVersionId(destinationDefinition.getDefaultVersionId());
    final DestinationConnection destinationConnection = MockData.destinationConnections().get(0);
    // we don't need sync operations in this test suite, zero them out.
    final StandardSync sync = Jsons.clone(MockData.standardSyncs().get(0)).withOperationIds(Collections.emptyList());

    workspaceService.writeStandardWorkspaceNoSecrets(workspace);
    sourceService.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion, Collections.emptyList());
    sourceService.writeSourceConnectionNoSecrets(sourceConnection);
    destinationService.writeConnectorMetadata(destinationDefinition, actorDefinitionVersion2, Collections.emptyList());
    destinationService.writeDestinationConnectionNoSecrets(destinationConnection);
    connectionService.writeStandardSync(sync);

    return sync.getConnectionId();
  }

  @Test
  void testReadingNonExistingState() throws IOException {
    Assertions.assertTrue(statePersistence.getCurrentState(UUID.randomUUID()).isEmpty());
  }

  @Test
  void testLegacyReadWrite() throws IOException {
    final StateWrapper state0 = new StateWrapper()
        .withStateType(StateType.LEGACY)
        .withLegacyState(Jsons.deserialize("{\"woot\": \"legacy states is passthrough\"}"));

    // Initial write/read loop, making sure we read what we wrote
    statePersistence.updateOrCreateState(connectionId, state0);
    final Optional<StateWrapper> state1 = statePersistence.getCurrentState(connectionId);

    Assertions.assertTrue(state1.isPresent());
    Assertions.assertEquals(StateType.LEGACY, state1.get().getStateType());
    Assertions.assertEquals(state0.getLegacyState(), state1.get().getLegacyState());

    // Updating a state
    final JsonNode newStateJson = Jsons.deserialize("{\"woot\": \"new state\"}");
    final StateWrapper state2 = clone(state1.get()).withLegacyState(newStateJson);
    statePersistence.updateOrCreateState(connectionId, state2);
    final Optional<StateWrapper> state3 = statePersistence.getCurrentState(connectionId);

    Assertions.assertTrue(state3.isPresent());
    Assertions.assertEquals(StateType.LEGACY, state3.get().getStateType());
    Assertions.assertEquals(newStateJson, state3.get().getLegacyState());

    // Deleting a state
    final StateWrapper state4 = clone(state3.get()).withLegacyState(null);
    statePersistence.updateOrCreateState(connectionId, state4);
    Assertions.assertTrue(statePersistence.getCurrentState(connectionId).isEmpty());
  }

  @Test
  void testLegacyMigrationToGlobal() throws IOException {
    final StateWrapper state0 = new StateWrapper()
        .withStateType(StateType.LEGACY)
        .withLegacyState(Jsons.deserialize("{\"woot\": \"legacy states is passthrough\"}"));

    statePersistence.updateOrCreateState(connectionId, state0);

    final StateWrapper newGlobalState = new StateWrapper()
        .withStateType(StateType.GLOBAL)
        .withGlobal(new AirbyteStateMessage()
            .withType(AirbyteStateType.GLOBAL)
            .withGlobal(new AirbyteGlobalState()
                .withSharedState(Jsons.deserialize("\"woot\""))
                .withStreamStates(Arrays.asList(
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("s1").withNamespace("n2"))
                        .withStreamState(Jsons.deserialize(STATE_ONE)),
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("s1"))
                        .withStreamState(Jsons.deserialize(STATE_TWO))))));
    statePersistence.updateOrCreateState(connectionId, newGlobalState);
    final StateWrapper storedGlobalState = statePersistence.getCurrentState(connectionId).orElseThrow();
    assertEquals(newGlobalState, storedGlobalState);
  }

  @Test
  void testLegacyMigrationToStream() throws IOException {
    final StateWrapper state0 = new StateWrapper()
        .withStateType(StateType.LEGACY)
        .withLegacyState(Jsons.deserialize("{\"woot\": \"legacy states is passthrough\"}"));

    statePersistence.updateOrCreateState(connectionId, state0);

    final StateWrapper newStreamState = new StateWrapper()
        .withStateType(StateType.STREAM)
        .withStateMessages(Arrays.asList(
            new AirbyteStateMessage()
                .withType(AirbyteStateType.STREAM)
                .withStream(new AirbyteStreamState()
                    .withStreamDescriptor(new StreamDescriptor().withName("s1").withNamespace("n1"))
                    .withStreamState(Jsons.deserialize(STATE_WITH_NAMESPACE))),
            new AirbyteStateMessage()
                .withType(AirbyteStateType.STREAM)
                .withStream(new AirbyteStreamState()
                    .withStreamDescriptor(new StreamDescriptor().withName("s2"))
                    .withStreamState(Jsons.deserialize(STREAM_STATE_2)))));
    statePersistence.updateOrCreateState(connectionId, newStreamState);
    final StateWrapper storedStreamState = statePersistence.getCurrentState(connectionId).orElseThrow();
    assertEquals(newStreamState, storedStreamState);
  }

  @Test
  void testGlobalReadWrite() throws IOException {
    final StateWrapper state0 = new StateWrapper()
        .withStateType(StateType.GLOBAL)
        .withGlobal(new AirbyteStateMessage()
            .withType(AirbyteStateType.GLOBAL)
            .withGlobal(new AirbyteGlobalState()
                .withSharedState(Jsons.deserialize(GLOBAL_STATE))
                .withStreamStates(Arrays.asList(
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("s1").withNamespace("n2"))
                        .withStreamState(Jsons.deserialize(STATE_ONE)),
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("s1"))
                        .withStreamState(Jsons.deserialize(STATE_TWO))))));

    // Initial write/read loop, making sure we read what we wrote
    statePersistence.updateOrCreateState(connectionId, state0);
    final Optional<StateWrapper> state1 = statePersistence.getCurrentState(connectionId);
    Assertions.assertTrue(state1.isPresent());
    assertEquals(state0, state1.get());

    // Updating a state
    final StateWrapper state2 = clone(state1.get());
    state2.getGlobal()
        .getGlobal().withSharedState(Jsons.deserialize("\"updated shared state\""))
        .getStreamStates().get(1).withStreamState(Jsons.deserialize("\"updated state2\""));
    statePersistence.updateOrCreateState(connectionId, state2);
    final Optional<StateWrapper> state3 = statePersistence.getCurrentState(connectionId);

    Assertions.assertTrue(state3.isPresent());
    assertEquals(state2, state3.get());

    // Updating a state with name and namespace
    final StateWrapper state4 = clone(state1.get());
    state4.getGlobal().getGlobal()
        .getStreamStates().get(0).withStreamState(Jsons.deserialize("\"updated state1\""));
    statePersistence.updateOrCreateState(connectionId, state4);
    final Optional<StateWrapper> state5 = statePersistence.getCurrentState(connectionId);

    Assertions.assertTrue(state5.isPresent());
    assertEquals(state4, state5.get());
  }

  @Test
  void testGlobalPartialReset() throws IOException {
    final StateWrapper state0 = new StateWrapper()
        .withStateType(StateType.GLOBAL)
        .withGlobal(new AirbyteStateMessage()
            .withType(AirbyteStateType.GLOBAL)
            .withGlobal(new AirbyteGlobalState()
                .withSharedState(Jsons.deserialize(GLOBAL_STATE))
                .withStreamStates(Arrays.asList(
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("s1").withNamespace("n2"))
                        .withStreamState(Jsons.deserialize(STATE_ONE)),
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("s1"))
                        .withStreamState(Jsons.deserialize(STATE_TWO))))));

    // Set the initial state
    statePersistence.updateOrCreateState(connectionId, state0);

    // incomplete reset does not remove the state
    final StateWrapper incompletePartialReset = new StateWrapper()
        .withStateType(StateType.GLOBAL)
        .withGlobal(new AirbyteStateMessage()
            .withType(AirbyteStateType.GLOBAL)
            .withGlobal(new AirbyteGlobalState()
                .withSharedState(Jsons.deserialize(GLOBAL_STATE))
                .withStreamStates(List.of(
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("s1"))
                        .withStreamState(Jsons.deserialize(STATE_TWO))))));
    statePersistence.updateOrCreateState(connectionId, incompletePartialReset);
    final StateWrapper incompletePartialResetResult = statePersistence.getCurrentState(connectionId).orElseThrow();
    Assertions.assertEquals(state0, incompletePartialResetResult);

    // The good partial reset
    final StateWrapper partialReset = new StateWrapper()
        .withStateType(StateType.GLOBAL)
        .withGlobal(new AirbyteStateMessage()
            .withType(AirbyteStateType.GLOBAL)
            .withGlobal(new AirbyteGlobalState()
                .withSharedState(Jsons.deserialize(GLOBAL_STATE))
                .withStreamStates(Arrays.asList(
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("s1").withNamespace("n2"))
                        .withStreamState(Jsons.deserialize(STATE_ONE)),
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("s1"))
                        .withStreamState(null)))));
    statePersistence.updateOrCreateState(connectionId, partialReset);
    final StateWrapper partialResetResult = statePersistence.getCurrentState(connectionId).orElseThrow();

    Assertions.assertEquals(partialReset.getGlobal().getGlobal().getSharedState(),
        partialResetResult.getGlobal().getGlobal().getSharedState());
    // {"name": "s1"} should have been removed from the stream states
    Assertions.assertEquals(1, partialResetResult.getGlobal().getGlobal().getStreamStates().size());
    Assertions.assertEquals(partialReset.getGlobal().getGlobal().getStreamStates().get(0),
        partialResetResult.getGlobal().getGlobal().getStreamStates().get(0));
  }

  @Test
  void testGlobalFullReset() throws IOException {
    final StateWrapper state0 = new StateWrapper()
        .withStateType(StateType.GLOBAL)
        .withGlobal(new AirbyteStateMessage()
            .withType(AirbyteStateType.GLOBAL)
            .withGlobal(new AirbyteGlobalState()
                .withSharedState(Jsons.deserialize(GLOBAL_STATE))
                .withStreamStates(Arrays.asList(
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("s1").withNamespace("n2"))
                        .withStreamState(Jsons.deserialize(STATE_ONE)),
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("s1"))
                        .withStreamState(Jsons.deserialize(STATE_TWO))))));

    final StateWrapper fullReset = new StateWrapper()
        .withStateType(StateType.GLOBAL)
        .withGlobal(new AirbyteStateMessage()
            .withType(AirbyteStateType.GLOBAL)
            .withGlobal(new AirbyteGlobalState()
                .withSharedState(null)
                .withStreamStates(Arrays.asList(
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("s1").withNamespace("n2"))
                        .withStreamState(null),
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("s1"))
                        .withStreamState(null)))));

    statePersistence.updateOrCreateState(connectionId, state0);
    statePersistence.updateOrCreateState(connectionId, fullReset);
    final Optional<StateWrapper> fullResetResult = statePersistence.getCurrentState(connectionId);
    Assertions.assertTrue(fullResetResult.isEmpty());
  }

  @Test
  void testGlobalStateAllowsEmptyNameAndNamespace() throws IOException {
    final StateWrapper state0 = new StateWrapper()
        .withStateType(StateType.GLOBAL)
        .withGlobal(new AirbyteStateMessage()
            .withType(AirbyteStateType.GLOBAL)
            .withGlobal(new AirbyteGlobalState()
                .withSharedState(Jsons.deserialize(GLOBAL_STATE))
                .withStreamStates(Arrays.asList(
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName(""))
                        .withStreamState(Jsons.deserialize("\"empty name state\"")),
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("").withNamespace(""))
                        .withStreamState(Jsons.deserialize("\"empty name and namespace state\""))))));

    statePersistence.updateOrCreateState(connectionId, state0);
    final StateWrapper state1 = statePersistence.getCurrentState(connectionId).orElseThrow();
    assertEquals(state0, state1);
  }

  @Test
  void testStreamReadWrite() throws IOException {
    final StateWrapper state0 = new StateWrapper()
        .withStateType(StateType.STREAM)
        .withStateMessages(Arrays.asList(
            new AirbyteStateMessage()
                .withType(AirbyteStateType.STREAM)
                .withStream(new AirbyteStreamState()
                    .withStreamDescriptor(new StreamDescriptor().withName("s1").withNamespace("n1"))
                    .withStreamState(Jsons.deserialize(STATE_WITH_NAMESPACE))),
            new AirbyteStateMessage()
                .withType(AirbyteStateType.STREAM)
                .withStream(new AirbyteStreamState()
                    .withStreamDescriptor(new StreamDescriptor().withName("s2"))
                    .withStreamState(Jsons.deserialize(STREAM_STATE_2)))));

    // Initial write/read loop, making sure we read what we wrote
    statePersistence.updateOrCreateState(connectionId, state0);
    final StateWrapper state1 = statePersistence.getCurrentState(connectionId).orElseThrow();
    assertEquals(state0, state1);

    // Updating a state
    final StateWrapper state2 = clone(state1);
    state2.getStateMessages().get(1).getStream().withStreamState(Jsons.deserialize("\"updated state s2\""));
    statePersistence.updateOrCreateState(connectionId, state2);
    final StateWrapper state3 = statePersistence.getCurrentState(connectionId).orElseThrow();
    assertEquals(state2, state3);

    // Updating a state with name and namespace
    final StateWrapper state4 = clone(state1);
    state4.getStateMessages().get(0).getStream().withStreamState(Jsons.deserialize("\"updated state s1\""));
    statePersistence.updateOrCreateState(connectionId, state4);
    final StateWrapper state5 = statePersistence.getCurrentState(connectionId).orElseThrow();
    assertEquals(state4, state5);
  }

  @Test
  void testStreamPartialUpdates() throws IOException {
    final StateWrapper state0 = new StateWrapper()
        .withStateType(StateType.STREAM)
        .withStateMessages(Arrays.asList(
            new AirbyteStateMessage()
                .withType(AirbyteStateType.STREAM)
                .withStream(new AirbyteStreamState()
                    .withStreamDescriptor(new StreamDescriptor().withName("s1").withNamespace("n1"))
                    .withStreamState(Jsons.deserialize(STATE_WITH_NAMESPACE))),
            new AirbyteStateMessage()
                .withType(AirbyteStateType.STREAM)
                .withStream(new AirbyteStreamState()
                    .withStreamDescriptor(new StreamDescriptor().withName("s2"))
                    .withStreamState(Jsons.deserialize(STREAM_STATE_2)))));

    statePersistence.updateOrCreateState(connectionId, state0);

    // Partial update
    final StateWrapper partialUpdate = new StateWrapper()
        .withStateType(StateType.STREAM)
        .withStateMessages(Collections.singletonList(
            new AirbyteStateMessage()
                .withType(AirbyteStateType.STREAM)
                .withStream(new AirbyteStreamState()
                    .withStreamDescriptor(new StreamDescriptor().withName("s1").withNamespace("n1"))
                    .withStreamState(Jsons.deserialize("\"updated\"")))));
    statePersistence.updateOrCreateState(connectionId, partialUpdate);
    final StateWrapper partialUpdateResult = statePersistence.getCurrentState(connectionId).orElseThrow();
    assertEquals(
        new StateWrapper()
            .withStateType(StateType.STREAM)
            .withStateMessages(Arrays.asList(
                new AirbyteStateMessage()
                    .withType(AirbyteStateType.STREAM)
                    .withStream(new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("s1").withNamespace("n1"))
                        .withStreamState(Jsons.deserialize("\"updated\""))),
                new AirbyteStateMessage()
                    .withType(AirbyteStateType.STREAM)
                    .withStream(new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("s2"))
                        .withStreamState(Jsons.deserialize(STREAM_STATE_2))))),
        partialUpdateResult);

    // Partial Reset
    final StateWrapper partialReset = new StateWrapper()
        .withStateType(StateType.STREAM)
        .withStateMessages(Collections.singletonList(
            new AirbyteStateMessage()
                .withType(AirbyteStateType.STREAM)
                .withStream(new AirbyteStreamState()
                    .withStreamDescriptor(new StreamDescriptor().withName("s2"))
                    .withStreamState(null))));
    statePersistence.updateOrCreateState(connectionId, partialReset);
    final StateWrapper partialResetResult = statePersistence.getCurrentState(connectionId).orElseThrow();
    assertEquals(
        new StateWrapper()
            .withStateType(StateType.STREAM)
            .withStateMessages(List.of(
                new AirbyteStateMessage()
                    .withType(AirbyteStateType.STREAM)
                    .withStream(new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("s1").withNamespace("n1"))
                        .withStreamState(Jsons.deserialize("\"updated\""))))),
        partialResetResult);
  }

  @Test
  void testStreamFullReset() throws IOException {
    final StateWrapper state0 = new StateWrapper()
        .withStateType(StateType.STREAM)
        .withStateMessages(Arrays.asList(
            new AirbyteStateMessage()
                .withType(AirbyteStateType.STREAM)
                .withStream(new AirbyteStreamState()
                    .withStreamDescriptor(new StreamDescriptor().withName("s1").withNamespace("n1"))
                    .withStreamState(Jsons.deserialize(STATE_WITH_NAMESPACE))),
            new AirbyteStateMessage()
                .withType(AirbyteStateType.STREAM)
                .withStream(new AirbyteStreamState()
                    .withStreamDescriptor(new StreamDescriptor().withName("s2"))
                    .withStreamState(Jsons.deserialize(STREAM_STATE_2)))));

    statePersistence.updateOrCreateState(connectionId, state0);

    // Partial update
    final StateWrapper fullReset = new StateWrapper()
        .withStateType(StateType.STREAM)
        .withStateMessages(Arrays.asList(
            new AirbyteStateMessage()
                .withType(AirbyteStateType.STREAM)
                .withStream(new AirbyteStreamState()
                    .withStreamDescriptor(new StreamDescriptor().withName("s1").withNamespace("n1"))
                    .withStreamState(null)),
            new AirbyteStateMessage()
                .withType(AirbyteStateType.STREAM)
                .withStream(new AirbyteStreamState()
                    .withStreamDescriptor(new StreamDescriptor().withName("s2"))
                    .withStreamState(null))));
    statePersistence.updateOrCreateState(connectionId, fullReset);
    final Optional<StateWrapper> fullResetResult = statePersistence.getCurrentState(connectionId);
    Assertions.assertTrue(fullResetResult.isEmpty());
  }

  @Test
  void testInconsistentTypeUpdates() throws IOException, SQLException {
    final StateWrapper streamState = new StateWrapper()
        .withStateType(StateType.STREAM)
        .withStateMessages(Arrays.asList(
            new AirbyteStateMessage()
                .withType(AirbyteStateType.STREAM)
                .withStream(new AirbyteStreamState()
                    .withStreamDescriptor(new StreamDescriptor().withName("s1").withNamespace("n1"))
                    .withStreamState(Jsons.deserialize(STATE_WITH_NAMESPACE))),
            new AirbyteStateMessage()
                .withType(AirbyteStateType.STREAM)
                .withStream(new AirbyteStreamState()
                    .withStreamDescriptor(new StreamDescriptor().withName("s2"))
                    .withStreamState(Jsons.deserialize(STREAM_STATE_2)))));
    statePersistence.updateOrCreateState(connectionId, streamState);

    Assertions.assertThrows(IllegalStateException.class, () -> {
      final StateWrapper globalState = new StateWrapper()
          .withStateType(StateType.GLOBAL)
          .withGlobal(new AirbyteStateMessage()
              .withType(AirbyteStateType.GLOBAL)
              .withGlobal(new AirbyteGlobalState()
                  .withSharedState(Jsons.deserialize(GLOBAL_STATE))
                  .withStreamStates(Arrays.asList(
                      new AirbyteStreamState()
                          .withStreamDescriptor(new StreamDescriptor().withName(""))
                          .withStreamState(Jsons.deserialize("\"empty name state\"")),
                      new AirbyteStreamState()
                          .withStreamDescriptor(new StreamDescriptor().withName("").withNamespace(""))
                          .withStreamState(Jsons.deserialize("\"empty name and namespace state\""))))));
      statePersistence.updateOrCreateState(connectionId, globalState);
    });

    // We should be guarded against those cases let's make sure we don't make things worse if we're in
    // an inconsistent state
    database.transaction(ctx -> {
      ctx.insertInto(DSL.table(STATE))
          .columns(DSL.field("id"), DSL.field("connection_id"), DSL.field("type"), DSL.field(STATE))
          .values(UUID.randomUUID(), connectionId, io.airbyte.db.instance.configs.jooq.generated.enums.StateType.GLOBAL, JSONB.valueOf("{}"))
          .execute();
      return null;
    });
    Assertions.assertThrows(IllegalStateException.class, () -> statePersistence.updateOrCreateState(connectionId, streamState));
    Assertions.assertThrows(IllegalStateException.class, () -> statePersistence.getCurrentState(connectionId));
  }

  @Test
  void testEnumsConversion() {
    // Making sure StateType we write to the DB and the StateType from the protocols are aligned.
    // Otherwise, we'll have to dig through runtime errors.
    Assertions.assertTrue(Enums.isCompatible(
        io.airbyte.db.instance.configs.jooq.generated.enums.StateType.class,
        io.airbyte.config.StateType.class));
  }

  @Test
  void testStatePersistenceLegacyWriteConsistency() throws IOException, SQLException {
    final JsonNode jsonState = Jsons.deserialize("{\"my\": \"state\"}");
    final StateWrapper stateWrapper = new StateWrapper().withStateType(StateType.LEGACY).withLegacyState(jsonState);
    statePersistence.updateOrCreateState(connectionId, stateWrapper);

    // Making sure we still follow the legacy format
    final List<State> readStates = database.transaction(ctx -> ctx.selectFrom(STATE)
        .where(DSL.field("connection_id").eq(connectionId))
        .fetch().map(r -> Jsons.deserialize(r.get(DSL.field(STATE, JSONB.class)).data(), State.class))
        .stream()
        .toList());
    Assertions.assertEquals(1, readStates.size());

    Assertions.assertEquals(readStates.get(0).getState(), stateWrapper.getLegacyState());
  }

  @Test
  void testBulkDeletePerStream() throws IOException {
    final StateWrapper perStreamToModify = new StateWrapper()
        .withStateType(StateType.STREAM)
        .withStateMessages(Arrays.asList(
            new AirbyteStateMessage()
                .withType(AirbyteStateType.STREAM)
                .withStream(new AirbyteStreamState()
                    .withStreamDescriptor(new StreamDescriptor().withName("del-1").withNamespace("del-n1"))
                    .withStreamState(Jsons.deserialize(""))),
            new AirbyteStateMessage()
                .withType(AirbyteStateType.STREAM)
                .withStream(new AirbyteStreamState()
                    .withStreamDescriptor(new StreamDescriptor().withName("keep-1"))
                    .withStreamState(Jsons.deserialize(""))),
            new AirbyteStateMessage()
                .withType(AirbyteStateType.STREAM)
                .withStream(new AirbyteStreamState()
                    .withStreamDescriptor(new StreamDescriptor().withName("del-2"))
                    .withStreamState(Jsons.deserialize(""))),
            new AirbyteStateMessage()
                .withType(AirbyteStateType.STREAM)
                .withStream(new AirbyteStreamState()
                    .withStreamDescriptor(new StreamDescriptor().withName("del-1").withNamespace("del-n2"))
                    .withStreamState(Jsons.deserialize(""))),
            new AirbyteStateMessage()
                .withType(AirbyteStateType.STREAM)
                .withStream(new AirbyteStreamState()
                    .withStreamDescriptor(new StreamDescriptor().withName("keep-1").withNamespace("keep-n1"))
                    .withStreamState(Jsons.deserialize("")))));
    statePersistence.updateOrCreateState(connectionId, clone(perStreamToModify));

    final var toDelete = Set.of(
        new io.airbyte.config.StreamDescriptor().withName("del-1").withNamespace("del-n1"),
        new io.airbyte.config.StreamDescriptor().withName("del-2"),
        new io.airbyte.config.StreamDescriptor().withName("del-1").withNamespace("del-n2"));
    statePersistence.bulkDelete(connectionId, toDelete);

    var curr = statePersistence.getCurrentState(connectionId);
    final StateWrapper exp = new StateWrapper()
        .withStateType(StateType.STREAM)
        .withStateMessages(Arrays.asList(
            new AirbyteStateMessage()
                .withType(AirbyteStateType.STREAM)
                .withStream(new AirbyteStreamState()
                    .withStreamDescriptor(new StreamDescriptor().withName("keep-1"))
                    .withStreamState(Jsons.deserialize(""))),
            new AirbyteStateMessage()
                .withType(AirbyteStateType.STREAM)
                .withStream(new AirbyteStreamState()
                    .withStreamDescriptor(new StreamDescriptor().withName("keep-1").withNamespace("keep-n1"))
                    .withStreamState(Jsons.deserialize("")))));
    assertEquals(exp, curr.get());
  }

  @Test
  void testBulkDeleteGlobal() throws IOException {
    final StateWrapper globalToModify = new StateWrapper()
        .withStateType(StateType.GLOBAL)
        .withGlobal(new AirbyteStateMessage()
            .withType(AirbyteStateType.GLOBAL)
            .withGlobal(new AirbyteGlobalState()
                .withSharedState(Jsons.deserialize("\"woot\""))
                .withStreamStates(Arrays.asList(
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("del-1").withNamespace("del-n1"))
                        .withStreamState(Jsons.deserialize("")),
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("keep-1"))
                        .withStreamState(Jsons.deserialize("")),
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("del-2"))
                        .withStreamState(Jsons.deserialize("")),
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("del-1").withNamespace("del-n2"))
                        .withStreamState(Jsons.deserialize("")),
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("keep-1").withNamespace("keep-n1"))
                        .withStreamState(Jsons.deserialize(""))))));

    statePersistence.updateOrCreateState(connectionId, clone(globalToModify));

    final var toDelete = Set.of(
        new io.airbyte.config.StreamDescriptor().withName("del-1").withNamespace("del-n1"),
        new io.airbyte.config.StreamDescriptor().withName("del-2"),
        new io.airbyte.config.StreamDescriptor().withName("del-1").withNamespace("del-n2"));
    statePersistence.bulkDelete(connectionId, toDelete);

    var curr = statePersistence.getCurrentState(connectionId);
    final StateWrapper exp = new StateWrapper()
        .withStateType(StateType.GLOBAL)
        .withGlobal(new AirbyteStateMessage()
            .withType(AirbyteStateType.GLOBAL)
            .withGlobal(new AirbyteGlobalState()
                .withSharedState(Jsons.deserialize("\"woot\""))
                .withStreamStates(Arrays.asList(
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("keep-1"))
                        .withStreamState(Jsons.deserialize("")),
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("keep-1").withNamespace("keep-n1"))
                        .withStreamState(Jsons.deserialize(""))))));
    assertEquals(exp, curr.get());
  }

  @Test
  void testBulkDeleteGlobalAllStreams() throws IOException {
    final StateWrapper globalToModify = new StateWrapper()
        .withStateType(StateType.GLOBAL)
        .withGlobal(new AirbyteStateMessage()
            .withType(AirbyteStateType.GLOBAL)
            .withGlobal(new AirbyteGlobalState()
                .withSharedState(Jsons.deserialize("\"woot\""))
                .withStreamStates(Arrays.asList(
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("del-1").withNamespace("del-n1"))
                        .withStreamState(Jsons.deserialize("")),
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("del-2"))
                        .withStreamState(Jsons.deserialize("")),
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("del-1").withNamespace("del-n2"))
                        .withStreamState(Jsons.deserialize(""))))));

    statePersistence.updateOrCreateState(connectionId, clone(globalToModify));

    final var toDelete = Set.of(
        new io.airbyte.config.StreamDescriptor().withName("del-1").withNamespace("del-n1"),
        new io.airbyte.config.StreamDescriptor().withName("del-2"),
        new io.airbyte.config.StreamDescriptor().withName("del-1").withNamespace("del-n2"));
    statePersistence.bulkDelete(connectionId, toDelete);

    var curr = statePersistence.getCurrentState(connectionId);

    assertTrue(curr.isEmpty());
  }

  @Test
  void testBulkDeleteCorrectConnection() throws IOException, JsonValidationException {
    final StateWrapper globalToModify = new StateWrapper()
        .withStateType(StateType.GLOBAL)
        .withGlobal(new AirbyteStateMessage()
            .withType(AirbyteStateType.GLOBAL)
            .withGlobal(new AirbyteGlobalState()
                .withSharedState(Jsons.deserialize("\"woot\""))
                .withStreamStates(Arrays.asList(
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("del-1").withNamespace("del-n1"))
                        .withStreamState(Jsons.deserialize("")),
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("keep-1"))
                        .withStreamState(Jsons.deserialize("")),
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("del-2"))
                        .withStreamState(Jsons.deserialize(""))))));

    statePersistence.updateOrCreateState(connectionId, clone(globalToModify));

    final var secondConn = setupSecondConnection();
    statePersistence.updateOrCreateState(secondConn, clone(globalToModify));

    final var toDelete = Set.of(
        new io.airbyte.config.StreamDescriptor().withName("del-1").withNamespace("del-n1"),
        new io.airbyte.config.StreamDescriptor().withName("del-2"));
    statePersistence.bulkDelete(connectionId, toDelete);

    var curr = statePersistence.getCurrentState(connectionId);
    final StateWrapper exp = new StateWrapper()
        .withStateType(StateType.GLOBAL)
        .withGlobal(new AirbyteStateMessage()
            .withType(AirbyteStateType.GLOBAL)
            .withGlobal(new AirbyteGlobalState()
                .withSharedState(Jsons.deserialize("\"woot\""))
                .withStreamStates(Collections.singletonList(
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("keep-1"))
                        .withStreamState(Jsons.deserialize(""))))));
    assertEquals(exp, curr.get());

    var untouched = statePersistence.getCurrentState(secondConn);
    assertEquals(globalToModify, untouched.get());
  }

  @Test
  void testBulkDeleteNoStreamsNoDelete() throws IOException, JsonValidationException {
    final StateWrapper globalToModify = new StateWrapper()
        .withStateType(StateType.GLOBAL)
        .withGlobal(new AirbyteStateMessage()
            .withType(AirbyteStateType.GLOBAL)
            .withGlobal(new AirbyteGlobalState()
                .withSharedState(Jsons.deserialize("\"woot\""))
                .withStreamStates(Arrays.asList(
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("del-1").withNamespace("del-n1"))
                        .withStreamState(Jsons.deserialize("")),
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("keep-1"))
                        .withStreamState(Jsons.deserialize("")),
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("del-2"))
                        .withStreamState(Jsons.deserialize(""))))));

    statePersistence.updateOrCreateState(connectionId, clone(globalToModify));

    final var secondConn = setupSecondConnection();
    statePersistence.updateOrCreateState(secondConn, clone(globalToModify));

    statePersistence.bulkDelete(connectionId, Set.of());

    var curr = statePersistence.getCurrentState(connectionId);
    assertEquals(clone(globalToModify), curr.get());

    var untouched = statePersistence.getCurrentState(secondConn);
    assertEquals(globalToModify, untouched.get());
  }

  @Test
  void testEraseGlobalState() throws IOException, JsonValidationException {
    final StateWrapper connectionState = new StateWrapper()
        .withStateType(StateType.GLOBAL)
        .withGlobal(new AirbyteStateMessage()
            .withType(AirbyteStateType.GLOBAL)
            .withGlobal(new AirbyteGlobalState()
                .withSharedState(Jsons.deserialize("0"))
                .withStreamStates(List.of(
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("user"))
                        .withStreamState(Jsons.deserialize("99")),
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("order").withNamespace("business"))
                        .withStreamState(Jsons.deserialize("99"))))));
    final StateWrapper otherState = new StateWrapper()
        .withStateType(StateType.GLOBAL)
        .withGlobal(new AirbyteStateMessage()
            .withType(AirbyteStateType.GLOBAL)
            .withGlobal(new AirbyteGlobalState()
                .withSharedState(Jsons.deserialize("\"2024-01-01\""))
                .withStreamStates(List.of(
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("shop"))
                        .withStreamState(Jsons.deserialize("\"test\"")),
                    new AirbyteStreamState()
                        .withStreamDescriptor(new StreamDescriptor().withName("seller"))
                        .withStreamState(Jsons.deserialize("\"joe\""))))));

    final UUID otherConnectionId = setupSecondConnection();
    statePersistence.updateOrCreateState(connectionId, connectionState);
    Assertions.assertTrue(statePersistence.getCurrentState(connectionId).isPresent(), "The main connection state is not present in database");
    statePersistence.updateOrCreateState(otherConnectionId, otherState);
    Assertions.assertTrue(statePersistence.getCurrentState(otherConnectionId).isPresent(), "The other connection state is not present in database");
    assertEquals(otherState, statePersistence.getCurrentState(otherConnectionId).get(), "The other connection state is incorrect");

    statePersistence.eraseState(connectionId);
    Assertions.assertTrue(statePersistence.getCurrentState(connectionId).isEmpty(), "The main connection state is still present");
    statePersistence.updateOrCreateState(otherConnectionId, otherState);
    Assertions.assertTrue(statePersistence.getCurrentState(otherConnectionId).isPresent(),
        "The other connection state is no longer present in database");
    assertEquals(otherState, statePersistence.getCurrentState(otherConnectionId).get(), "the other connection state has been altered");

  }

  @Test
  void testEraseStreamState() throws JsonValidationException, IOException {
    final StateWrapper connectionState = new StateWrapper()
        .withStateType(StateType.STREAM)
        .withStateMessages(List.of(
            new AirbyteStateMessage().withType(AirbyteStateType.STREAM)
                .withStream(new AirbyteStreamState()
                    .withStreamDescriptor(new StreamDescriptor().withName("user"))
                    .withStreamState(Jsons.deserialize("0"))),
            new AirbyteStateMessage().withType(AirbyteStateType.STREAM)
                .withStream(new AirbyteStreamState()
                    .withStreamDescriptor(new StreamDescriptor().withName("order").withNamespace("business"))
                    .withStreamState(Jsons.deserialize("10")))));
    final StateWrapper otherState = new StateWrapper()
        .withStateType(StateType.STREAM)
        .withStateMessages(List.of(
            new AirbyteStateMessage().withType(AirbyteStateType.STREAM)
                .withStream(new AirbyteStreamState()
                    .withStreamDescriptor(new StreamDescriptor().withName("shop"))
                    .withStreamState(Jsons.deserialize("\"test\""))),
            new AirbyteStateMessage().withType(AirbyteStateType.STREAM)
                .withStream(new AirbyteStreamState()
                    .withStreamDescriptor(new StreamDescriptor().withName("seller"))
                    .withStreamState(Jsons.deserialize("\"joe\"")))

        ));
    final UUID otherConnectionId = setupSecondConnection();
    statePersistence.updateOrCreateState(connectionId, connectionState);
    Assertions.assertTrue(statePersistence.getCurrentState(connectionId).isPresent(), "The main connection state is not present in database");
    statePersistence.updateOrCreateState(otherConnectionId, otherState);
    Assertions.assertTrue(statePersistence.getCurrentState(otherConnectionId).isPresent(), "The other connection state is not present in database");
    assertEquals(otherState, statePersistence.getCurrentState(otherConnectionId).get(), "The other connection state is incorrect");

    statePersistence.eraseState(connectionId);
    Assertions.assertTrue(statePersistence.getCurrentState(connectionId).isEmpty(), "The main connection state is still present");
    statePersistence.updateOrCreateState(otherConnectionId, otherState);
    Assertions.assertTrue(statePersistence.getCurrentState(otherConnectionId).isPresent(),
        "The other connection state is no longer present in database");
    assertEquals(otherState, statePersistence.getCurrentState(otherConnectionId).get(), "the other connection state has been altered");

  }

  private UUID setupSecondConnection() throws JsonValidationException, IOException {
    final StandardWorkspace workspace = MockData.standardWorkspaces().get(0);
    final StandardSourceDefinition sourceDefinition = MockData.publicSourceDefinition();
    final SourceConnection sourceConnection = MockData.sourceConnections().get(0);
    final ActorDefinitionVersion actorDefinitionVersion = MockData.actorDefinitionVersion()
        .withActorDefinitionId(sourceDefinition.getSourceDefinitionId())
        .withVersionId(sourceDefinition.getDefaultVersionId());
    final StandardDestinationDefinition destinationDefinition = MockData.grantableDestinationDefinition1();
    final ActorDefinitionVersion actorDefinitionVersion2 = MockData.actorDefinitionVersion()
        .withActorDefinitionId(destinationDefinition.getDestinationDefinitionId())
        .withVersionId(destinationDefinition.getDefaultVersionId());
    final DestinationConnection destinationConnection = MockData.destinationConnections().get(1);
    // we don't need sync operations in this test suite, zero them out.
    final StandardSync sync = Jsons.clone(MockData.standardSyncs().get(1)).withOperationIds(Collections.emptyList());

    workspaceService.writeStandardWorkspaceNoSecrets(workspace);
    sourceService.writeConnectorMetadata(sourceDefinition, actorDefinitionVersion, Collections.emptyList());
    sourceService.writeSourceConnectionNoSecrets(sourceConnection);
    destinationService.writeConnectorMetadata(destinationDefinition, actorDefinitionVersion2, Collections.emptyList());
    destinationService.writeDestinationConnectionNoSecrets(destinationConnection);
    connectionService.writeStandardSync(sync);
    return sync.getConnectionId();
  }

  private StateWrapper clone(final StateWrapper state) {
    return switch (state.getStateType()) {
      case LEGACY -> new StateWrapper()
          .withLegacyState(Jsons.deserialize(Jsons.serialize(state.getLegacyState())))
          .withStateType(state.getStateType());
      case STREAM -> new StateWrapper()
          .withStateMessages(
              state.getStateMessages().stream().map(msg -> Jsons.deserialize(Jsons.serialize(msg), AirbyteStateMessage.class)).toList())
          .withStateType(state.getStateType());
      case GLOBAL -> new StateWrapper()
          .withGlobal(Jsons.deserialize(Jsons.serialize(state.getGlobal()), AirbyteStateMessage.class))
          .withStateType(state.getStateType());
    };
  }

  private void assertEquals(final StateWrapper lhs, final StateWrapper rhs) {
    Assertions.assertEquals(Jsons.serialize(lhs), Jsons.serialize(rhs));
  }

  private void assertEquals(final StateWrapper lhs, final StateWrapper rhs, final String message) {
    Assertions.assertEquals(Jsons.serialize(lhs), Jsons.serialize(rhs), message);
  }

}
