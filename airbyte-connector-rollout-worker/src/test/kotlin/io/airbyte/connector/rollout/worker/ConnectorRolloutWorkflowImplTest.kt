/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker

import io.airbyte.api.model.generated.ConnectorRolloutActorSelectionInfo
import io.airbyte.api.model.generated.ConnectorRolloutActorSyncInfo
import io.airbyte.commons.temporal.converter.AirbyteTemporalDataConverter
import io.airbyte.config.ConnectorEnumRolloutState
import io.airbyte.config.ConnectorEnumRolloutStrategy
import io.airbyte.config.ConnectorRolloutFinalState
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputFinalize
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputFind
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputGet
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputPause
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputRollout
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityOutputVerifyDefaultVersion
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutOutput
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutWorkflowInput
import io.airbyte.connector.rollout.worker.activities.CleanupActivity
import io.airbyte.connector.rollout.worker.activities.DoRolloutActivity
import io.airbyte.connector.rollout.worker.activities.FinalizeRolloutActivity
import io.airbyte.connector.rollout.worker.activities.FindRolloutActivity
import io.airbyte.connector.rollout.worker.activities.GetRolloutActivity
import io.airbyte.connector.rollout.worker.activities.PauseRolloutActivity
import io.airbyte.connector.rollout.worker.activities.PromoteOrRollbackActivity
import io.airbyte.connector.rollout.worker.activities.StartRolloutActivity
import io.airbyte.connector.rollout.worker.activities.VerifyDefaultVersionActivity
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowClientOptions
import io.temporal.client.WorkflowException
import io.temporal.client.WorkflowFailedException
import io.temporal.client.WorkflowOptions
import io.temporal.client.WorkflowStub
import io.temporal.failure.ApplicationFailure
import io.temporal.failure.TimeoutFailure
import io.temporal.testing.TestEnvironmentOptions
import io.temporal.testing.TestWorkflowEnvironment
import io.temporal.worker.Worker
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.EnumSource
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.Mockito
import org.mockito.Mockito.atLeastOnce
import org.mockito.Mockito.doNothing
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import java.util.UUID
import java.util.stream.Stream
import kotlin.time.DurationUnit
import kotlin.time.toDuration
import kotlin.time.toJavaDuration

class ConnectorRolloutWorkflowImplTest {
  private lateinit var testEnv: TestWorkflowEnvironment
  private lateinit var worker: Worker
  private lateinit var workflowClient: WorkflowClient
  private lateinit var workflowStub: ConnectorRolloutWorkflow
  val doRolloutActivity: DoRolloutActivity =
    Mockito.mock(
      DoRolloutActivity::class.java,
      Mockito.withSettings().withoutAnnotations(),
    )
  val finalizeRolloutActivity: FinalizeRolloutActivity =
    Mockito.mock(
      FinalizeRolloutActivity::class.java,
      Mockito.withSettings().withoutAnnotations(),
    )
  val findRolloutActivity: FindRolloutActivity =
    Mockito.mock(
      FindRolloutActivity::class.java,
      Mockito.withSettings().withoutAnnotations(),
    )
  val getRolloutActivity: GetRolloutActivity =
    Mockito.mock(
      GetRolloutActivity::class.java,
      Mockito.withSettings().withoutAnnotations(),
    )
  val promoteOrRollbackActivity: PromoteOrRollbackActivity =
    Mockito.mock(
      PromoteOrRollbackActivity::class.java,
      Mockito.withSettings().withoutAnnotations(),
    )
  val startRolloutActivity: StartRolloutActivity =
    Mockito.mock(
      StartRolloutActivity::class.java,
      Mockito.withSettings().withoutAnnotations(),
    )
  val verifyDefaultVersionActivity: VerifyDefaultVersionActivity =
    Mockito.mock(
      VerifyDefaultVersionActivity::class.java,
      Mockito.withSettings().withoutAnnotations(),
    )
  val cleanupActivity: CleanupActivity =
    Mockito.mock(
      CleanupActivity::class.java,
      Mockito.withSettings().withoutAnnotations(),
    )
  val pauseRolloutActivity: PauseRolloutActivity =
    Mockito.mock(
      PauseRolloutActivity::class.java,
      Mockito.withSettings().withoutAnnotations(),
    )

  companion object {
    private const val TEST_TASK_QUEUE = "test_task_queue"
    private const val DOCKER_REPOSITORY = "airbyte/source-faker"
    private const val DOCKER_IMAGE_TAG = "0.2"
    private const val PREVIOUS_VERSION_DOCKER_IMAGE_TAG = "0.1"
    private val ACTOR_DEFINITION_ID = UUID.randomUUID()
    private val ROLLOUT_ID = UUID.randomUUID()
    private const val WORKFLOW_ID = "WORKFLOW1"
    private val ACTOR_IDS = listOf(UUID.randomUUID())
    private val USER_ID = UUID.randomUUID()
    private val ROLLOUT_STRATEGY = ConnectorEnumRolloutStrategy.MANUAL

    @JvmStatic
    fun exceptionProvider(): Stream<Arguments> =
      Stream.of(
        Arguments.of(ApplicationFailure::class.java, "Simulated ApplicationFailure"),
        Arguments.of(IllegalArgumentException::class.java, "Simulated IllegalArgumentException"),
      )
  }

  @BeforeEach
  fun setUp() {
    testEnv =
      TestWorkflowEnvironment.newInstance(
        TestEnvironmentOptions
          .newBuilder()
          .setWorkflowClientOptions(
            WorkflowClientOptions
              .newBuilder()
              .setDataConverter(
                AirbyteTemporalDataConverter(),
              ).build(),
          ).build(),
      )
    worker = testEnv.newWorker(TEST_TASK_QUEUE)
    worker.registerWorkflowImplementationTypes(ConnectorRolloutWorkflowImpl::class.java)
    worker.registerActivitiesImplementations(
      doRolloutActivity,
      finalizeRolloutActivity,
      findRolloutActivity,
      getRolloutActivity,
      promoteOrRollbackActivity,
      startRolloutActivity,
      verifyDefaultVersionActivity,
      cleanupActivity,
      pauseRolloutActivity,
    )

    workflowClient = testEnv.workflowClient
    testEnv.start()
    // Get a workflow stub using the same task queue the worker uses.
    val workflowOptions =
      WorkflowOptions
        .newBuilder()
        .setTaskQueue(TEST_TASK_QUEUE)
        .setWorkflowId(WORKFLOW_ID)
        .build()

    workflowStub = testEnv.workflowClient.newWorkflowStub(ConnectorRolloutWorkflow::class.java, workflowOptions)
  }

  @AfterEach
  fun tearDown() {
    testEnv.close()
  }

  @Test
  fun `test ConnectorRolloutWorkflow automated rollout insufficient data`() {
    val input =
      ConnectorRolloutWorkflowInput(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
        ACTOR_DEFINITION_ID,
        ROLLOUT_ID,
        USER_ID,
        ConnectorEnumRolloutStrategy.AUTOMATED,
        PREVIOUS_VERSION_DOCKER_IMAGE_TAG,
        null,
        null,
        null,
        true,
        1,
        1,
        1,
      )

    val insufficientDataConnectorRolloutOutput =
      ConnectorRolloutOutput(
        state = ConnectorEnumRolloutState.IN_PROGRESS,
        actorSyncs = emptyMap(),
        actorSelectionInfo =
          ConnectorRolloutActorSelectionInfo()
            .numActors(0)
            .numPinnedToConnectorRollout(0)
            .numActorsEligibleOrAlreadyPinned(0),
      )

    `when`(
      startRolloutActivity.startRollout(MockitoHelper.anyObject(), MockitoHelper.anyObject()),
    ).thenReturn(getMockOutput(ConnectorEnumRolloutState.WORKFLOW_STARTED))
    `when`(getRolloutActivity.getRollout(MockitoHelper.anyObject())).thenReturn(insufficientDataConnectorRolloutOutput)

    // Run workflow
    WorkflowClient.start(workflowStub::run, input)
    testEnv.sleep(1.toDuration(DurationUnit.SECONDS).toJavaDuration())

    val workflowById: WorkflowStub = testEnv.workflowClient.newUntypedWorkflowStub(WORKFLOW_ID)

    // The workflow should be waiting for user intervention, so getResult will throw an exception
    var failure: TimeoutFailure? = null
    try {
      workflowById.getResult(String::class.java)
    } catch (e: WorkflowException) {
      failure = e.cause as TimeoutFailure?
      assertEquals("TIMEOUT_TYPE_START_TO_CLOSE", failure!!.timeoutType.toString())
    }
    assertNotNull(failure)

    verify(startRolloutActivity).startRollout(MockitoHelper.anyObject(), MockitoHelper.anyObject())
    verify(getRolloutActivity, times(2)).getRollout(MockitoHelper.anyObject())
    verify(verifyDefaultVersionActivity, Mockito.never()).getAndVerifyDefaultVersion(MockitoHelper.anyObject())
    verify(promoteOrRollbackActivity, Mockito.never()).promoteOrRollback(MockitoHelper.anyObject())
    verify(finalizeRolloutActivity, Mockito.never()).finalizeRollout(MockitoHelper.anyObject())
  }

  @Test
  fun `test ConnectorRolloutWorkflow automated rollout releases when success threshold is met`() {
    val successActorSelectionInfo =
      ConnectorRolloutActorSelectionInfo()
        .numPinnedToConnectorRollout(1)
        .numActorsEligibleOrAlreadyPinned(1)
    val successActorSyncs =
      mapOf<UUID, ConnectorRolloutActorSyncInfo>(
        UUID.randomUUID() to
          ConnectorRolloutActorSyncInfo()
            .numSucceeded(1)
            .numFailed(0)
            .numConnections(1),
      )
    val input =
      ConnectorRolloutWorkflowInput(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
        ACTOR_DEFINITION_ID,
        ROLLOUT_ID,
        USER_ID,
        ConnectorEnumRolloutStrategy.AUTOMATED,
        PREVIOUS_VERSION_DOCKER_IMAGE_TAG,
        null,
        null,
        null,
        true,
        1,
        1,
        1,
      )

    val successConnectorRolloutOutput =
      ConnectorRolloutOutput(
        state = ConnectorEnumRolloutState.IN_PROGRESS,
        actorSyncs = successActorSyncs,
        actorSelectionInfo = successActorSelectionInfo,
      )

    `when`(
      startRolloutActivity.startRollout(MockitoHelper.anyObject(), MockitoHelper.anyObject()),
    ).thenReturn(getMockOutput(ConnectorEnumRolloutState.WORKFLOW_STARTED))
    `when`(getRolloutActivity.getRollout(MockitoHelper.anyObject())).thenReturn(successConnectorRolloutOutput)
    `when`(verifyDefaultVersionActivity.getAndVerifyDefaultVersion(MockitoHelper.anyObject()))
      .thenReturn(ConnectorRolloutActivityOutputVerifyDefaultVersion(true))
    `when`(finalizeRolloutActivity.finalizeRollout(MockitoHelper.anyObject()))
      .thenReturn(getMockOutput(ConnectorEnumRolloutState.SUCCEEDED))

    // Run workflow
    WorkflowClient.start(workflowStub::run, input)

    val workflowById: WorkflowStub = testEnv.workflowClient.newUntypedWorkflowStub(WORKFLOW_ID)

    val result = workflowById.getResult(String::class.java)
    assertEquals(ConnectorEnumRolloutState.SUCCEEDED.toString(), result)

    verify(startRolloutActivity).startRollout(MockitoHelper.anyObject(), MockitoHelper.anyObject())
    verify(getRolloutActivity, times(2)).getRollout(MockitoHelper.anyObject())
    verify(verifyDefaultVersionActivity).getAndVerifyDefaultVersion(MockitoHelper.anyObject())
    verify(promoteOrRollbackActivity).promoteOrRollback(MockitoHelper.anyObject())
    verify(finalizeRolloutActivity).finalizeRollout(MockitoHelper.anyObject())
  }

  @Test
  fun `test ConnectorRolloutWorkflow automated rollout is paused on failures`() {
    val input =
      ConnectorRolloutWorkflowInput(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
        ACTOR_DEFINITION_ID,
        ROLLOUT_ID,
        USER_ID,
        ConnectorEnumRolloutStrategy.AUTOMATED,
        PREVIOUS_VERSION_DOCKER_IMAGE_TAG,
        null,
        null,
        null,
        true,
        1,
        1,
        1,
      )

    val failureConnectorRolloutOutput =
      ConnectorRolloutOutput(
        state = ConnectorEnumRolloutState.IN_PROGRESS,
        actorSyncs =
          mapOf<UUID, ConnectorRolloutActorSyncInfo>(
            UUID.randomUUID() to
              ConnectorRolloutActorSyncInfo()
                .numSucceeded(1)
                .numFailed(1)
                .numConnections(2),
          ),
        actorSelectionInfo =
          ConnectorRolloutActorSelectionInfo()
            .numActors(1)
            .numPinnedToConnectorRollout(1)
            .numActorsEligibleOrAlreadyPinned(1),
      )

    val pausedConnectorRolloutOutput =
      ConnectorRolloutOutput(
        state = ConnectorEnumRolloutState.PAUSED,
        actorSyncs =
          mapOf<UUID, ConnectorRolloutActorSyncInfo>(
            UUID.randomUUID() to
              ConnectorRolloutActorSyncInfo()
                .numSucceeded(1)
                .numFailed(1)
                .numConnections(2),
          ),
        actorSelectionInfo =
          ConnectorRolloutActorSelectionInfo()
            .numActors(1)
            .numPinnedToConnectorRollout(1)
            .numActorsEligibleOrAlreadyPinned(1),
      )

    `when`(
      startRolloutActivity.startRollout(MockitoHelper.anyObject(), MockitoHelper.anyObject()),
    ).thenReturn(getMockOutput(ConnectorEnumRolloutState.WORKFLOW_STARTED))
    `when`(getRolloutActivity.getRollout(MockitoHelper.anyObject())).thenReturn(failureConnectorRolloutOutput)
    `when`(pauseRolloutActivity.pauseRollout(MockitoHelper.anyObject())).thenReturn(pausedConnectorRolloutOutput)

    // Run workflow
    WorkflowClient.start(workflowStub::run, input)
    testEnv.sleep(10.toDuration(DurationUnit.SECONDS).toJavaDuration())

    val workflowById: WorkflowStub = testEnv.workflowClient.newUntypedWorkflowStub(WORKFLOW_ID)

    // The workflow should be waiting for user intervention, so getResult will throw an exception
    var failure: TimeoutFailure? = null
    try {
      workflowById.getResult(String::class.java)
    } catch (e: WorkflowException) {
      failure = e.cause as TimeoutFailure?
      assertEquals("TIMEOUT_TYPE_START_TO_CLOSE", failure!!.timeoutType.toString())
    }
    assertNotNull(failure)

    verify(startRolloutActivity).startRollout(MockitoHelper.anyObject(), MockitoHelper.anyObject())
    verify(getRolloutActivity, times(2)).getRollout(MockitoHelper.anyObject())
    verify(pauseRolloutActivity).pauseRollout(MockitoHelper.anyObject())
    verify(verifyDefaultVersionActivity, Mockito.never()).getAndVerifyDefaultVersion(MockitoHelper.anyObject())
    verify(promoteOrRollbackActivity, Mockito.never()).promoteOrRollback(MockitoHelper.anyObject())
    verify(finalizeRolloutActivity, Mockito.never()).finalizeRollout(MockitoHelper.anyObject())
  }

  @ParameterizedTest
  @MethodSource("exceptionProvider")
  fun `test ConnectorRolloutWorkflow automated rollout is paused on exception`(
    exceptionType: Class<out Throwable>,
    message: String,
  ) {
    val input =
      ConnectorRolloutWorkflowInput(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
        ACTOR_DEFINITION_ID,
        ROLLOUT_ID,
        USER_ID,
        ConnectorEnumRolloutStrategy.AUTOMATED,
        PREVIOUS_VERSION_DOCKER_IMAGE_TAG,
        null,
        null,
        null,
        true,
        1,
        1,
        1,
      )

    val pausedConnectorRolloutOutput =
      ConnectorRolloutOutput(
        state = ConnectorEnumRolloutState.PAUSED,
        actorSyncs =
          mapOf<UUID, ConnectorRolloutActorSyncInfo>(
            UUID.randomUUID() to
              ConnectorRolloutActorSyncInfo()
                .numSucceeded(1)
                .numFailed(1)
                .numConnections(2),
          ),
        actorSelectionInfo =
          ConnectorRolloutActorSelectionInfo()
            .numActors(1)
            .numPinnedToConnectorRollout(1)
            .numActorsEligibleOrAlreadyPinned(1),
      )

    `when`(
      startRolloutActivity.startRollout(MockitoHelper.anyObject(), MockitoHelper.anyObject()),
    ).thenReturn(getMockOutput(ConnectorEnumRolloutState.WORKFLOW_STARTED))
    if (exceptionType == ApplicationFailure::class.java) {
      `when`(doRolloutActivity.doRollout(MockitoHelper.anyObject()))
        .thenThrow(ApplicationFailure.newFailure("Simulated ApplicationFailure", "TestFailure"))
    } else {
      `when`(doRolloutActivity.doRollout(MockitoHelper.anyObject()))
        .thenThrow(IllegalArgumentException("Simulated IllegalArgumentException"))
    }
    `when`(pauseRolloutActivity.pauseRollout(MockitoHelper.anyObject())).thenReturn(pausedConnectorRolloutOutput)

    // Run workflow
    WorkflowClient.start(workflowStub::run, input)
    testEnv.sleep(10.toDuration(DurationUnit.SECONDS).toJavaDuration())

    val workflowById: WorkflowStub = testEnv.workflowClient.newUntypedWorkflowStub(WORKFLOW_ID)

    // The workflow should be waiting for user intervention, so getResult will throw an exception
    var failure: TimeoutFailure? = null
    try {
      workflowById.getResult(String::class.java)
    } catch (e: WorkflowException) {
      failure = e.cause as TimeoutFailure?
      assertEquals("TIMEOUT_TYPE_START_TO_CLOSE", failure!!.timeoutType.toString())
    }
    assertNotNull(failure)

    verify(startRolloutActivity).startRollout(MockitoHelper.anyObject(), MockitoHelper.anyObject())
    verify(pauseRolloutActivity).pauseRollout(MockitoHelper.anyObject())
    verify(verifyDefaultVersionActivity, Mockito.never()).getAndVerifyDefaultVersion(MockitoHelper.anyObject())
    verify(promoteOrRollbackActivity, Mockito.never()).promoteOrRollback(MockitoHelper.anyObject())
    verify(finalizeRolloutActivity, Mockito.never()).finalizeRollout(MockitoHelper.anyObject())
  }

  @Test
  fun `test ConnectorRolloutWorkflow pauses when rollout expires`() {
    val input =
      ConnectorRolloutWorkflowInput(
        dockerRepository = DOCKER_REPOSITORY,
        dockerImageTag = DOCKER_IMAGE_TAG,
        actorDefinitionId = ACTOR_DEFINITION_ID,
        rolloutId = ROLLOUT_ID,
        updatedBy = USER_ID,
        rolloutStrategy = ConnectorEnumRolloutStrategy.AUTOMATED,
        initialVersionDockerImageTag = PREVIOUS_VERSION_DOCKER_IMAGE_TAG,
        rolloutExpirationSeconds = 1, // Force quick expiration
        waitBetweenRolloutSeconds = 10, // Make sure rollout won't progress before expiration
        waitBetweenSyncResultsQueriesSeconds = 1,
        migratePins = true,
      )

    val inProgressRolloutOutput =
      ConnectorRolloutOutput(
        state = ConnectorEnumRolloutState.IN_PROGRESS,
        actorSyncs = emptyMap(),
        actorSelectionInfo =
          ConnectorRolloutActorSelectionInfo()
            .numActors(0)
            .numPinnedToConnectorRollout(0)
            .numActorsEligibleOrAlreadyPinned(0),
      )

    val pausedRolloutOutput =
      ConnectorRolloutOutput(
        state = ConnectorEnumRolloutState.PAUSED,
        actorSyncs = emptyMap(),
        actorSelectionInfo =
          ConnectorRolloutActorSelectionInfo()
            .numActors(0)
            .numPinnedToConnectorRollout(0)
            .numActorsEligibleOrAlreadyPinned(0),
      )

    `when`(startRolloutActivity.startRollout(MockitoHelper.anyObject(), MockitoHelper.anyObject()))
      .thenReturn(getMockOutput(ConnectorEnumRolloutState.WORKFLOW_STARTED))
    `when`(getRolloutActivity.getRollout(MockitoHelper.anyObject()))
      .thenReturn(inProgressRolloutOutput)
    `when`(pauseRolloutActivity.pauseRollout(MockitoHelper.anyObject()))
      .thenReturn(pausedRolloutOutput)

    WorkflowClient.start(workflowStub::run, input)

    testEnv.sleep(5.toDuration(DurationUnit.SECONDS).toJavaDuration()) // Wait enough for expiration

    val workflowById = testEnv.workflowClient.newUntypedWorkflowStub(WORKFLOW_ID)

    var failure: TimeoutFailure? = null
    try {
      workflowById.getResult(String::class.java)
    } catch (e: WorkflowException) {
      failure = e.cause as TimeoutFailure?
      assertEquals("TIMEOUT_TYPE_START_TO_CLOSE", failure!!.timeoutType.toString())
    }
    assertNotNull(failure)

    verify(startRolloutActivity).startRollout(MockitoHelper.anyObject(), MockitoHelper.anyObject())
    verify(getRolloutActivity, times(2)).getRollout(MockitoHelper.anyObject())
    verify(pauseRolloutActivity).pauseRollout(MockitoHelper.anyObject())
    verify(verifyDefaultVersionActivity, Mockito.never()).getAndVerifyDefaultVersion(MockitoHelper.anyObject())
    verify(promoteOrRollbackActivity, Mockito.never()).promoteOrRollback(MockitoHelper.anyObject())
    verify(finalizeRolloutActivity, Mockito.never()).finalizeRollout(MockitoHelper.anyObject())
  }

  @ParameterizedTest
  @EnumSource(ConnectorRolloutFinalState::class)
  fun `test ConnectorRolloutWorkflow state for manual rollout`(finalState: ConnectorRolloutFinalState) {
    `when`(
      startRolloutActivity.startRollout(MockitoHelper.anyObject(), MockitoHelper.anyObject()),
    ).thenReturn(getMockOutput(ConnectorEnumRolloutState.WORKFLOW_STARTED))

    // Start workflow asynchronously so we can send in `update` commands
    WorkflowClient.start(
      workflowStub::run,
      ConnectorRolloutWorkflowInput(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
        ACTOR_DEFINITION_ID,
        ROLLOUT_ID,
        USER_ID,
        ROLLOUT_STRATEGY,
        null,
        null,
        null,
        null,
        true,
        1,
        1,
        1,
      ),
    )

    if (finalState != ConnectorRolloutFinalState.CANCELED) {
      `when`(
        promoteOrRollbackActivity.promoteOrRollback(MockitoHelper.anyObject()),
      ).thenReturn(ConnectorRolloutOutput(state = ConnectorEnumRolloutState.FINALIZING))
      `when`(verifyDefaultVersionActivity.getAndVerifyDefaultVersion(MockitoHelper.anyObject()))
        .thenReturn(ConnectorRolloutActivityOutputVerifyDefaultVersion(true))
    }
    `when`(
      finalizeRolloutActivity.finalizeRollout(MockitoHelper.anyObject()),
    ).thenReturn(getMockOutput(ConnectorEnumRolloutState.fromValue(finalState.value())))

    workflowStub.finalizeRollout(
      ConnectorRolloutActivityInputFinalize(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
        ACTOR_DEFINITION_ID,
        ROLLOUT_ID,
        PREVIOUS_VERSION_DOCKER_IMAGE_TAG,
        finalState,
        null,
        null,
        USER_ID,
        ROLLOUT_STRATEGY,
      ),
    )

    val workflowById: WorkflowStub = testEnv.workflowClient.newUntypedWorkflowStub(WORKFLOW_ID)

    val result = workflowById.getResult(String::class.java)
    assertEquals(finalState.toString(), result)

    verify(startRolloutActivity).startRollout(MockitoHelper.anyObject(), MockitoHelper.anyObject())
    if (finalState != ConnectorRolloutFinalState.CANCELED) {
      verify(promoteOrRollbackActivity).promoteOrRollback(MockitoHelper.anyObject())
    }
    verify(finalizeRolloutActivity).finalizeRollout(MockitoHelper.anyObject())
  }

  @Test
  fun `test startRollout fails causes workflow failure and calls cleanup activity`() {
    `when`(
      startRolloutActivity.startRollout(
        Mockito.anyString(),
        MockitoHelper.anyObject(),
      ),
    ).thenThrow(RuntimeException("Simulated failure in startRollout"))
    doNothing().`when`(cleanupActivity).cleanup(MockitoHelper.anyObject())

    val workflowById: WorkflowStub = testEnv.workflowClient.newUntypedWorkflowStub(WORKFLOW_ID)

    // Start workflow asynchronously so we can send in `update` commands
    WorkflowClient.start(
      workflowStub::run,
      ConnectorRolloutWorkflowInput(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
        ACTOR_DEFINITION_ID,
        ROLLOUT_ID,
        USER_ID,
        ROLLOUT_STRATEGY,
        null,
        null,
        null,
        null,
        true,
        1,
        1,
        1,
      ),
    )

    // Verify that the exception causes the workflow to fail
    assertThrows(WorkflowFailedException::class.java) {
      workflowById.getResult(String::class.java)
    }

    verify(startRolloutActivity, atLeastOnce()).startRollout(MockitoHelper.anyObject(), MockitoHelper.anyObject())
    verify(cleanupActivity).cleanup(MockitoHelper.anyObject())
  }

  @Test
  fun `test doRollout update handler`() {
    WorkflowClient.start(
      workflowStub::run,
      ConnectorRolloutWorkflowInput(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
        ACTOR_DEFINITION_ID,
        ROLLOUT_ID,
        USER_ID,
        ROLLOUT_STRATEGY,
        null,
        null,
        null,
        null,
        true,
        1,
        1,
        1,
      ),
    )

    `when`(doRolloutActivity.doRollout(MockitoHelper.anyObject()))
      .thenReturn(getMockOutput(ConnectorEnumRolloutState.IN_PROGRESS))
    workflowStub.progressRollout(
      ConnectorRolloutActivityInputRollout(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
        ACTOR_DEFINITION_ID,
        ROLLOUT_ID,
        ACTOR_IDS,
        null,
        USER_ID,
        ROLLOUT_STRATEGY,
      ),
    )
    verify(doRolloutActivity).doRollout(MockitoHelper.anyObject())
  }

  @Test
  fun `test getRollout update handler`() {
    WorkflowClient.start(
      workflowStub::run,
      ConnectorRolloutWorkflowInput(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
        ACTOR_DEFINITION_ID,
        ROLLOUT_ID,
        USER_ID,
        ROLLOUT_STRATEGY,
        null,
        null,
        null,
        null,
        true,
        1,
        1,
        1,
      ),
    )

    workflowStub.getRollout(
      ConnectorRolloutActivityInputGet(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
        ACTOR_DEFINITION_ID,
        ROLLOUT_ID,
      ),
    )
    verify(getRolloutActivity).getRollout(MockitoHelper.anyObject())
  }

  @Test
  fun `test pauseRollout update handler`() {
    WorkflowClient.start(
      workflowStub::run,
      ConnectorRolloutWorkflowInput(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
        ACTOR_DEFINITION_ID,
        ROLLOUT_ID,
        USER_ID,
        ROLLOUT_STRATEGY,
        null,
        null,
        null,
        null,
        true,
        1,
        1,
        1,
      ),
    )

    workflowStub.pauseRollout(
      ConnectorRolloutActivityInputPause(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
        ACTOR_DEFINITION_ID,
        ROLLOUT_ID,
        "test",
      ),
    )
    verify(pauseRolloutActivity).pauseRollout(MockitoHelper.anyObject())
  }

  @Test
  fun `test findRollout update handler`() {
    WorkflowClient.start(
      workflowStub::run,
      ConnectorRolloutWorkflowInput(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
        ACTOR_DEFINITION_ID,
        ROLLOUT_ID,
        USER_ID,
        ROLLOUT_STRATEGY,
        null,
        null,
        null,
        null,
        true,
        1,
        1,
        1,
      ),
    )

    workflowStub.findRollout(
      ConnectorRolloutActivityInputFind(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
        ACTOR_DEFINITION_ID,
      ),
    )
    verify(findRolloutActivity).findRollout(MockitoHelper.anyObject())
  }

  @Test
  fun `test finalizeRollout update handler calls promote and verify and finalize on SUCCEEDED`() {
    WorkflowClient.start(
      workflowStub::run,
      ConnectorRolloutWorkflowInput(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
        ACTOR_DEFINITION_ID,
        ROLLOUT_ID,
        USER_ID,
        ROLLOUT_STRATEGY,
        null,
        null,
        null,
        null,
        true,
        1,
        1,
        1,
      ),
    )

    `when`(promoteOrRollbackActivity.promoteOrRollback(MockitoHelper.anyObject()))
      .thenReturn(getMockOutput(ConnectorEnumRolloutState.FINALIZING))
    `when`(verifyDefaultVersionActivity.getAndVerifyDefaultVersion(MockitoHelper.anyObject()))
      .thenReturn(ConnectorRolloutActivityOutputVerifyDefaultVersion(true))
    `when`(finalizeRolloutActivity.finalizeRollout(MockitoHelper.anyObject()))
      .thenReturn(getMockOutput(ConnectorEnumRolloutState.SUCCEEDED))

    workflowStub.finalizeRollout(
      ConnectorRolloutActivityInputFinalize(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
        ACTOR_DEFINITION_ID,
        ROLLOUT_ID,
        PREVIOUS_VERSION_DOCKER_IMAGE_TAG,
        ConnectorRolloutFinalState.SUCCEEDED,
        null,
        null,
        USER_ID,
        ROLLOUT_STRATEGY,
      ),
    )
    verify(promoteOrRollbackActivity).promoteOrRollback(MockitoHelper.anyObject())
    verify(verifyDefaultVersionActivity).getAndVerifyDefaultVersion(MockitoHelper.anyObject())
    verify(finalizeRolloutActivity).finalizeRollout(MockitoHelper.anyObject())
  }

  @Test
  fun `test finalizeRollout update handler calls rollback and finalize on FAILED_ROLLED_BACK`() {
    WorkflowClient.start(
      workflowStub::run,
      ConnectorRolloutWorkflowInput(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
        ACTOR_DEFINITION_ID,
        ROLLOUT_ID,
        USER_ID,
        ROLLOUT_STRATEGY,
        null,
        null,
        null,
        null,
        true,
        1,
        1,
        1,
      ),
    )

    `when`(promoteOrRollbackActivity.promoteOrRollback(MockitoHelper.anyObject()))
      .thenReturn(getMockOutput(ConnectorEnumRolloutState.FINALIZING))
    `when`(finalizeRolloutActivity.finalizeRollout(MockitoHelper.anyObject()))
      .thenReturn(getMockOutput(ConnectorEnumRolloutState.FAILED_ROLLED_BACK))

    workflowStub.finalizeRollout(
      ConnectorRolloutActivityInputFinalize(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
        ACTOR_DEFINITION_ID,
        ROLLOUT_ID,
        PREVIOUS_VERSION_DOCKER_IMAGE_TAG,
        ConnectorRolloutFinalState.FAILED_ROLLED_BACK,
        null,
        null,
        USER_ID,
        ROLLOUT_STRATEGY,
      ),
    )
    verify(promoteOrRollbackActivity).promoteOrRollback(MockitoHelper.anyObject())
    verify(verifyDefaultVersionActivity, Mockito.never()).getAndVerifyDefaultVersion(MockitoHelper.anyObject())
    verify(finalizeRolloutActivity).finalizeRollout(MockitoHelper.anyObject())
  }

  @Test
  fun `test finalizeRollout update handler only calls finalize on CANCELED`() {
    WorkflowClient.start(
      workflowStub::run,
      ConnectorRolloutWorkflowInput(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
        ACTOR_DEFINITION_ID,
        ROLLOUT_ID,
        USER_ID,
        ROLLOUT_STRATEGY,
        null,
        null,
        null,
        null,
        true,
        1,
        1,
        1,
      ),
    )

    `when`(finalizeRolloutActivity.finalizeRollout(MockitoHelper.anyObject()))
      .thenReturn(getMockOutput(ConnectorEnumRolloutState.CANCELED))

    workflowStub.finalizeRollout(
      ConnectorRolloutActivityInputFinalize(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
        ACTOR_DEFINITION_ID,
        ROLLOUT_ID,
        PREVIOUS_VERSION_DOCKER_IMAGE_TAG,
        ConnectorRolloutFinalState.CANCELED,
        null,
        null,
        USER_ID,
        ROLLOUT_STRATEGY,
      ),
    )
    verify(promoteOrRollbackActivity, Mockito.never()).promoteOrRollback(MockitoHelper.anyObject())
    verify(verifyDefaultVersionActivity, Mockito.never()).getAndVerifyDefaultVersion(MockitoHelper.anyObject())
    verify(finalizeRolloutActivity).finalizeRollout(MockitoHelper.anyObject())
  }

  object MockitoHelper {
    fun <T> anyObject(): T {
      Mockito.any<T>()
      return uninitialized()
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> uninitialized(): T = null as T
  }

  fun getMockOutput(state: ConnectorEnumRolloutState): ConnectorRolloutOutput = ConnectorRolloutOutput(state = state)
}
