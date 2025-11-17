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
import io.airbyte.connector.rollout.worker.activities.CleanupActivityImpl
import io.airbyte.connector.rollout.worker.activities.DoRolloutActivity
import io.airbyte.connector.rollout.worker.activities.DoRolloutActivityImpl
import io.airbyte.connector.rollout.worker.activities.FinalizeRolloutActivity
import io.airbyte.connector.rollout.worker.activities.FinalizeRolloutActivityImpl
import io.airbyte.connector.rollout.worker.activities.FindRolloutActivity
import io.airbyte.connector.rollout.worker.activities.FindRolloutActivityImpl
import io.airbyte.connector.rollout.worker.activities.GetRolloutActivity
import io.airbyte.connector.rollout.worker.activities.GetRolloutActivityImpl
import io.airbyte.connector.rollout.worker.activities.PauseRolloutActivity
import io.airbyte.connector.rollout.worker.activities.PauseRolloutActivityImpl
import io.airbyte.connector.rollout.worker.activities.PromoteOrRollbackActivity
import io.airbyte.connector.rollout.worker.activities.PromoteOrRollbackActivityImpl
import io.airbyte.connector.rollout.worker.activities.StartRolloutActivity
import io.airbyte.connector.rollout.worker.activities.StartRolloutActivityImpl
import io.airbyte.connector.rollout.worker.activities.VerifyDefaultVersionActivity
import io.airbyte.connector.rollout.worker.activities.VerifyDefaultVersionActivityImpl
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
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
import java.util.UUID
import kotlin.time.DurationUnit
import kotlin.time.toDuration
import kotlin.time.toJavaDuration

internal class ConnectorRolloutWorkflowImplTest {
  private lateinit var testEnv: TestWorkflowEnvironment
  private lateinit var worker: Worker
  private lateinit var workflowClient: WorkflowClient
  private lateinit var workflowStub: ConnectorRolloutWorkflow
  private lateinit var doRolloutActivity: DoRolloutActivity
  private lateinit var finalizeRolloutActivity: FinalizeRolloutActivity
  private lateinit var findRolloutActivity: FindRolloutActivity
  private lateinit var getRolloutActivity: GetRolloutActivity
  private lateinit var promoteOrRollbackActivity: PromoteOrRollbackActivity
  private lateinit var startRolloutActivity: StartRolloutActivity
  private lateinit var verifyDefaultVersionActivity: VerifyDefaultVersionActivity
  private lateinit var cleanupActivity: CleanupActivity
  private lateinit var pauseRolloutActivity: PauseRolloutActivity

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
    fun exceptionProvider() =
      listOf(
        Arguments.of(ApplicationFailure::class.java, "Simulated ApplicationFailure"),
        Arguments.of(IllegalArgumentException::class.java, "Simulated IllegalArgumentException"),
      )
  }

  @BeforeEach
  fun setUp() {
    doRolloutActivity = mockk<DoRolloutActivityImpl>()
    finalizeRolloutActivity = mockk<FinalizeRolloutActivityImpl>()
    findRolloutActivity = mockk<FindRolloutActivityImpl>()
    getRolloutActivity = mockk<GetRolloutActivityImpl>()
    promoteOrRollbackActivity = mockk<PromoteOrRollbackActivityImpl>()
    startRolloutActivity = mockk<StartRolloutActivityImpl>()
    verifyDefaultVersionActivity = mockk<VerifyDefaultVersionActivityImpl>()
    cleanupActivity = mockk<CleanupActivityImpl>()
    pauseRolloutActivity = mockk<PauseRolloutActivityImpl>()

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

    every {
      startRolloutActivity.startRollout(any(), any())
    } returns getMockOutput(ConnectorEnumRolloutState.WORKFLOW_STARTED)
    every { getRolloutActivity.getRollout(any()) } returns insufficientDataConnectorRolloutOutput
    every { doRolloutActivity.doRollout(any()) } returns getMockOutput(ConnectorEnumRolloutState.IN_PROGRESS)
    every { pauseRolloutActivity.pauseRollout(any()) } returns
      ConnectorRolloutOutput(
        state = ConnectorEnumRolloutState.PAUSED,
        actorSyncs = emptyMap(),
        actorSelectionInfo =
          ConnectorRolloutActorSelectionInfo()
            .numActors(0)
            .numPinnedToConnectorRollout(0)
            .numActorsEligibleOrAlreadyPinned(0),
      )

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

    verify { startRolloutActivity.startRollout(any(), any()) }
    verify(exactly = 2) { getRolloutActivity.getRollout(any()) }
    verify(exactly = 0) { verifyDefaultVersionActivity.getAndVerifyDefaultVersion(any()) }
    verify(exactly = 0) { promoteOrRollbackActivity.promoteOrRollback(any()) }
    verify(exactly = 0) { finalizeRolloutActivity.finalizeRollout(any()) }
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

    every {
      startRolloutActivity.startRollout(any(), any())
    } returns getMockOutput(ConnectorEnumRolloutState.WORKFLOW_STARTED)
    every { getRolloutActivity.getRollout(any()) } returns successConnectorRolloutOutput
    every { doRolloutActivity.doRollout(any()) } returns successConnectorRolloutOutput
    every { promoteOrRollbackActivity.promoteOrRollback(any()) } returns
      getMockOutput(ConnectorEnumRolloutState.FINALIZING)
    every { verifyDefaultVersionActivity.getAndVerifyDefaultVersion(any()) } returns
      ConnectorRolloutActivityOutputVerifyDefaultVersion(true)
    every { finalizeRolloutActivity.finalizeRollout(any()) } returns
      getMockOutput(ConnectorEnumRolloutState.SUCCEEDED)

    // Run workflow
    WorkflowClient.start(workflowStub::run, input)

    val workflowById: WorkflowStub = testEnv.workflowClient.newUntypedWorkflowStub(WORKFLOW_ID)

    val result = workflowById.getResult(String::class.java)
    assertEquals(ConnectorEnumRolloutState.SUCCEEDED.toString(), result)

    verify { startRolloutActivity.startRollout(any(), any()) }
    verify(exactly = 2) { getRolloutActivity.getRollout(any()) }
    verify { verifyDefaultVersionActivity.getAndVerifyDefaultVersion(any()) }
    verify { promoteOrRollbackActivity.promoteOrRollback(any()) }
    verify { finalizeRolloutActivity.finalizeRollout(any()) }
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

    every {
      startRolloutActivity.startRollout(any(), any())
    } returns getMockOutput(ConnectorEnumRolloutState.WORKFLOW_STARTED)
    every { getRolloutActivity.getRollout(any()) } returns failureConnectorRolloutOutput
    every { doRolloutActivity.doRollout(any()) } returns failureConnectorRolloutOutput
    every { pauseRolloutActivity.pauseRollout(any()) } returns pausedConnectorRolloutOutput

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

    verify { startRolloutActivity.startRollout(any(), any()) }
    verify(exactly = 2) { getRolloutActivity.getRollout(any()) }
    verify { pauseRolloutActivity.pauseRollout(any()) }
    verify(exactly = 0) { verifyDefaultVersionActivity.getAndVerifyDefaultVersion(any()) }
    verify(exactly = 0) { promoteOrRollbackActivity.promoteOrRollback(any()) }
    verify(exactly = 0) { finalizeRolloutActivity.finalizeRollout(any()) }
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

    val inProgressRolloutOutput =
      ConnectorRolloutOutput(
        state = ConnectorEnumRolloutState.IN_PROGRESS,
        actorSyncs = emptyMap(),
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

    every {
      startRolloutActivity.startRollout(any(), any())
    } returns getMockOutput(ConnectorEnumRolloutState.WORKFLOW_STARTED)
    every { getRolloutActivity.getRollout(any()) } returns inProgressRolloutOutput
    if (exceptionType == ApplicationFailure::class.java) {
      every { doRolloutActivity.doRollout(any()) } answers {
        throw ApplicationFailure.newFailure("Simulated ApplicationFailure", "TestFailure")
      }
    } else {
      every { doRolloutActivity.doRollout(any()) } answers {
        throw IllegalArgumentException("Simulated IllegalArgumentException")
      }
    }
    every { pauseRolloutActivity.pauseRollout(any()) } returns pausedConnectorRolloutOutput

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

    verify { startRolloutActivity.startRollout(any(), any()) }
    verify { pauseRolloutActivity.pauseRollout(any()) }
    verify(exactly = 0) { verifyDefaultVersionActivity.getAndVerifyDefaultVersion(any()) }
    verify(exactly = 0) { promoteOrRollbackActivity.promoteOrRollback(any()) }
    verify(exactly = 0) { finalizeRolloutActivity.finalizeRollout(any()) }
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

    every { startRolloutActivity.startRollout(any(), any()) } returns
      getMockOutput(ConnectorEnumRolloutState.WORKFLOW_STARTED)
    every { getRolloutActivity.getRollout(any()) } returns
      inProgressRolloutOutput
    every { doRolloutActivity.doRollout(any()) } returns
      inProgressRolloutOutput
    every { pauseRolloutActivity.pauseRollout(any()) } returns
      pausedRolloutOutput

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

    verify { startRolloutActivity.startRollout(any(), any()) }
    verify(exactly = 2) { getRolloutActivity.getRollout(any()) }
    verify { pauseRolloutActivity.pauseRollout(any()) }
    verify(exactly = 0) { verifyDefaultVersionActivity.getAndVerifyDefaultVersion(any()) }
    verify(exactly = 0) { promoteOrRollbackActivity.promoteOrRollback(any()) }
    verify(exactly = 0) { finalizeRolloutActivity.finalizeRollout(any()) }
  }

  @ParameterizedTest
  @EnumSource(ConnectorRolloutFinalState::class)
  fun `test ConnectorRolloutWorkflow state for manual rollout`(finalState: ConnectorRolloutFinalState) {
    every {
      startRolloutActivity.startRollout(any(), any())
    } returns getMockOutput(ConnectorEnumRolloutState.WORKFLOW_STARTED)

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
      every {
        promoteOrRollbackActivity.promoteOrRollback(any())
      } returns ConnectorRolloutOutput(state = ConnectorEnumRolloutState.FINALIZING)
      every { verifyDefaultVersionActivity.getAndVerifyDefaultVersion(any()) } returns
        ConnectorRolloutActivityOutputVerifyDefaultVersion(true)
    }
    every {
      finalizeRolloutActivity.finalizeRollout(any())
    } returns getMockOutput(ConnectorEnumRolloutState.fromValue(finalState.value()))

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

    verify { startRolloutActivity.startRollout(any(), any()) }
    if (finalState != ConnectorRolloutFinalState.CANCELED) {
      verify { promoteOrRollbackActivity.promoteOrRollback(any()) }
    }
    verify { finalizeRolloutActivity.finalizeRollout(any()) }
  }

  @Test
  fun `test startRollout fails causes workflow failure and calls cleanup activity`() {
    every {
      startRolloutActivity.startRollout(
        any<String>(),
        any(),
      )
    } answers { throw RuntimeException("Simulated failure in startRollout") }
    every { cleanupActivity.cleanup(any()) } just Runs

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

    verify(atLeast = 1) { startRolloutActivity.startRollout(any(), any()) }
    verify { cleanupActivity.cleanup(any()) }
  }

  @Test
  fun `test doRollout update handler`() {
    every { startRolloutActivity.startRollout(any(), any()) } returns
      getMockOutput(ConnectorEnumRolloutState.WORKFLOW_STARTED)
    every { doRolloutActivity.doRollout(any()) } returns
      getMockOutput(ConnectorEnumRolloutState.IN_PROGRESS)

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
    verify { doRolloutActivity.doRollout(any()) }
  }

  @Test
  fun `test getRollout update handler`() {
    every { startRolloutActivity.startRollout(any(), any()) } returns
      getMockOutput(ConnectorEnumRolloutState.WORKFLOW_STARTED)
    every { getRolloutActivity.getRollout(any()) } returns
      getMockOutput(ConnectorEnumRolloutState.IN_PROGRESS)

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
    verify { getRolloutActivity.getRollout(any()) }
  }

  @Test
  fun `test pauseRollout update handler`() {
    every { startRolloutActivity.startRollout(any(), any()) } returns
      getMockOutput(ConnectorEnumRolloutState.WORKFLOW_STARTED)
    every { pauseRolloutActivity.pauseRollout(any()) } returns
      getMockOutput(ConnectorEnumRolloutState.PAUSED)

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
    verify { pauseRolloutActivity.pauseRollout(any()) }
  }

  @Test
  fun `test findRollout update handler`() {
    every { startRolloutActivity.startRollout(any(), any()) } returns
      getMockOutput(ConnectorEnumRolloutState.WORKFLOW_STARTED)
    every { findRolloutActivity.findRollout(any()) } returns emptyList()

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
    verify { findRolloutActivity.findRollout(any()) }
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

    every { promoteOrRollbackActivity.promoteOrRollback(any()) } returns
      getMockOutput(ConnectorEnumRolloutState.FINALIZING)
    every { verifyDefaultVersionActivity.getAndVerifyDefaultVersion(any()) } returns
      ConnectorRolloutActivityOutputVerifyDefaultVersion(true)
    every { finalizeRolloutActivity.finalizeRollout(any()) } returns
      getMockOutput(ConnectorEnumRolloutState.SUCCEEDED)

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
    verify { promoteOrRollbackActivity.promoteOrRollback(any()) }
    verify { verifyDefaultVersionActivity.getAndVerifyDefaultVersion(any()) }
    verify { finalizeRolloutActivity.finalizeRollout(any()) }
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

    every { promoteOrRollbackActivity.promoteOrRollback(any()) } returns
      getMockOutput(ConnectorEnumRolloutState.FINALIZING)
    every { finalizeRolloutActivity.finalizeRollout(any()) } returns
      getMockOutput(ConnectorEnumRolloutState.FAILED_ROLLED_BACK)

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
    verify { promoteOrRollbackActivity.promoteOrRollback(any()) }
    verify(exactly = 0) { verifyDefaultVersionActivity.getAndVerifyDefaultVersion(any()) }
    verify { finalizeRolloutActivity.finalizeRollout(any()) }
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

    every { finalizeRolloutActivity.finalizeRollout(any()) } returns
      getMockOutput(ConnectorEnumRolloutState.CANCELED)

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
    verify(exactly = 0) { promoteOrRollbackActivity.promoteOrRollback(any()) }
    verify(exactly = 0) { verifyDefaultVersionActivity.getAndVerifyDefaultVersion(any()) }
    verify { finalizeRolloutActivity.finalizeRollout(any()) }
  }

  fun getMockOutput(state: ConnectorEnumRolloutState): ConnectorRolloutOutput = ConnectorRolloutOutput(state = state)
}
