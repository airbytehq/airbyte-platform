package io.airbyte.connector.rollout.worker

import io.airbyte.commons.temporal.converter.AirbyteTemporalDataConverter
import io.airbyte.config.ConnectorEnumRolloutState
import io.airbyte.config.ConnectorRolloutFinalState
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputFinalize
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputFind
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputGet
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputRollout
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputStart
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutOutput
import io.airbyte.connector.rollout.worker.activities.CleanupActivity
import io.airbyte.connector.rollout.worker.activities.DoRolloutActivity
import io.airbyte.connector.rollout.worker.activities.FinalizeRolloutActivity
import io.airbyte.connector.rollout.worker.activities.FindRolloutActivity
import io.airbyte.connector.rollout.worker.activities.GetRolloutActivity
import io.airbyte.connector.rollout.worker.activities.PromoteOrRollbackActivity
import io.airbyte.connector.rollout.worker.activities.StartRolloutActivity
import io.airbyte.connector.rollout.worker.activities.VerifyDefaultVersionActivity
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowClientOptions
import io.temporal.client.WorkflowFailedException
import io.temporal.client.WorkflowOptions
import io.temporal.client.WorkflowStub
import io.temporal.client.WorkflowUpdateException
import io.temporal.testing.TestEnvironmentOptions
import io.temporal.testing.TestWorkflowEnvironment
import io.temporal.worker.Worker
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.mockito.Mockito
import org.mockito.Mockito.doNothing
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import java.util.UUID

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

  companion object {
    private const val TEST_TASK_QUEUE = "test_task_queue"
    private const val DOCKER_REPOSITORY = "airbyte/source-faker"
    private const val DOCKER_IMAGE_TAG = "0.1"
    private val ACTOR_DEFINITION_ID = UUID.randomUUID()
    private val ROLLOUT_ID = UUID.randomUUID()
    private const val WORKFLOW_ID = "WORKFLOW1"
    private val ACTOR_IDS = listOf(UUID.randomUUID())
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
      cleanupActivity,
      doRolloutActivity,
      finalizeRolloutActivity,
      findRolloutActivity,
      getRolloutActivity,
      promoteOrRollbackActivity,
      startRolloutActivity,
      verifyDefaultVersionActivity,
    )

    workflowClient = testEnv.workflowClient
    workflowStub =
      workflowClient.newWorkflowStub(
        ConnectorRolloutWorkflow::class.java,
        WorkflowOptions.newBuilder().setTaskQueue(TEST_TASK_QUEUE).build(),
      )
    testEnv.start()
    // Get a workflow stub using the same task queue the worker uses.
    val workflowOptions =
      WorkflowOptions
        .newBuilder()
        .setTaskQueue(TEST_TASK_QUEUE)
        .setWorkflowId(WORKFLOW_ID)
        .build()

    workflowStub = testEnv.workflowClient.newWorkflowStub(ConnectorRolloutWorkflow::class.java, workflowOptions)

    // Start workflow asynchronously so we can send in `update` commands
    WorkflowClient.start(
      workflowStub::run,
      ConnectorRolloutActivityInputStart(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
        ACTOR_DEFINITION_ID,
        ROLLOUT_ID,
      ),
    )
  }

  @AfterEach
  fun tearDown() {
    testEnv.close()
  }

  @ParameterizedTest
  @EnumSource(ConnectorRolloutFinalState::class)
  fun `test ConnectorRolloutWorkflow state`(finalState: ConnectorRolloutFinalState) {
    `when`(startRolloutActivity.startRollout(Mockito.anyString(), MockitoHelper.anyObject()))
      .thenReturn(getMockOutput(ConnectorEnumRolloutState.WORKFLOW_STARTED))
    if (finalState != ConnectorRolloutFinalState.CANCELED_ROLLED_BACK) {
      `when`(
        promoteOrRollbackActivity.promoteOrRollback(MockitoHelper.anyObject()),
      ).thenReturn(ConnectorRolloutOutput(state = ConnectorEnumRolloutState.FINALIZING))
    }
    `when`(
      finalizeRolloutActivity.finalizeRollout(MockitoHelper.anyObject()),
    ).thenReturn(getMockOutput(ConnectorEnumRolloutState.fromValue(finalState.value())))

    // Send a request to the startRollout `update` handler
    workflowStub.startRollout(
      ConnectorRolloutActivityInputStart(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
        ACTOR_DEFINITION_ID,
        ROLLOUT_ID,
      ),
    )
    workflowStub.finalizeRollout(
      ConnectorRolloutActivityInputFinalize(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
        ACTOR_DEFINITION_ID,
        ROLLOUT_ID,
        finalState,
      ),
    )

    val workflowById: WorkflowStub = testEnv.workflowClient.newUntypedWorkflowStub(WORKFLOW_ID)

    val result = workflowById.getResult(String::class.java)
    assertEquals(finalState.toString(), result)

    verify(startRolloutActivity).startRollout(Mockito.anyString(), MockitoHelper.anyObject())
    if (finalState != ConnectorRolloutFinalState.CANCELED_ROLLED_BACK) {
      verify(promoteOrRollbackActivity).promoteOrRollback(MockitoHelper.anyObject())
    }
    verify(finalizeRolloutActivity).finalizeRollout(MockitoHelper.anyObject())
  }

  @Test
  fun `test startRollout fails causes workflow failure and calls cleanup activity`() {
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

    val startRolloutActivity: StartRolloutActivity =
      Mockito.mock(
        StartRolloutActivity::class.java,
        Mockito.withSettings().withoutAnnotations(),
      )
    `when`(
      startRolloutActivity.startRollout(
        Mockito.anyString(),
        MockitoHelper.anyObject(),
      ),
    ).thenThrow(RuntimeException("Simulated failure in startRollout"))
    doNothing().`when`(cleanupActivity).cleanup(MockitoHelper.anyObject())

    worker.registerActivitiesImplementations(
      cleanupActivity,
      doRolloutActivity,
      finalizeRolloutActivity,
      findRolloutActivity,
      getRolloutActivity,
      promoteOrRollbackActivity,
      startRolloutActivity,
      verifyDefaultVersionActivity,
    )

    workflowClient = testEnv.workflowClient
    workflowStub =
      workflowClient.newWorkflowStub(
        ConnectorRolloutWorkflow::class.java,
        WorkflowOptions.newBuilder().setTaskQueue(TEST_TASK_QUEUE).build(),
      )
    testEnv.start()

    // Get a workflow stub using the same task queue the worker uses.
    val workflowOptions =
      WorkflowOptions
        .newBuilder()
        .setTaskQueue(TEST_TASK_QUEUE)
        .setWorkflowId(WORKFLOW_ID)
        .build()

    val workflow: ConnectorRolloutWorkflow =
      testEnv
        .workflowClient
        .newWorkflowStub(ConnectorRolloutWorkflow::class.java, workflowOptions)

    // Start workflow asynchronously so we can send in `update` commands
    val workflowStarted =
      WorkflowClient.start(
        workflow::run,
        ConnectorRolloutActivityInputStart(
          DOCKER_REPOSITORY,
          DOCKER_IMAGE_TAG,
          ACTOR_DEFINITION_ID,
          ROLLOUT_ID,
        ),
      )
    assertEquals(workflowStarted.workflowId, WORKFLOW_ID)

    assertThrows(WorkflowUpdateException::class.java) {
      workflow.startRollout(
        ConnectorRolloutActivityInputStart(
          DOCKER_REPOSITORY,
          DOCKER_IMAGE_TAG,
          ACTOR_DEFINITION_ID,
          ROLLOUT_ID,
        ),
      )
    }
    val workflowById: WorkflowStub = testEnv.workflowClient.newUntypedWorkflowStub(WORKFLOW_ID)

    // Verify that the cleanup activity was called
    verify(cleanupActivity).cleanup(MockitoHelper.anyObject())

    // Verify that the exception causes the workflow to fail
    assertThrows(WorkflowFailedException::class.java) {
      workflowById.getResult(String::class.java)
    }
  }

  @Test
  fun `test doRollout update handler`() {
    `when`(doRolloutActivity.doRollout(MockitoHelper.anyObject()))
      .thenReturn(getMockOutput(ConnectorEnumRolloutState.IN_PROGRESS))
    workflowStub.doRollout(
      ConnectorRolloutActivityInputRollout(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
        ACTOR_DEFINITION_ID,
        ROLLOUT_ID,
        ACTOR_IDS,
      ),
    )
    verify(doRolloutActivity).doRollout(MockitoHelper.anyObject())
  }

  @Test
  fun `test getRollout update handler`() {
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
  fun `test findRollout update handler`() {
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
    `when`(promoteOrRollbackActivity.promoteOrRollback(MockitoHelper.anyObject()))
      .thenReturn(getMockOutput(ConnectorEnumRolloutState.FINALIZING))
    `when`(finalizeRolloutActivity.finalizeRollout(MockitoHelper.anyObject()))
      .thenReturn(getMockOutput(ConnectorEnumRolloutState.SUCCEEDED))

    workflowStub.finalizeRollout(
      ConnectorRolloutActivityInputFinalize(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
        ACTOR_DEFINITION_ID,
        ROLLOUT_ID,
        ConnectorRolloutFinalState.SUCCEEDED,
      ),
    )
    verify(promoteOrRollbackActivity).promoteOrRollback(MockitoHelper.anyObject())
    verify(verifyDefaultVersionActivity).verifyDefaultVersion(MockitoHelper.anyObject())
    verify(finalizeRolloutActivity).finalizeRollout(MockitoHelper.anyObject())
  }

  @Test
  fun `test finalizeRollout update handler calls rollback and finalize on FAILED_ROLLED_BACK`() {
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
        ConnectorRolloutFinalState.FAILED_ROLLED_BACK,
      ),
    )
    verify(promoteOrRollbackActivity).promoteOrRollback(MockitoHelper.anyObject())
    verify(verifyDefaultVersionActivity, Mockito.never()).verifyDefaultVersion(MockitoHelper.anyObject())
    verify(finalizeRolloutActivity).finalizeRollout(MockitoHelper.anyObject())
  }

  @Test
  fun `test finalizeRollout update handler only calls finalize on CANCELED_ROLLED_BACK`() {
    `when`(finalizeRolloutActivity.finalizeRollout(MockitoHelper.anyObject()))
      .thenReturn(getMockOutput(ConnectorEnumRolloutState.CANCELED_ROLLED_BACK))

    workflowStub.finalizeRollout(
      ConnectorRolloutActivityInputFinalize(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
        ACTOR_DEFINITION_ID,
        ROLLOUT_ID,
        ConnectorRolloutFinalState.CANCELED_ROLLED_BACK,
      ),
    )
    verify(promoteOrRollbackActivity, Mockito.never()).promoteOrRollback(MockitoHelper.anyObject())
    verify(verifyDefaultVersionActivity, Mockito.never()).verifyDefaultVersion(MockitoHelper.anyObject())
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
