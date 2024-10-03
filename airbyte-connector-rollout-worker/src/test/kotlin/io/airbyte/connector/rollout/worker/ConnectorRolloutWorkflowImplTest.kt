package io.airbyte.connector.rollout.worker

import io.airbyte.commons.temporal.converter.AirbyteTemporalDataConverter
import io.airbyte.config.ConnectorRolloutFinalState
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputFinalize
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputFind
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputGet
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputRollout
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputStart
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
import java.util.UUID

class ConnectorRolloutWorkflowImplTest {
  private lateinit var testEnv: TestWorkflowEnvironment
  private lateinit var worker: Worker
  private lateinit var workflowClient: WorkflowClient
  private lateinit var workflowStub: ConnectorRolloutWorkflow
  val doRolloutActivity: DoRolloutActivity =
    Mockito.mock<DoRolloutActivity>(
      DoRolloutActivity::class.java,
      Mockito.withSettings().withoutAnnotations(),
    )
  val finalizeRolloutActivity: FinalizeRolloutActivity =
    Mockito.mock<FinalizeRolloutActivity>(
      FinalizeRolloutActivity::class.java,
      Mockito.withSettings().withoutAnnotations(),
    )
  val findRolloutActivity: FindRolloutActivity =
    Mockito.mock<FindRolloutActivity>(
      FindRolloutActivity::class.java,
      Mockito.withSettings().withoutAnnotations(),
    )
  val getRolloutActivity: GetRolloutActivity =
    Mockito.mock<GetRolloutActivity>(
      GetRolloutActivity::class.java,
      Mockito.withSettings().withoutAnnotations(),
    )
  val promoteOrRollbackActivity: PromoteOrRollbackActivity =
    Mockito.mock<PromoteOrRollbackActivity>(
      PromoteOrRollbackActivity::class.java,
      Mockito.withSettings().withoutAnnotations(),
    )
  val startRolloutActivity: StartRolloutActivity =
    Mockito.mock<StartRolloutActivity>(
      StartRolloutActivity::class.java,
      Mockito.withSettings().withoutAnnotations(),
    )
  val verifyDefaultVersionActivity: VerifyDefaultVersionActivity =
    Mockito.mock<VerifyDefaultVersionActivity>(
      VerifyDefaultVersionActivity::class.java,
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
        TestEnvironmentOptions.newBuilder().setWorkflowClientOptions(
          WorkflowClientOptions.newBuilder().setDataConverter(
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
      WorkflowOptions.newBuilder()
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
  }

  @Test
  fun `test startRollout fails causes workflow failure`() {
    testEnv =
      TestWorkflowEnvironment.newInstance(
        TestEnvironmentOptions.newBuilder().setWorkflowClientOptions(
          WorkflowClientOptions.newBuilder().setDataConverter(
            AirbyteTemporalDataConverter(),
          ).build(),
        ).build(),
      )
    worker = testEnv.newWorker(TEST_TASK_QUEUE)
    worker.registerWorkflowImplementationTypes(ConnectorRolloutWorkflowImpl::class.java)

    val startRolloutActivity: StartRolloutActivity =
      Mockito.mock<StartRolloutActivity>(
        StartRolloutActivity::class.java,
        Mockito.withSettings().withoutAnnotations(),
      )
    Mockito.`when`(
      startRolloutActivity.startRollout(
        Mockito.anyString(),
        MockitoHelper.anyObject(),
      ),
    ).thenThrow(RuntimeException("Simulated failure in startRollout"))

    worker.registerActivitiesImplementations(
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
      WorkflowOptions.newBuilder()
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

    // Verify that the exception causes the workflow to fail
    assertThrows(WorkflowFailedException::class.java) {
      workflowById.getResult(String::class.java)
    }
  }

  @Test
  fun `test doRollout update handler`() {
    workflowStub.doRollout(
      ConnectorRolloutActivityInputRollout(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
        ACTOR_DEFINITION_ID,
        ROLLOUT_ID,
        ACTOR_IDS,
      ),
    )
    Mockito.verify(doRolloutActivity).doRollout(MockitoHelper.anyObject())
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
    Mockito.verify(getRolloutActivity).getRollout(MockitoHelper.anyObject())
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
    Mockito.verify(findRolloutActivity).findRollout(MockitoHelper.anyObject())
  }

  @Test
  fun `test finalizeRollout update handler calls promote and verify and finalize on SUCCEEDED`() {
    workflowStub.finalizeRollout(
      ConnectorRolloutActivityInputFinalize(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
        ACTOR_DEFINITION_ID,
        ROLLOUT_ID,
        ConnectorRolloutFinalState.SUCCEEDED,
      ),
    )
    Mockito.verify(promoteOrRollbackActivity).promoteOrRollback(MockitoHelper.anyObject())
    Mockito.verify(verifyDefaultVersionActivity).verifyDefaultVersion(MockitoHelper.anyObject())
    Mockito.verify(finalizeRolloutActivity).finalizeRollout(MockitoHelper.anyObject())
  }

  @Test
  fun `test finalizeRollout update handler calls rollback and finalize on FAILED_ROLLED_BACK`() {
    workflowStub.finalizeRollout(
      ConnectorRolloutActivityInputFinalize(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
        ACTOR_DEFINITION_ID,
        ROLLOUT_ID,
        ConnectorRolloutFinalState.FAILED_ROLLED_BACK,
      ),
    )
    Mockito.verify(promoteOrRollbackActivity).promoteOrRollback(MockitoHelper.anyObject())
    Mockito.verify(verifyDefaultVersionActivity, Mockito.never()).verifyDefaultVersion(MockitoHelper.anyObject())
    Mockito.verify(finalizeRolloutActivity).finalizeRollout(MockitoHelper.anyObject())
  }

  @Test
  fun `test finalizeRollout update handler only calls finalize on CANCELED_ROLLED_BACK`() {
    workflowStub.finalizeRollout(
      ConnectorRolloutActivityInputFinalize(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
        ACTOR_DEFINITION_ID,
        ROLLOUT_ID,
        ConnectorRolloutFinalState.CANCELED_ROLLED_BACK,
      ),
    )
    Mockito.verify(promoteOrRollbackActivity, Mockito.never()).promoteOrRollback(MockitoHelper.anyObject())
    Mockito.verify(verifyDefaultVersionActivity, Mockito.never()).verifyDefaultVersion(MockitoHelper.anyObject())
    Mockito.verify(finalizeRolloutActivity).finalizeRollout(MockitoHelper.anyObject())
  }

  object MockitoHelper {
    fun <T> anyObject(): T {
      Mockito.any<T>()
      return uninitialized()
    }

    @Suppress("UNCHECKED_CAST")
    fun <T> uninitialized(): T = null as T
  }
}
