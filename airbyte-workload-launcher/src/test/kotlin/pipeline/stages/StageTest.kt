package pipeline.stages

import fixtures.RecordFixtures
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.workload.launcher.fixtures.TestStage
import io.airbyte.workload.launcher.metrics.CustomMetricPublisher
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.STAGE_NAME_TAG
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.STATUS_TAG
import io.airbyte.workload.launcher.metrics.WorkloadLauncherMetricMetadata
import io.airbyte.workload.launcher.pipeline.stages.StageName
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class StageTest {
  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun `emits metrics`(success: Boolean) {
    val metricPublisher: CustomMetricPublisher = mockk()
    every { metricPublisher.count(WorkloadLauncherMetricMetadata.WORKLOAD_STAGE_START, *anyVararg()) } returns Unit
    every { metricPublisher.count(WorkloadLauncherMetricMetadata.WORKLOAD_STAGE_DONE, *anyVararg()) } returns Unit

    val stage =
      spyk(
        TestStage(
          StageName.CLAIM,
          { _: StageName, _: LaunchStageIO -> "" },
          shouldThrow = !success,
          metricPublisher = metricPublisher,
        ),
      )
    val input = LaunchStageIO(msg = RecordFixtures.launcherInput("0"))
    stage.apply(input)

    verify {
      stage.applyStage(input)
      metricPublisher.count(
        WorkloadLauncherMetricMetadata.WORKLOAD_STAGE_START,
        *varargAny {
          it.equals(MetricAttribute(STAGE_NAME_TAG, StageName.CLAIM.name))
        },
      )
      metricPublisher.count(
        WorkloadLauncherMetricMetadata.WORKLOAD_STAGE_DONE,
        *varargAny {
          it.equals(MetricAttribute(STAGE_NAME_TAG, StageName.CLAIM.name))
        },
      )
      metricPublisher.count(
        WorkloadLauncherMetricMetadata.WORKLOAD_STAGE_DONE,
        *varargAny {
          it.equals(MetricAttribute(STATUS_TAG, if (success) "ok" else "error"))
        },
      )
    }
  }
}
