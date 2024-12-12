package io.airbyte.connector.rollout.worker.activities

import io.airbyte.api.model.generated.ConnectorRolloutStrategy
import io.airbyte.config.ConnectorEnumRolloutStrategy
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class ActivityHelperTest {
  @Test
  fun `test getRolloutStrategyFromInput`() {
    assertEquals(
      ConnectorRolloutStrategy.MANUAL.toString(),
      getRolloutStrategyFromInput(null).toString(),
    )

    assertEquals(
      ConnectorRolloutStrategy.MANUAL.toString(),
      getRolloutStrategyFromInput(ConnectorEnumRolloutStrategy.MANUAL).toString(),
    )

    assertEquals(
      ConnectorRolloutStrategy.AUTOMATED.toString(),
      getRolloutStrategyFromInput(ConnectorEnumRolloutStrategy.AUTOMATED).toString(),
    )
  }
}
