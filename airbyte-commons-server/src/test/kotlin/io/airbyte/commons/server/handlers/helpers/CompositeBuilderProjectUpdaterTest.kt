/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import io.airbyte.api.model.generated.ExistingConnectorBuilderProjectWithWorkspaceId
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

internal class CompositeBuilderProjectUpdaterTest {
  @Test
  @DisplayName("updateConnectorBuilderProject should call updateConnectorBuilderProject on underlying updaters")
  fun testUpdateCompositeBuilderProjectUpdaterDelegates() {
    val update = mockk<ExistingConnectorBuilderProjectWithWorkspaceId>(relaxed = true)
    val updaterA = mockk<BuilderProjectUpdater>(relaxed = true)
    val updaterB = mockk<BuilderProjectUpdater>(relaxed = true)
    val projectUpdater = CompositeBuilderProjectUpdater(listOf(updaterA, updaterB))
    projectUpdater.persistBuilderProjectUpdate(update)

    verify(exactly = 1) { updaterA.persistBuilderProjectUpdate(update) }
    verify(exactly = 1) { updaterB.persistBuilderProjectUpdate(update) }
  }
}
