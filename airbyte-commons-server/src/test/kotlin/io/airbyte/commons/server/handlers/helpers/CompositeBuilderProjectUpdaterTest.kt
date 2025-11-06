/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import io.airbyte.api.model.generated.ExistingConnectorBuilderProjectWithWorkspaceId
import org.junit.Test
import org.junit.jupiter.api.DisplayName
import org.mockito.Mockito

class CompositeBuilderProjectUpdaterTest {
  @Test
  @DisplayName("updateConnectorBuilderProject should call updateConnectorBuilderProject on underlying updaters")
  fun testUpdateCompositeBuilderProjectUpdaterDelegates() {
    val update = Mockito.mock(ExistingConnectorBuilderProjectWithWorkspaceId::class.java)
    val updaterA = Mockito.mock(BuilderProjectUpdater::class.java)
    val updaterB = Mockito.mock(BuilderProjectUpdater::class.java)
    val projectUpdater = CompositeBuilderProjectUpdater(listOf(updaterA, updaterB))
    projectUpdater.persistBuilderProjectUpdate(update)

    Mockito
      .verify(updaterA, Mockito.times(1))
      .persistBuilderProjectUpdate(update)
    Mockito
      .verify(updaterB, Mockito.times(1))
      .persistBuilderProjectUpdate(update)
  }
}
