/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.config.init
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ConnectorBuilderService
import io.mockk.mockk
import org.junit.jupiter.api.BeforeEach

internal class DeclarativeSourceUpdaterTest {
  private var mConnectorBuilderService: ConnectorBuilderService = mockk()
  private var mActorDefinitionService: ActorDefinitionService = mockk()
  private lateinit var declarativeSourceUpdater: DeclarativeSourceUpdater

  @BeforeEach
  fun setup() {
    declarativeSourceUpdater = DeclarativeSourceUpdater(mConnectorBuilderService, mActorDefinitionService)
  }
}
