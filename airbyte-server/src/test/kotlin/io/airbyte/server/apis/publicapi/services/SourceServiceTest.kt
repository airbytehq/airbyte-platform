/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.services

import io.airbyte.api.model.generated.PartialSourceUpdate
import io.airbyte.api.model.generated.SourceRead
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.airbyte.commons.server.handlers.SourceHandler
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.micronaut.runtime.AirbyteApiConfig
import io.airbyte.publicApi.server.generated.models.SourcePatchRequest
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

class SourceServiceTest {
  private lateinit var sourceService: SourceServiceImpl
  private val sourceHandler: SourceHandler = mockk()
  private val schedulerHandler: SchedulerHandler = mockk()
  private val currentUserService: CurrentUserService = mockk()
  private val userService: UserServiceImpl = mockk()

  private val sourceId = UUID.randomUUID()

  @BeforeEach
  fun setUp() {
    sourceService =
      SourceServiceImpl(
        userService = userService,
        sourceHandler = sourceHandler,
        schedulerHandler = schedulerHandler,
        currentUserService = currentUserService,
        airbyteApiConfig = AirbyteApiConfig(),
      )
  }

  @Test
  fun `partialUpdateSource with secretId calls updateSourceWithOptionalSecret`() {
    val secretId = "test-secret-id"
    val sourcePatchRequest =
      SourcePatchRequest(
        configuration = Jsons.deserialize("{}"),
        name = "updated-source",
        secretId = secretId,
      )

    val partialSourceUpdateSlot = slot<PartialSourceUpdate>()
    val sourceRead =
      SourceRead().apply {
        this.sourceId = this@SourceServiceTest.sourceId
        this.name = "updated-source"
        this.sourceDefinitionId = UUID.randomUUID()
        this.workspaceId = UUID.randomUUID()
        this.connectionConfiguration = Jsons.deserialize("{}")
        this.createdAt = 1L
      }

    every { sourceHandler.updateSourceWithOptionalSecret(capture(partialSourceUpdateSlot), allowInlineOAuthServerOutputSecrets = true) } returns
      sourceRead

    sourceService.partialUpdateSource(sourceId, sourcePatchRequest)

    verify(exactly = 1) { sourceHandler.updateSourceWithOptionalSecret(any(), allowInlineOAuthServerOutputSecrets = true) }
    verify(exactly = 0) { sourceHandler.partialUpdateSource(any<PartialSourceUpdate>()) }

    val captured = partialSourceUpdateSlot.captured
    assert(captured.sourceId == sourceId)
    assert(captured.secretId == secretId)
    assert(captured.name == "updated-source")
  }

  @Test
  fun `partialUpdateSource without secretId still calls updateSourceWithOptionalSecret`() {
    val inlineOAuthConfiguration = Jsons.jsonNode(mapOf(LWA_APP_ID_FIELD to "lwa-app-id"))
    val sourcePatchRequest =
      SourcePatchRequest(
        configuration = inlineOAuthConfiguration,
        name = "updated-source",
      )

    val partialSourceUpdateSlot = slot<PartialSourceUpdate>()
    val sourceRead =
      SourceRead().apply {
        this.sourceId = this@SourceServiceTest.sourceId
        this.name = "updated-source"
        this.sourceDefinitionId = UUID.randomUUID()
        this.workspaceId = UUID.randomUUID()
        this.connectionConfiguration = Jsons.deserialize("{}")
        this.createdAt = 1L
      }

    every { sourceHandler.updateSourceWithOptionalSecret(capture(partialSourceUpdateSlot), allowInlineOAuthServerOutputSecrets = true) } returns
      sourceRead

    sourceService.partialUpdateSource(sourceId, sourcePatchRequest)

    verify(exactly = 1) { sourceHandler.updateSourceWithOptionalSecret(any(), allowInlineOAuthServerOutputSecrets = true) }
    verify(exactly = 0) { sourceHandler.partialUpdateSource(any<PartialSourceUpdate>()) }

    val captured = partialSourceUpdateSlot.captured
    assert(captured.connectionConfiguration == inlineOAuthConfiguration)
  }

  private companion object {
    private const val LWA_APP_ID_FIELD = "lwa_app_id"
  }
}
