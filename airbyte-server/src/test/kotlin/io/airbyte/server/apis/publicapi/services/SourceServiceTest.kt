/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.services

import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.UnexpectedProblem
import io.airbyte.api.server.generated.models.FailureReason
import io.airbyte.api.server.generated.models.JobConfigType
import io.airbyte.api.server.generated.models.PartialSourceUpdate
import io.airbyte.api.server.generated.models.SourceDiscoverSchemaRead
import io.airbyte.api.server.generated.models.SourceRead
import io.airbyte.api.server.generated.models.SynchronousJobRead
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
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
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
      SourceRead(
        sourceId = sourceId,
        name = "updated-source",
        sourceDefinitionId = UUID.randomUUID(),
        workspaceId = UUID.randomUUID(),
        connectionConfiguration = Jsons.deserialize("{}"),
        sourceName = "source-definition",
        createdAt = 1L,
      )

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
      SourceRead(
        sourceId = sourceId,
        name = "updated-source",
        sourceDefinitionId = UUID.randomUUID(),
        workspaceId = UUID.randomUUID(),
        connectionConfiguration = Jsons.deserialize("{}"),
        sourceName = "source-definition",
        createdAt = 1L,
      )

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

  @Test
  fun `schema discovery hides internal failure messages including absent failure reason`() {
    for (failureReason in listOf(null, FailureReason(internalMessage = "internal connector stack trace", timestamp = 0L))) {
      every { schedulerHandler.discoverSchemaForSourceFromSourceId(any()) } returns
        SourceDiscoverSchemaRead(
          jobInfo =
            SynchronousJobRead(
              succeeded = false,
              failureReason = failureReason,
              id = UUID.randomUUID(),
              configType = JobConfigType.DISCOVER_SCHEMA,
              createdAt = 0L,
              endedAt = 0L,
            ),
        )

      val error = assertThrows<UnexpectedProblem> { sourceService.getSourceSchema(sourceId, false) }
      assertNull(error.problem.getData())
      assertEquals("The source connector failed while discovering its schema. Check the source configuration and retry.", error.problem.getDetail())
    }
  }

  @Test
  fun `schema discovery preserves connector user-facing failure messages`() {
    val externalMessage = "The configured account cannot read this table."
    every { schedulerHandler.discoverSchemaForSourceFromSourceId(any()) } returns
      SourceDiscoverSchemaRead(
        jobInfo =
          SynchronousJobRead(
            succeeded = false,
            failureReason = FailureReason(internalMessage = "secret internal text", externalMessage = externalMessage, timestamp = 0L),
            id = UUID.randomUUID(),
            configType = JobConfigType.DISCOVER_SCHEMA,
            createdAt = 0L,
            endedAt = 0L,
          ),
      )

    val error = assertThrows<UnexpectedProblem> { sourceService.getSourceSchema(sourceId, false) }
    assertEquals(externalMessage, (error.problem.getData() as ProblemMessageData).message)
  }
}
