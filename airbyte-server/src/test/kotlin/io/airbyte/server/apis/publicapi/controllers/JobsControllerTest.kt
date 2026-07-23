/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.AuthenticatedUser
import io.airbyte.data.helpers.WorkspaceHelper
import io.airbyte.publicApi.server.generated.models.JobCreateRequest
import io.airbyte.publicApi.server.generated.models.JobResponse
import io.airbyte.publicApi.server.generated.models.JobTypeEnum
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.services.ConnectionService
import io.airbyte.server.apis.publicapi.services.JobService
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID
import java.util.concurrent.Callable

class JobsControllerTest {
  private lateinit var controller: JobsController
  private val jobService: JobService = mockk()
  private val connectionService: ConnectionService = mockk()
  private val trackingHelper: TrackingHelper = mockk(relaxed = true)
  private val roleResolver: RoleResolver = mockk(relaxed = true)
  private val currentUserService: CurrentUserService = mockk()
  private val workspaceHelper: WorkspaceHelper = mockk()

  @BeforeEach
  fun setUp() {
    every { currentUserService.getCurrentUser() } returns AuthenticatedUser().withUserId(UUID.randomUUID())
    every { trackingHelper.callWithTracker<Any>(any(), any(), any(), any()) } answers {
      (firstArg() as Callable<Any>).call()
    }

    controller =
      JobsController(
        jobService = jobService,
        connectionService = connectionService,
        trackingHelper = trackingHelper,
        roleResolver = roleResolver,
        currentUserService = currentUserService,
        workspaceHelper = workspaceHelper,
      )
  }

  @Test
  fun `public create sync job asks service to return after job id`() {
    val connectionId = UUID.randomUUID()
    val workspaceId = UUID.randomUUID()
    val organizationId = UUID.randomUUID()
    val jobResponse = mockk<JobResponse>()

    every { connectionService.getConnection(connectionId) } returns
      mockk {
        every { this@mockk.workspaceId } returns workspaceId.toString()
      }
    every { workspaceHelper.getOrganizationForWorkspace(workspaceId) } returns organizationId
    every { jobService.sync(connectionId, organizationId, returnAfterJobId = true) } returns jobResponse

    controller.publicCreateJob(
      JobCreateRequest(
        connectionId = connectionId.toString(),
        jobType = JobTypeEnum.SYNC,
      ),
    )

    verify { jobService.sync(connectionId, organizationId, returnAfterJobId = true) }
  }
}
