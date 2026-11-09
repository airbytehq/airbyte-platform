/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api

import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.workload.auth.WorkloadCreateRule
import io.airbyte.workload.auth.WorkloadDataplaneListRule
import io.airbyte.workload.auth.WorkloadGlobalRule
import io.airbyte.workload.auth.WorkloadIdRule
import io.airbyte.workload.auth.WorkloadQueueGroupRule
import jakarta.ws.rs.Path
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test

class WorkloadRouteAuthorizationTest {
  @Test
  fun `every workload route declares a concrete protected rule`() {
    val routes = WorkloadApi::class.java.methods.filter { it.getAnnotation(Path::class.java) != null }
    val expectedRules =
      mapOf(
        "workloadCreate" to WorkloadCreateRule::class,
        "workloadFailure" to WorkloadIdRule::class,
        "workloadSuccess" to WorkloadIdRule::class,
        "workloadRunning" to WorkloadIdRule::class,
        "workloadCancel" to WorkloadIdRule::class,
        "workloadClaim" to WorkloadIdRule::class,
        "workloadLaunched" to WorkloadIdRule::class,
        "workloadGet" to WorkloadIdRule::class,
        "workloadLogUploadAuthorization" to WorkloadIdRule::class,
        "workloadHeartbeat" to WorkloadIdRule::class,
        "workloadList" to WorkloadDataplaneListRule::class,
        "workloadListWithExpiredDeadline" to WorkloadDataplaneListRule::class,
        "workloadListActive" to WorkloadDataplaneListRule::class,
        "workloadListOldNonSync" to WorkloadDataplaneListRule::class,
        "workloadListOldSync" to WorkloadDataplaneListRule::class,
        "pollWorkloadQueue" to WorkloadQueueGroupRule::class,
        "countWorkloadQueueDepth" to WorkloadQueueGroupRule::class,
        "getWorkloadQueueStats" to WorkloadGlobalRule::class,
        "workloadQueueClean" to WorkloadGlobalRule::class,
      )

    assertEquals(19, routes.size)
    assertEquals(expectedRules.keys, routes.map { it.name }.toSet())
    for (route in routes) {
      val authorization = route.getAnnotation(AuthorizeOperation::class.java)
      assertNotNull(authorization, "${route.name} has no operation rule")
      assertEquals(RouteAccessPolicy.PROTECTED, authorization.access, route.name)
      assertEquals(expectedRules.getValue(route.name), authorization.rule, route.name)
    }
  }
}
