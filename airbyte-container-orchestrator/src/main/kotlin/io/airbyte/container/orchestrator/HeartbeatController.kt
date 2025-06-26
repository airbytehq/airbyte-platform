/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator

import io.micronaut.http.MediaType
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.Produces

private val response = mapOf("up" to true)

/**
 * Creates a controller that returns a 200 JSON response on any path requested.
 *
 *
 * This is intended to stay up as long as the Kube worker exists so pods spun up can check if the
 * spawning Kube worker still exists.
 */
@Controller
class HeartbeatController {
  @Get
  @Produces(MediaType.APPLICATION_JSON)
  fun get() = response
}
