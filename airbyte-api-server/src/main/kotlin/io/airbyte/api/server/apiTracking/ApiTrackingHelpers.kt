/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.api.server.apiTracking

import io.airbyte.analytics.Deployment
import io.airbyte.analytics.TrackingClientSingleton
import io.airbyte.commons.version.AirbyteVersion
import io.airbyte.config.Configs
import org.slf4j.LoggerFactory
import java.util.Optional
import java.util.UUID

/**
 * This class builds upon inherited tracking clients from OSS Airbyte.
 * <p>
 * The {@link io.airbyte.analytics.LoggingTrackingClient} is pretty straightforward.
 * <p>
 * The {@link io.airbyte.analytics.SegmentTrackingClient} is where things get slightly confusing.
 * <p>
 * The Segment client expects an initialisation call to be made via the setupTrackingClient(UUID,
 * AirbyteVersion, TrackingStrategy, Database) method.
 * <p>
 */
private val log = LoggerFactory.getLogger("ApiTrackingHelpers.kt")

// Operation names
private val AIRBYTE_API_CALL = "Airbyte_API_Call"

private val USER_ID = "user_id"
private val ENDPOINT = "endpoint"
private val OPERATION = "operation"
private val STATUS_CODE = "status_code"
private val WORKSPACE = "workspace"

fun setupTrackingClient(
  airbyteVersion: AirbyteVersion,
  deploymentMode: Configs.DeploymentMode,
  trackingStrategy: Configs.TrackingStrategy,
  workerEnvironment: Configs.WorkerEnvironment,
) {
  log.debug("deployment mode: $deploymentMode")
  log.debug("airbyte version: $airbyteVersion")
  log.debug("tracking strategy: $trackingStrategy")
  log.debug("worker environment: $workerEnvironment")

  // fake a deployment UUID until we want to have the public api server talking to the database
  // directly
  val deploymentId = UUID.randomUUID()
  log.info("setting up tracking client with deploymentId: $deploymentId")
  TrackingClientSingleton.initializeWithoutDatabase(
    trackingStrategy,
    Deployment(
      deploymentMode,
      deploymentId,
      workerEnvironment,
    ),
    airbyteVersion,
  )
}

fun track(
  userId: UUID?,
  endpointPath: String?,
  httpOperation: String?,
  httpStatusCode: Int,
  workspaceId: Optional<UUID>,
) {
  val payload = mutableMapOf(
    Pair(USER_ID, userId),
    Pair(ENDPOINT, endpointPath),
    Pair(OPERATION, httpOperation),
    Pair(STATUS_CODE, httpStatusCode),
  )
  if (workspaceId.isPresent) {
    payload[WORKSPACE] = workspaceId.get().toString()
  }
  TrackingClientSingleton.get().track(
    userId,
    AIRBYTE_API_CALL,
    payload as Map<String, Any>?,
  )
}
