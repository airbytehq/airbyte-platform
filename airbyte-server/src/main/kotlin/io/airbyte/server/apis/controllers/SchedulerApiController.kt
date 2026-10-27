/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.SchedulerApi
import io.airbyte.api.server.generated.models.CheckConnectionRead
import io.airbyte.api.server.generated.models.DestinationCoreConfig
import io.airbyte.api.server.generated.models.SourceCoreConfig
import io.airbyte.api.server.generated.models.SourceDiscoverSchemaRead
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller
@Secured(SecurityRule.IS_AUTHENTICATED)
open class SchedulerApiController(
  private val schedulerHandler: SchedulerHandler,
) : SchedulerApi {
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.SchedulerDestinationRule::class,
  )
  override fun executeDestinationCheckConnection(
    @Body destinationCoreConfig: DestinationCoreConfig,
  ): CheckConnectionRead =
    execute {
      schedulerHandler.checkDestinationConnectionFromDestinationCreate(
        destinationCoreConfig,
      )
    }

  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.SchedulerSourceRule::class,
  )
  override fun executeSourceCheckConnection(
    @Body sourceCoreConfig: SourceCoreConfig,
  ): CheckConnectionRead = execute { schedulerHandler.checkSourceConnectionFromSourceCreate(sourceCoreConfig) }

  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.SchedulerSourceRule::class,
  )
  override fun executeSourceDiscoverSchema(
    @Body sourceCoreConfig: SourceCoreConfig,
  ): SourceDiscoverSchemaRead =
    execute {
      schedulerHandler.discoverSchemaForSourceFromSourceCreate(
        sourceCoreConfig,
      )
    }
}
