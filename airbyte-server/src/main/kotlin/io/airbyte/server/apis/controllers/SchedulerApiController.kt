/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.SchedulerApi
import io.airbyte.api.model.generated.CheckConnectionRead
import io.airbyte.api.model.generated.DestinationCoreConfig
import io.airbyte.api.model.generated.SourceCoreConfig
import io.airbyte.api.model.generated.SourceDiscoverSchemaRead
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.handlers.SchedulerHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller("/api/v1/scheduler")
@Secured(SecurityRule.IS_AUTHENTICATED)
open class SchedulerApiController(
  private val schedulerHandler: SchedulerHandler,
) : SchedulerApi {
  @Post("/destinations/check_connection")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  override fun executeDestinationCheckConnection(
    @Body destinationCoreConfig: DestinationCoreConfig,
  ): CheckConnectionRead? =
    execute {
      schedulerHandler.checkDestinationConnectionFromDestinationCreate(
        destinationCoreConfig,
      )
    }

  @Post("/sources/check_connection")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  override fun executeSourceCheckConnection(
    @Body sourceCoreConfig: SourceCoreConfig,
  ): CheckConnectionRead? = execute { schedulerHandler.checkSourceConnectionFromSourceCreate(sourceCoreConfig) }

  @Post("/sources/discover_schema")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.SCHEDULER)
  override fun executeSourceDiscoverSchema(
    @Body sourceCoreConfig: SourceCoreConfig,
  ): SourceDiscoverSchemaRead? =
    execute {
      schedulerHandler.discoverSchemaForSourceFromSourceCreate(
        sourceCoreConfig,
      )
    }
}
