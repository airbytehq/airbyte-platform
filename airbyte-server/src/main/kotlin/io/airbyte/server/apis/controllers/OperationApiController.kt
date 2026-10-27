/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.OperationApi
import io.airbyte.api.server.generated.models.CheckOperationRead
import io.airbyte.api.server.generated.models.ConnectionIdRequestBody
import io.airbyte.api.server.generated.models.OperationCreate
import io.airbyte.api.server.generated.models.OperationIdRequestBody
import io.airbyte.api.server.generated.models.OperationRead
import io.airbyte.api.server.generated.models.OperationReadList
import io.airbyte.api.server.generated.models.OperationUpdate
import io.airbyte.api.server.generated.models.OperatorConfiguration
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.handlers.OperationsHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.airbyte.server.auth.AuthenticatedMetadataRule
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Status
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller
@Secured(SecurityRule.IS_AUTHENTICATED)
open class OperationApiController(
  private val operationsHandler: OperationsHandler,
) : OperationApi {
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = AuthenticatedMetadataRule::class,
  )
  override fun checkOperation(
    @Body operatorConfiguration: OperatorConfiguration,
  ): CheckOperationRead = execute { operationsHandler.checkOperation(operatorConfiguration) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.SavedOperationWriteRule::class,
  )
  override fun createOperation(
    @Body operationCreate: OperationCreate,
  ): OperationRead = execute { operationsHandler.createOperation(operationCreate) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Status(
    HttpStatus.NO_CONTENT,
  )
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.SavedOperationWriteRule::class,
  )
  override fun deleteOperation(
    @Body operationIdRequestBody: OperationIdRequestBody,
  ) {
    execute<Any?> {
      operationsHandler.deleteOperation(operationIdRequestBody)
      null
    }
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.SavedOperationReadRule::class,
  )
  override fun getOperation(
    @Body operationIdRequestBody: OperationIdRequestBody,
  ): OperationRead = execute { operationsHandler.getOperation(operationIdRequestBody) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.SavedOperationListRule::class,
  )
  override fun listOperationsForConnection(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ): OperationReadList = execute { operationsHandler.listOperationsForConnection(connectionIdRequestBody) }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.SavedOperationWriteRule::class,
  )
  override fun updateOperation(
    @Body operationUpdate: OperationUpdate,
  ): OperationRead = execute { operationsHandler.updateOperation(operationUpdate) }
}
