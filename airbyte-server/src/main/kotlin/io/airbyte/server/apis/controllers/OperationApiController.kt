/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.OperationApi
import io.airbyte.api.model.generated.CheckOperationRead
import io.airbyte.api.model.generated.ConnectionIdRequestBody
import io.airbyte.api.model.generated.OperationCreate
import io.airbyte.api.model.generated.OperationIdRequestBody
import io.airbyte.api.model.generated.OperationRead
import io.airbyte.api.model.generated.OperationReadList
import io.airbyte.api.model.generated.OperationUpdate
import io.airbyte.api.model.generated.OperatorConfiguration
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.handlers.OperationsHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.http.annotation.Status
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller("/api/v1/operations")
@Secured(SecurityRule.IS_AUTHENTICATED)
open class OperationApiController(
  private val operationsHandler: OperationsHandler,
) : OperationApi {
  @Post("/check")
  @Secured(AuthRoleConstants.AUTHENTICATED_USER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun checkOperation(
    @Body operatorConfiguration: OperatorConfiguration,
  ): CheckOperationRead? = execute { operationsHandler.checkOperation(operatorConfiguration) }

  @Post("/create")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun createOperation(
    @Body operationCreate: OperationCreate,
  ): OperationRead? = execute { operationsHandler.createOperation(operationCreate) }

  @Post("/delete")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Status(
    HttpStatus.NO_CONTENT,
  )
  override fun deleteOperation(
    @Body operationIdRequestBody: OperationIdRequestBody,
  ) {
    execute<Any?> {
      operationsHandler.deleteOperation(operationIdRequestBody)
      null
    }
  }

  @Post("/get")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getOperation(
    @Body operationIdRequestBody: OperationIdRequestBody,
  ): OperationRead? = execute { operationsHandler.getOperation(operationIdRequestBody) }

  @Post("/list")
  @Secured(AuthRoleConstants.WORKSPACE_READER, AuthRoleConstants.ORGANIZATION_READER)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listOperationsForConnection(
    @Body connectionIdRequestBody: ConnectionIdRequestBody,
  ): OperationReadList? = execute { operationsHandler.listOperationsForConnection(connectionIdRequestBody) }

  @Post("/update")
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun updateOperation(
    @Body operationUpdate: OperationUpdate,
  ): OperationRead? = execute { operationsHandler.updateOperation(operationUpdate) }
}
