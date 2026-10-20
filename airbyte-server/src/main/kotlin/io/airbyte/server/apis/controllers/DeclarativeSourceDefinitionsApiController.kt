/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.server.generated.apis.DeclarativeSourceDefinitionsApi
import io.airbyte.api.server.generated.models.DeclarativeManifestsReadList
import io.airbyte.api.server.generated.models.DeclarativeSourceDefinitionCreateManifestRequestBody
import io.airbyte.api.server.generated.models.ListDeclarativeManifestsRequestBody
import io.airbyte.api.server.generated.models.UpdateActiveManifestRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.handlers.DeclarativeSourceDefinitionsHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.micronaut.context.annotation.Context
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Status
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
class DeclarativeSourceDefinitionsApiController(
  private val handler: DeclarativeSourceDefinitionsHandler,
) : DeclarativeSourceDefinitionsApi {
  @Status(HttpStatus.CREATED)
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.WORKSPACE_SOURCE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun createDeclarativeSourceDefinitionManifest(
    @Body requestBody: DeclarativeSourceDefinitionCreateManifestRequestBody,
  ) {
    execute<Any?> {
      handler.createDeclarativeSourceDefinitionManifest(requestBody)
      null
    }
  }

  @Status(HttpStatus.NO_CONTENT)
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.WORKSPACE_SOURCE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun updateDeclarativeManifestVersion(
    @Body requestBody: UpdateActiveManifestRequestBody,
  ) {
    execute<Any?> {
      handler.updateDeclarativeManifestVersion(requestBody)
      null
    }
  }

  @Status(HttpStatus.OK)
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.WORKSPACE_SOURCE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.LEGACY,
  )
  override fun listDeclarativeManifests(
    @Body requestBody: ListDeclarativeManifestsRequestBody,
  ): DeclarativeManifestsReadList = execute { handler.listManifestVersions(requestBody) }
}
