/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.DeclarativeSourceDefinitionsApi
import io.airbyte.api.model.generated.DeclarativeManifestsReadList
import io.airbyte.api.model.generated.DeclarativeSourceDefinitionCreateManifestRequestBody
import io.airbyte.api.model.generated.ListDeclarativeManifestsRequestBody
import io.airbyte.api.model.generated.UpdateActiveManifestRequestBody
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.handlers.DeclarativeSourceDefinitionsHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.server.apis.execute
import io.micronaut.context.annotation.Context
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.http.annotation.Status
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller("/api/v1/declarative_source_definitions")
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
class DeclarativeSourceDefinitionsApiController(
  private val handler: DeclarativeSourceDefinitionsHandler,
) : DeclarativeSourceDefinitionsApi {
  @Post(uri = "/create_manifest")
  @Status(HttpStatus.CREATED)
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun createDeclarativeSourceDefinitionManifest(
    @Body requestBody: DeclarativeSourceDefinitionCreateManifestRequestBody,
  ) {
    execute<Any?> {
      handler.createDeclarativeSourceDefinitionManifest(requestBody)
      null
    }
  }

  @Post(uri = "/update_active_manifest")
  @Status(HttpStatus.NO_CONTENT)
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun updateDeclarativeManifestVersion(
    @Body requestBody: UpdateActiveManifestRequestBody,
  ) {
    execute<Any?> {
      handler.updateDeclarativeManifestVersion(requestBody)
      null
    }
  }

  @Post(uri = "/list_manifests")
  @Status(HttpStatus.OK)
  @Secured(AuthRoleConstants.WORKSPACE_EDITOR, AuthRoleConstants.ORGANIZATION_EDITOR)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun listDeclarativeManifests(
    @Body requestBody: ListDeclarativeManifestsRequestBody,
  ): DeclarativeManifestsReadList? = execute { handler.listManifestVersions(requestBody) }
}
