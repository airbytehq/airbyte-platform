/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.CatalogApi
import io.airbyte.api.model.generated.DiffCatalogsRequest
import io.airbyte.api.model.generated.DiffCatalogsResponse
import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.commons.server.services.CatalogDiffService
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.data.services.CatalogService
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.SourceService
import io.airbyte.metrics.lib.TracingHelper
import io.micronaut.context.annotation.Context
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import java.util.UUID

@Controller("/api/v1/catalogs/diff")
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
class CatalogApiController(
  private val catalogDiffService: CatalogDiffService,
  private val roleResolver: RoleResolver,
  private val catalogService: CatalogService,
  private val connectionService: ConnectionService,
  private val sourceService: SourceService,
) : CatalogApi {
  @Post
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun diffCatalogs(
    @Body diffCatalogsRequest: DiffCatalogsRequest,
  ): DiffCatalogsResponse {
    // Validate that connection_id is provided when persist_changes is true
    if (diffCatalogsRequest.persistChanges == true && diffCatalogsRequest.connectionId == null) {
      throw BadRequestProblem(
        ProblemMessageData().message("connection_id is required when persist_changes is true"),
      )
    }

    // Require WORKSPACE_EDITOR role when persisting changes, WORKSPACE_READER otherwise
    val requiredRole =
      if (diffCatalogsRequest.persistChanges == true) {
        AuthRoleConstants.WORKSPACE_EDITOR
      } else {
        AuthRoleConstants.WORKSPACE_READER
      }

    return withRoleValidation(
      currentCatalogId = diffCatalogsRequest.currentCatalogId,
      newCatalogId = diffCatalogsRequest.newCatalogId,
      connectionId = diffCatalogsRequest.connectionId,
      role = requiredRole,
    ) {
      diffCatalogsRequest.connectionId?.let { TracingHelper.addConnection(it) }
      catalogDiffService.diffCatalogs(diffCatalogsRequest)
    }
  }

  @InternalForTesting
  internal fun <T> withRoleValidation(
    currentCatalogId: UUID,
    newCatalogId: UUID,
    connectionId: UUID?,
    role: String,
    call: () -> T,
  ): T {
    val workspaceId: UUID

    // Priority 1: If connection ID is provided, use it for workspace resolution
    // This is the preferred path as it's more reliable and doesn't depend on ActorCatalogFetchEvent records
    if (connectionId != null) {
      workspaceId = resolveWorkspaceIdFromConnection(connectionId)

      // Validate both catalogs belong to the connection's source
      val connection =
        connectionService.getStandardSync(connectionId)
          ?: throw ForbiddenProblem(ProblemMessageData().message("User does not have the required permissions to access the resource(s)."))

      validateCatalogBelongsToSource(currentCatalogId, connection.sourceId)
      validateCatalogBelongsToSource(newCatalogId, connection.sourceId)
    } else {
      // Priority 2: Fall back to catalog-based resolution (for direct catalog diff API usage without connection context)
      val currentCatalogWorkspaceId = resolveWorkspaceIdFromCatalog(currentCatalogId)
      val newCatalogWorkspaceId = resolveWorkspaceIdFromCatalog(newCatalogId)

      // Ensure both catalogs belong to the same workspace
      if (currentCatalogWorkspaceId != newCatalogWorkspaceId) {
        throw ForbiddenProblem(ProblemMessageData().message("User does not have the required permissions to access the resource(s)."))
      }

      workspaceId = currentCatalogWorkspaceId
    }

    // Validate user has required role for the workspace
    roleResolver
      .newRequest()
      .withCurrentUser()
      .withRef(AuthenticationId.WORKSPACE_ID, workspaceId)
      .requireOneOfRoles(setOf(role))

    return call()
  }

  @InternalForTesting
  internal fun resolveWorkspaceIdFromConnection(connectionId: UUID): UUID {
    val connection =
      connectionService.getStandardSync(connectionId)
        ?: throw ForbiddenProblem(ProblemMessageData().message("User does not have the required permissions to access the resource(s)."))

    val source =
      sourceService.getSourceConnection(connection.sourceId)
        ?: throw ForbiddenProblem(ProblemMessageData().message("User does not have the required permissions to access the resource(s)."))

    return source.workspaceId
  }

  @InternalForTesting
  internal fun resolveWorkspaceIdFromCatalog(catalogId: UUID): UUID {
    // Catalog → ActorCatalogFetchEvent → Actor (Source) → Workspace
    val actorId =
      catalogService.getActorIdByCatalogId(catalogId).orElseThrow {
        ForbiddenProblem(ProblemMessageData().message("User does not have the required permissions to access the resource(s)."))
      }

    val source =
      sourceService.getSourceConnection(actorId)
        ?: throw ForbiddenProblem(ProblemMessageData().message("User does not have the required permissions to access the resource(s)."))

    return source.workspaceId
  }

  @InternalForTesting
  internal fun validateCatalogBelongsToSource(
    catalogId: UUID,
    sourceId: UUID,
  ) {
    // Try to find the catalog's actor via ActorCatalogFetchEvent (if exists)
    val catalogActorId = catalogService.getActorIdByCatalogId(catalogId)

    if (catalogActorId.isPresent) {
      // If fetch event exists, validate it matches the expected source
      if (catalogActorId.get() != sourceId) {
        throw ForbiddenProblem(
          ProblemMessageData().message("Catalog does not belong to the specified connection's source"),
        )
      }
    }
    // If no fetch event exists (new async flow), we trust the connection-based validation
    // The catalog was just discovered for this source, so it implicitly belongs to it
  }
}
