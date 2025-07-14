/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.controllers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.api.model.generated.ConnectorBuilderProjectDetails
import io.airbyte.api.model.generated.ConnectorBuilderProjectIdWithWorkspaceId
import io.airbyte.api.model.generated.ConnectorBuilderProjectRead
import io.airbyte.api.model.generated.ConnectorBuilderProjectWithWorkspaceId
import io.airbyte.api.model.generated.ConnectorBuilderPublishRequestBody
import io.airbyte.api.model.generated.CustomDestinationDefinitionCreate
import io.airbyte.api.model.generated.CustomSourceDefinitionCreate
import io.airbyte.api.model.generated.DeclarativeSourceDefinitionCreateManifestRequestBody
import io.airbyte.api.model.generated.DeclarativeSourceManifest
import io.airbyte.api.model.generated.DestinationDefinitionCreate
import io.airbyte.api.model.generated.DestinationDefinitionRead
import io.airbyte.api.model.generated.DestinationDefinitionUpdate
import io.airbyte.api.model.generated.SourceDefinitionCreate
import io.airbyte.api.model.generated.SourceDefinitionRead
import io.airbyte.api.model.generated.SourceDefinitionUpdate
import io.airbyte.api.model.generated.WorkspaceIdActorDefinitionRequestBody
import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.constants.AirbyteCatalogConstants
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.ConnectorBuilderProjectsHandler
import io.airbyte.commons.server.handlers.DeclarativeSourceDefinitionsHandler
import io.airbyte.commons.server.handlers.DestinationDefinitionsHandler
import io.airbyte.commons.server.handlers.SourceDefinitionsHandler
import io.airbyte.commons.server.support.AuthenticationId
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.ConfigNotFoundType
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.config.DeclarativeManifest
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ConnectorBuilderService
import io.airbyte.publicApi.server.generated.apis.PublicDeclarativeSourceDefinitionsApi
import io.airbyte.publicApi.server.generated.apis.PublicDestinationDefinitionsApi
import io.airbyte.publicApi.server.generated.apis.PublicSourceDefinitionsApi
import io.airbyte.publicApi.server.generated.models.CreateDeclarativeSourceDefinitionRequest
import io.airbyte.publicApi.server.generated.models.CreateDefinitionRequest
import io.airbyte.publicApi.server.generated.models.DeclarativeSourceDefinitionResponse
import io.airbyte.publicApi.server.generated.models.DeclarativeSourceDefinitionsResponse
import io.airbyte.publicApi.server.generated.models.DefinitionResponse
import io.airbyte.publicApi.server.generated.models.DefinitionsResponse
import io.airbyte.publicApi.server.generated.models.UpdateDeclarativeSourceDefinitionRequest
import io.airbyte.publicApi.server.generated.models.UpdateDefinitionRequest
import io.airbyte.server.apis.publicapi.apiTracking.TrackingHelper
import io.airbyte.server.apis.publicapi.constants.API_PATH
import io.airbyte.server.apis.publicapi.errorHandlers.ConfigClientErrorHandler
import io.airbyte.server.apis.publicapi.services.UserService
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.http.BasicHttpAttributes
import io.micronaut.http.annotation.Controller
import io.micronaut.http.context.ServerRequestContext
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import jakarta.ws.rs.core.Response
import java.net.URI
import java.util.UUID

@Controller(API_PATH)
@Secured(SecurityRule.IS_AUTHENTICATED)
class DefinitionsController(
  val roleResolver: RoleResolver,
  val currentUserService: CurrentUserService,
  val sourceDefinitionsHandler: SourceDefinitionsHandler,
  val destinationDefinitionsHandler: DestinationDefinitionsHandler,
  val userService: UserService,
  val trackingHelper: TrackingHelper,
  val connectorBuilderProjectsHandler: ConnectorBuilderProjectsHandler,
  val connectorBuilderService: ConnectorBuilderService,
  val declarativeSourceDefinitionsHandler: DeclarativeSourceDefinitionsHandler,
  val airbyteEdition: AirbyteEdition,
) : PublicSourceDefinitionsApi,
  PublicDestinationDefinitionsApi,
  PublicDeclarativeSourceDefinitionsApi {
  override fun publicCreateSourceDefinition(
    workspaceId: UUID,
    createDefinitionRequest: CreateDefinitionRequest,
  ) = wrap {
    if (airbyteEdition == AirbyteEdition.CLOUD) {
      throw BadRequestProblem(
        ProblemMessageData().message("Non-declarative definitions cannot be created or updated in Airbyte Cloud."),
      )
    }
    ensureUserCanWrite(workspaceId)

    sourceDefinitionsHandler
      .createCustomSourceDefinition(
        createDefinitionRequest.toCustomSourceDefinitionCreate(workspaceId),
      ).toPublicApiModel()
      .ok()
  }

  override fun publicCreateDeclarativeSourceDefinition(
    workspaceId: UUID,
    request: CreateDeclarativeSourceDefinitionRequest,
  ) = wrap {
    ensureUserCanWrite(workspaceId)

    val manifest: JsonNode = ObjectMapper().valueToTree(request.manifest)

    // The "manifest" field contains the "spec", but it has a snake_case connection_specification
    // and the platform code needs camelCase connectionSpecification.
    val spec = Jsons.clone(manifest.get("spec")) as ObjectNode
    spec.replace("connectionSpecification", spec.get("connection_specification"))

    val res =
      connectorBuilderProjectsHandler.createConnectorBuilderProject(
        ConnectorBuilderProjectWithWorkspaceId()
          .workspaceId(workspaceId)
          .builderProject(
            ConnectorBuilderProjectDetails()
              .name(request.name)
              .draftManifest(manifest),
          ),
      )

    connectorBuilderProjectsHandler.publishConnectorBuilderProject(
      ConnectorBuilderPublishRequestBody()
        .builderProjectId(res.builderProjectId)
        .workspaceId(res.workspaceId)
        .name(request.name)
        .initialDeclarativeManifest(
          DeclarativeSourceManifest()
            .manifest(manifest)
            .spec(spec)
            .description("")
            .version(1),
        ),
    )

    connectorBuilderProjectsHandler
      .getConnectorBuilderProjectWithManifest(
        ConnectorBuilderProjectIdWithWorkspaceId()
          .workspaceId(workspaceId)
          .builderProjectId(res.builderProjectId)
          .version(1),
      ).toPublicApi()
      .ok()
  }

  override fun publicCreateDestinationDefinition(
    workspaceId: UUID,
    createDefinitionRequest: CreateDefinitionRequest,
  ) = wrap {
    if (airbyteEdition == AirbyteEdition.CLOUD) {
      throw BadRequestProblem(
        ProblemMessageData().message("Non-declarative definitions cannot be created or updated in Airbyte Cloud."),
      )
    }
    ensureUserCanWrite(workspaceId)

    destinationDefinitionsHandler
      .createCustomDestinationDefinition(
        createDefinitionRequest.toCustomDestinationDefinitionCreate(workspaceId),
      ).toPublicApiModel()
      .ok()
  }

  override fun publicDeleteSourceDefinition(
    workspaceId: UUID,
    definitionId: UUID,
  ) = wrap {
    val def = sourceDefinitionsHandler.getSourceDefinition(definitionId, false)
    // Don't allow deleting declarative source definitions via this endpoint.
    if (def.dockerRepository == AirbyteCatalogConstants.AIRBYTE_SOURCE_DECLARATIVE_MANIFEST_IMAGE) {
      throw ConfigNotFoundException(ConfigNotFoundType.STANDARD_SOURCE_DEFINITION, definitionId.toString())
    }
    ensureUserCanWrite(workspaceId, def.custom)

    sourceDefinitionsHandler.deleteSourceDefinition(definitionId)
    def.toPublicApiModel().ok()
  }

  override fun publicDeleteDeclarativeSourceDefinition(
    workspaceId: UUID,
    definitionId: UUID,
  ) = wrap {
    ensureUserCanWrite(workspaceId)

    val def = sourceDefinitionsHandler.getSourceDefinition(definitionId, false)
    val proj = connectorBuilderService.getConnectorBuilderProjectIdForActorDefinitionId(definitionId)
    if (proj.isEmpty) {
      throw ConfigNotFoundException(ConfigNotFoundType.STANDARD_SOURCE_DEFINITION, definitionId.toString())
    }
    val projId = proj.get()

    val response =
      connectorBuilderService
        .getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(definitionId)
        .toPublicApi(def.name)
        .ok()
    connectorBuilderService.deleteBuilderProject(projId)
    sourceDefinitionsHandler.deleteSourceDefinition(definitionId)

    return@wrap response
  }

  override fun publicDeleteDestinationDefinition(
    workspaceId: UUID,
    definitionId: UUID,
  ) = wrap {
    val def = destinationDefinitionsHandler.getDestinationDefinition(definitionId, false)
    ensureUserCanWrite(workspaceId, def.custom)
    destinationDefinitionsHandler.deleteDestinationDefinition(definitionId)
    def.toPublicApiModel().ok()
  }

  override fun publicGetSourceDefinition(
    workspaceId: UUID,
    definitionId: UUID,
  ) = wrap {
    ensureUserCanRead(workspaceId)

    // Use the listSourceDefinitionsForWorkspace function so that version pins, entitlements,
    // and other details are taken into account.
    val defs =
      sourceDefinitionsHandler
        .listSourceDefinitionsForWorkspace(WorkspaceIdActorDefinitionRequestBody().workspaceId(workspaceId))
        .sourceDefinitions

    val def = defs.firstOrNull { it.sourceDefinitionId == definitionId }
    if (def == null) {
      throw ConfigNotFoundException(ConfigNotFoundType.STANDARD_SOURCE_DEFINITION, definitionId.toString())
    }

    // Don't allow reading declarative source definitions via this endpoint.
    if (def.dockerRepository == AirbyteCatalogConstants.AIRBYTE_SOURCE_DECLARATIVE_MANIFEST_IMAGE) {
      throw ConfigNotFoundException(ConfigNotFoundType.STANDARD_SOURCE_DEFINITION, definitionId.toString())
    }

    def.toPublicApiModel().ok()
  }

  override fun publicGetDeclarativeSourceDefinition(
    workspaceId: UUID,
    definitionId: UUID,
  ) = wrap {
    ensureUserCanRead(workspaceId)

    val def = sourceDefinitionsHandler.getSourceDefinition(definitionId, false)
    connectorBuilderService
      .getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(definitionId)
      .toPublicApi(def.name)
      .ok()
  }

  override fun publicGetDestinationDefinition(
    workspaceId: UUID,
    definitionId: UUID,
  ) = wrap {
    ensureUserCanRead(workspaceId)

    // Use the listDestinationDefinitionsForWorkspace function so that version pins, entitlements,
    // and other details are taken into account.
    val defs =
      destinationDefinitionsHandler
        .listDestinationDefinitionsForWorkspace(WorkspaceIdActorDefinitionRequestBody().workspaceId(workspaceId))
        .destinationDefinitions

    val def = defs.firstOrNull { it.destinationDefinitionId == definitionId }
    if (def == null) {
      throw ConfigNotFoundException(ConfigNotFoundType.STANDARD_DESTINATION_DEFINITION, definitionId.toString())
    }

    def.toPublicApiModel().ok()
  }

  override fun publicListSourceDefinitions(workspaceId: UUID) =
    wrap {
      ensureUserCanRead(workspaceId)

      DefinitionsResponse(
        sourceDefinitionsHandler
          .listSourceDefinitionsForWorkspace(WorkspaceIdActorDefinitionRequestBody().workspaceId(workspaceId))
          .sourceDefinitions
          .filter { it.dockerRepository != AirbyteCatalogConstants.AIRBYTE_SOURCE_DECLARATIVE_MANIFEST_IMAGE }
          .map { it.toPublicApiModel() },
      ).ok()
    }

  override fun publicListDeclarativeSourceDefinitions(workspaceId: UUID) =
    wrap {
      ensureUserCanRead(workspaceId)

      DeclarativeSourceDefinitionsResponse(
        sourceDefinitionsHandler
          .listSourceDefinitionsForWorkspace(WorkspaceIdActorDefinitionRequestBody().workspaceId(workspaceId))
          .sourceDefinitions
          .filter { it.dockerRepository == AirbyteCatalogConstants.AIRBYTE_SOURCE_DECLARATIVE_MANIFEST_IMAGE }
          .map {
            val manifest = connectorBuilderService.getCurrentlyActiveDeclarativeManifestsByActorDefinitionId(it.sourceDefinitionId)
            DeclarativeSourceDefinitionResponse(
              id = it.sourceDefinitionId.toString(),
              name = it.name,
              manifest = manifest.manifest,
              version = manifest.version,
            )
          },
      ).ok()
    }

  override fun publicListDestinationDefinitions(workspaceId: UUID) =
    wrap {
      ensureUserCanRead(workspaceId)

      DefinitionsResponse(
        destinationDefinitionsHandler
          .listDestinationDefinitionsForWorkspace(WorkspaceIdActorDefinitionRequestBody().workspaceId(workspaceId))
          .destinationDefinitions
          .map { it.toPublicApiModel() },
      ).ok()
    }

  override fun publicUpdateSourceDefinition(
    workspaceId: UUID,
    definitionId: UUID,
    updateDefinitionRequest: UpdateDefinitionRequest,
  ) = wrap {
    if (airbyteEdition == AirbyteEdition.CLOUD) {
      throw BadRequestProblem(
        ProblemMessageData().message("Non-declarative definitions cannot be created or updated in Airbyte Cloud."),
      )
    }

    val def = sourceDefinitionsHandler.getSourceDefinition(definitionId, false)
    ensureUserCanWrite(workspaceId, def.custom)

    sourceDefinitionsHandler
      .updateSourceDefinition(
        SourceDefinitionUpdate()
          .sourceDefinitionId(definitionId)
          .name(updateDefinitionRequest.name)
          .dockerImageTag(updateDefinitionRequest.dockerImageTag)
          .workspaceId(workspaceId),
      ).toPublicApiModel()
      .ok()
  }

  override fun publicUpdateDeclarativeSourceDefinition(
    workspaceId: UUID,
    definitionId: UUID,
    request: UpdateDeclarativeSourceDefinitionRequest,
  ) = wrap {
    val proj = connectorBuilderService.getConnectorBuilderProjectIdForActorDefinitionId(definitionId)
    if (proj.isEmpty) {
      throw ConfigNotFoundException(ConfigNotFoundType.STANDARD_SOURCE_DEFINITION, definitionId.toString())
    }
    val projId = proj.get()

    ensureUserCanWrite(workspaceId)

    val maxVersion =
      connectorBuilderService
        .getDeclarativeManifestsByActorDefinitionId(definitionId)
        .toList()
        .maxOf { it.version }
    val nextVersion = maxVersion + 1

    val manifest: JsonNode = ObjectMapper().valueToTree(request.manifest)

    // The "manifest" field contains the "spec", but it has a snake_case connection_specification
    // and the platform code needs camelCase connectionSpecification.
    val spec = Jsons.clone(manifest.get("spec")) as ObjectNode
    spec.replace("connectionSpecification", spec.get("connection_specification"))

    declarativeSourceDefinitionsHandler.createDeclarativeSourceDefinitionManifest(
      DeclarativeSourceDefinitionCreateManifestRequestBody()
        .sourceDefinitionId(definitionId)
        .workspaceId(workspaceId)
        .setAsActiveManifest(true)
        .declarativeManifest(
          DeclarativeSourceManifest()
            .manifest(manifest)
            .spec(spec)
            .description("")
            .version(nextVersion),
        ),
    )

    connectorBuilderProjectsHandler
      .getConnectorBuilderProjectWithManifest(
        ConnectorBuilderProjectIdWithWorkspaceId()
          .workspaceId(workspaceId)
          .builderProjectId(projId)
          .version(nextVersion),
      ).toPublicApi()
      .ok()
  }

  override fun publicUpdateDestinationDefinition(
    workspaceId: UUID,
    definitionId: UUID,
    updateDefinitionRequest: UpdateDefinitionRequest,
  ) = wrap {
    if (airbyteEdition == AirbyteEdition.CLOUD) {
      throw BadRequestProblem(
        ProblemMessageData().message("Non-declarative definitions cannot be created or updated in Airbyte Cloud."),
      )
    }
    val def = destinationDefinitionsHandler.getDestinationDefinition(definitionId, false)
    ensureUserCanWrite(workspaceId, def.custom)

    destinationDefinitionsHandler
      .updateDestinationDefinition(
        DestinationDefinitionUpdate()
          .destinationDefinitionId(definitionId)
          .name(updateDefinitionRequest.name)
          .dockerImageTag(updateDefinitionRequest.dockerImageTag)
          .workspaceId(workspaceId),
      ).toPublicApiModel()
      .ok()
  }

  // wrap controller endpoints in common functionality: segment tracking, error conversion, etc.
  private fun wrap(block: () -> Response): Response {
    val currentRequest = ServerRequestContext.currentRequest<Any>().get()
    val template = BasicHttpAttributes.getUriTemplate(currentRequest).orElse(currentRequest.path)
    val method = currentRequest.method.name

    val userId: UUID = currentUserService.getCurrentUser().userId
    val res: Response =
      trackingHelper.callWithTracker({
        try {
          return@callWithTracker block()
        } catch (e: Exception) {
          logger.error(e) { "Failed to call `${currentRequest.path}`" }
          ConfigClientErrorHandler.handleError(e)
          // handleError() above should always throw an exception,
          // but if it doesn't, return an unknown server error.
          return@callWithTracker Response.serverError().build()
        }
      }, template, method, userId)
    trackingHelper.trackSuccess(template, method, userId)
    return res
  }

  private fun ensureUserCanRead(workspaceId: UUID) {
    roleResolver
      .newRequest()
      .withCurrentUser()
      .withRef(AuthenticationId.WORKSPACE_ID, workspaceId)
      .requireRole(AuthRoleConstants.WORKSPACE_READER)
  }

  private fun ensureUserCanWrite(
    workspaceId: UUID,
    isCustom: Boolean = true,
  ) {
    if (!isCustom) {
      throw BadRequestProblem(
        ProblemMessageData().message("Public definitions cannot be modified."),
      )
    }

    roleResolver
      .newRequest()
      .withCurrentUser()
      .withRef(AuthenticationId.WORKSPACE_ID, workspaceId)
      .requireRole(AuthRoleConstants.WORKSPACE_EDITOR)
  }
}

private val logger = KotlinLogging.logger {}

private fun <T> T.ok() = Response.status(Response.Status.OK.statusCode).entity(this).build()

private fun SourceDefinitionRead.toPublicApiModel(): DefinitionResponse =
  DefinitionResponse(
    id = this.sourceDefinitionId.toString(),
    name = this.name,
    dockerRepository = this.dockerRepository,
    dockerImageTag = this.dockerImageTag,
    documentationUrl = this.documentationUrl.toString(),
  )

private fun DestinationDefinitionRead.toPublicApiModel(): DefinitionResponse =
  DefinitionResponse(
    id = this.destinationDefinitionId.toString(),
    name = this.name,
    dockerRepository = this.dockerRepository,
    dockerImageTag = this.dockerImageTag,
    documentationUrl = this.documentationUrl.toString(),
  )

private fun CreateDefinitionRequest.toCustomSourceDefinitionCreate(workspaceId: UUID) =
  CustomSourceDefinitionCreate()
    .workspaceId(workspaceId)
    .sourceDefinition(
      SourceDefinitionCreate()
        .name(this.name)
        .dockerImageTag(this.dockerImageTag)
        .dockerRepository(this.dockerRepository)
        .documentationUrl(this.documentationUrl ?: URI.create("")),
    )

private fun CreateDefinitionRequest.toCustomDestinationDefinitionCreate(workspaceId: UUID) =
  CustomDestinationDefinitionCreate()
    .workspaceId(workspaceId)
    .destinationDefinition(
      DestinationDefinitionCreate()
        .name(this.name)
        .dockerImageTag(this.dockerImageTag)
        .dockerRepository(this.dockerRepository)
        .documentationUrl(this.documentationUrl ?: URI.create("")),
    )

private fun ConnectorBuilderProjectRead.toPublicApi() =
  DeclarativeSourceDefinitionResponse(
    id = builderProject.sourceDefinitionId.toString(),
    name = builderProject.name,
    manifest = declarativeManifest.manifest,
    version = builderProject.activeDeclarativeManifestVersion,
  )

private fun DeclarativeManifest.toPublicApi(name: String) =
  DeclarativeSourceDefinitionResponse(
    id = actorDefinitionId.toString(),
    name = name,
    manifest = manifest,
    version = version,
  )
