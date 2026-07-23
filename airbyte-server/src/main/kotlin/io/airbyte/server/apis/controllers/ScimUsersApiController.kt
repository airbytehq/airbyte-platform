/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.scim.ScimPatchRequest
import io.airbyte.api.scim.ScimUserRequest
import io.airbyte.api.scim.generated.apis.ScimUsersApi
import io.airbyte.api.scim.generated.models.ScimUser
import io.airbyte.api.scim.generated.models.ScimUserListResponse
import io.airbyte.domain.models.scim.ScimUserConflictException
import io.airbyte.domain.models.scim.ScimUserFilterAttribute
import io.airbyte.domain.models.scim.ScimUserFilterClause
import io.airbyte.domain.models.scim.ScimUserNotFoundException
import io.airbyte.domain.services.scim.ScimAuthenticationContext
import io.airbyte.domain.services.scim.ScimMutationService
import io.airbyte.domain.services.scim.ScimUserLifecycleService
import io.airbyte.server.scim.SCIM_LIST_RESPONSE_SCHEMA
import io.airbyte.server.scim.SCIM_MEDIA_TYPE
import io.airbyte.server.scim.ScimErrors
import io.airbyte.server.scim.ScimFilter
import io.airbyte.server.scim.ScimFilterParser
import io.airbyte.server.scim.ScimPagination
import io.airbyte.server.scim.ScimPatchProcessor
import io.airbyte.server.scim.ScimUserRequestParser
import io.airbyte.server.scim.ScimUserResourceService
import io.airbyte.server.scim.scimAuthenticationContext
import io.airbyte.server.scim.scimCreated
import io.airbyte.server.scim.scimDeleted
import io.airbyte.server.scim.scimUpdated
import io.micronaut.context.annotation.Value
import io.micronaut.http.HttpHeaders
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.MutableHttpResponse
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.context.ServerRequestContext
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule
import java.net.URI
import java.util.UUID

@Controller
@Secured(SecurityRule.IS_ANONYMOUS)
open class ScimUsersApiController(
  private val lifecycleService: ScimUserLifecycleService,
  private val mutationService: ScimMutationService,
  private val resourceService: ScimUserResourceService,
  @param:Value("\${airbyte.airbyte-url:}") private val configuredBaseUrl: String = "",
) : ScimUsersApi {
  override fun createUser(
    @Body ioAirbyteApiScimScimUserRequest: ScimUserRequest,
    attributes: String?,
    excludedAttributes: String?,
  ): MutableHttpResponse<ScimUser> {
    val request = currentRequest()
    val context = request.scimAuthenticationContext()
    val input = ScimUserRequestParser.parse(ioAirbyteApiScimScimUserRequest.body)
    val projection = resourceService.compileProjection(attributes, excludedAttributes)
    val user =
      translateDomain { mutationService.execute(context) { lifecycleService.create(context.configurationId, context.organizationId.value, input) } }
    val location = resourceService.canonicalUserLocation(canonicalBaseUri(request), user.id.toString())
    return scimCreated(resourceService.render(user, canonicalBaseUri(request), projection), location)
  }

  override fun deleteUser(id: UUID): MutableHttpResponse<*> {
    val context = authenticationContext()
    translateDomain {
      mutationService.execute(context) {
        lifecycleService.delete(context.configurationId, context.organizationId.value, id)
      }
    }
    return scimDeleted()
  }

  override fun getUser(
    id: UUID,
    attributes: String?,
    excludedAttributes: String?,
  ): MutableHttpResponse<ScimUser> {
    val context = authenticationContext()
    val projection = resourceService.compileProjection(attributes, excludedAttributes)
    val user = translateDomain { lifecycleService.get(context.configurationId, context.organizationId.value, id) }
    return scimUpdated(resourceService.render(user, canonicalBaseUri(currentRequest()), projection))
  }

  override fun listUsers(
    filter: String?,
    startIndex: Int?,
    count: Int?,
    attributes: String?,
    excludedAttributes: String?,
  ): MutableHttpResponse<ScimUserListResponse> {
    val context = authenticationContext()
    val projection = resourceService.compileProjection(attributes, excludedAttributes)
    val filters = filter?.let { ScimFilterParser.parseUser(it).clauses.map(::toDomainFilter) }.orEmpty()
    val normalizedStartIndex = (startIndex ?: 1).coerceAtLeast(1)
    val normalizedCount = (count ?: ScimPagination.DEFAULT_COUNT).coerceIn(0, ScimPagination.MAX_COUNT)
    val page =
      lifecycleService.list(
        context.configurationId,
        context.organizationId.value,
        filters,
        normalizedStartIndex.toLong() - 1,
        normalizedCount,
      )
    val enrichedUsers =
      if (page.resources.isEmpty()) {
        emptyList()
      } else {
        lifecycleService.enrichGroups(context.configurationId, context.organizationId.value, page.resources)
      }
    val baseUri = canonicalBaseUri(currentRequest())
    val response =
      ScimUserListResponse(
        schemas = listOf(SCIM_LIST_RESPONSE_SCHEMA),
        totalResults = page.totalResults,
        resources = enrichedUsers.map { resourceService.render(it, baseUri, projection) },
        startIndex = normalizedStartIndex,
        itemsPerPage = enrichedUsers.size,
      )
    return HttpResponse.ok(response).contentType(SCIM_MEDIA_TYPE)
  }

  override fun patchUser(
    id: UUID,
    @Body ioAirbyteApiScimScimPatchRequest: ScimPatchRequest,
    attributes: String?,
    excludedAttributes: String?,
  ): MutableHttpResponse<ScimUser> {
    val request = currentRequest()
    val context = request.scimAuthenticationContext()
    val baseUri = canonicalBaseUri(request)
    val projection = resourceService.compileProjection(attributes, excludedAttributes)
    ScimPatchProcessor.validateUserPatch(ioAirbyteApiScimScimPatchRequest.body)
    val user =
      translateDomain {
        mutationService.execute(context) {
          val current = lifecycleService.get(context.configurationId, context.organizationId.value, id)
          val patch = ScimPatchProcessor.applyUser(resourceService.completeResource(current, baseUri), ioAirbyteApiScimScimPatchRequest.body)
          lifecycleService.patch(
            context.configurationId,
            context.organizationId.value,
            id,
            ScimUserRequestParser.parse(patch.resource),
            patch.activeTransitions.map { it.to },
          )
        }
      }
    return scimUpdated(resourceService.render(user, baseUri, projection))
  }

  override fun replaceUser(
    id: UUID,
    @Body ioAirbyteApiScimScimUserRequest: ScimUserRequest,
    attributes: String?,
    excludedAttributes: String?,
  ): MutableHttpResponse<ScimUser> {
    val request = currentRequest()
    val context = request.scimAuthenticationContext()
    val input = ScimUserRequestParser.parse(ioAirbyteApiScimScimUserRequest.body)
    val projection = resourceService.compileProjection(attributes, excludedAttributes)
    val user =
      translateDomain {
        mutationService.execute(context) {
          lifecycleService.replace(context.configurationId, context.organizationId.value, id, input)
        }
      }
    return scimUpdated(resourceService.render(user, canonicalBaseUri(request), projection))
  }

  private fun authenticationContext(): ScimAuthenticationContext = currentRequest().scimAuthenticationContext()

  private fun currentRequest(): HttpRequest<*> =
    ServerRequestContext.currentRequest<Any>().orElseThrow { IllegalStateException("SCIM request context is missing") }

  private fun canonicalBaseUri(request: HttpRequest<*>): URI {
    if (configuredBaseUrl.isNotBlank()) {
      return URI.create("${configuredBaseUrl.trimEnd('/')}/")
    }
    if (request.uri.isAbsolute) {
      return URI(request.uri.scheme, request.uri.authority, "/", null, null)
    }
    val forwardedProtocol = request.headers["X-Forwarded-Proto"]?.substringBefore(',')?.trim()
    val forwardedHost = request.headers["X-Forwarded-Host"]?.substringBefore(',')?.trim()
    val protocol = forwardedProtocol?.takeIf(String::isNotBlank) ?: if (request.isSecure) "https" else "http"
    val host =
      forwardedHost?.takeIf(String::isNotBlank)
        ?: request.headers[HttpHeaders.HOST]
        ?: "${request.serverAddress.hostString}:${request.serverAddress.port}"
    return URI.create("$protocol://$host/")
  }

  private fun toDomainFilter(filter: ScimFilter.Equal): ScimUserFilterClause =
    ScimUserFilterClause(
      attribute =
        when (filter.attribute) {
          ScimFilter.Attribute.USER_NAME -> ScimUserFilterAttribute.USER_NAME
          ScimFilter.Attribute.EXTERNAL_ID -> ScimUserFilterAttribute.EXTERNAL_ID
          ScimFilter.Attribute.EMAIL -> ScimUserFilterAttribute.EMAIL
          ScimFilter.Attribute.WORK_EMAIL -> ScimUserFilterAttribute.WORK_EMAIL
          else -> throw ScimErrors.invalidFilter()
        },
      value = filter.value,
    )

  private inline fun <T> translateDomain(block: () -> T): T =
    try {
      block()
    } catch (_: ScimUserNotFoundException) {
      throw ScimErrors.notFound("User not found")
    } catch (_: ScimUserConflictException) {
      throw ScimErrors.uniqueness()
    }
}
