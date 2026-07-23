/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.scim.ScimGroupRequest
import io.airbyte.api.scim.ScimPatchRequest
import io.airbyte.api.scim.generated.apis.ScimGroupsApi
import io.airbyte.api.scim.generated.models.ScimGroup
import io.airbyte.api.scim.generated.models.ScimGroupListResponse
import io.airbyte.domain.models.scim.ScimGroupConflictException
import io.airbyte.domain.models.scim.ScimGroupFilterAttribute
import io.airbyte.domain.models.scim.ScimGroupFilterClause
import io.airbyte.domain.models.scim.ScimGroupInvalidMemberException
import io.airbyte.domain.models.scim.ScimGroupNotFoundException
import io.airbyte.domain.services.scim.ScimAuthenticationContext
import io.airbyte.domain.services.scim.ScimGroupLifecycleService
import io.airbyte.domain.services.scim.ScimMutationService
import io.airbyte.server.scim.SCIM_LIST_RESPONSE_SCHEMA
import io.airbyte.server.scim.SCIM_MEDIA_TYPE
import io.airbyte.server.scim.ScimErrors
import io.airbyte.server.scim.ScimFilter
import io.airbyte.server.scim.ScimFilterParser
import io.airbyte.server.scim.ScimGroupRequestParser
import io.airbyte.server.scim.ScimGroupResourceService
import io.airbyte.server.scim.ScimMemberValidator
import io.airbyte.server.scim.ScimPagination
import io.airbyte.server.scim.ScimPatchProcessor
import io.airbyte.server.scim.ScimTenant
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
open class ScimGroupsApiController(
  private val lifecycleService: ScimGroupLifecycleService,
  private val mutationService: ScimMutationService,
  private val resourceService: ScimGroupResourceService,
  @param:Value("\${airbyte.airbyte-url:}") private val configuredBaseUrl: String = "",
) : ScimGroupsApi {
  override fun createGroup(
    @Body ioAirbyteApiScimScimGroupRequest: ScimGroupRequest,
    attributes: String?,
    excludedAttributes: String?,
  ): MutableHttpResponse<ScimGroup> {
    val request = currentRequest()
    val context = request.scimAuthenticationContext()
    val input = ScimGroupRequestParser.parse(ioAirbyteApiScimScimGroupRequest.body)
    val projection = resourceService.compileProjection(attributes, excludedAttributes)
    val group =
      translateDomain {
        mutationService.execute(context) {
          lifecycleService.create(context.configurationId, context.organizationId.value, input)
        }
      }
    val baseUri = canonicalBaseUri(request)
    val location = resourceService.canonicalGroupLocation(baseUri, group.id.toString())
    return scimCreated(resourceService.render(group, baseUri, projection), location)
  }

  override fun deleteGroup(id: UUID): MutableHttpResponse<*> {
    val context = authenticationContext()
    translateDomain {
      mutationService.execute(context) {
        lifecycleService.delete(context.configurationId, context.organizationId.value, id)
      }
    }
    return scimDeleted()
  }

  override fun getGroup(
    id: UUID,
    attributes: String?,
    excludedAttributes: String?,
  ): MutableHttpResponse<ScimGroup> {
    val context = authenticationContext()
    val projection = resourceService.compileProjection(attributes, excludedAttributes)
    val group = translateDomain { lifecycleService.get(context.configurationId, context.organizationId.value, id) }
    return scimUpdated(resourceService.render(group, canonicalBaseUri(currentRequest()), projection))
  }

  override fun listGroups(
    filter: String?,
    startIndex: Int?,
    count: Int?,
    attributes: String?,
    excludedAttributes: String?,
  ): MutableHttpResponse<ScimGroupListResponse> {
    val context = authenticationContext()
    val projection = resourceService.compileProjection(attributes, excludedAttributes)
    val domainFilter = filter?.let { toDomainFilter(ScimFilterParser.parseGroup(it)) }
    val normalizedStartIndex = (startIndex ?: 1).coerceAtLeast(1)
    val normalizedCount = (count ?: ScimPagination.DEFAULT_COUNT).coerceIn(0, ScimPagination.MAX_COUNT)
    val page =
      lifecycleService.list(
        context.configurationId,
        context.organizationId.value,
        domainFilter,
        normalizedStartIndex.toLong() - 1,
        normalizedCount,
      )
    val baseUri = canonicalBaseUri(currentRequest())
    val response =
      ScimGroupListResponse(
        schemas = listOf(SCIM_LIST_RESPONSE_SCHEMA),
        totalResults = page.totalResults,
        resources = page.resources.map { resourceService.render(it, baseUri, projection) },
        startIndex = normalizedStartIndex,
        itemsPerPage = page.resources.size,
      )
    return HttpResponse.ok(response).contentType(SCIM_MEDIA_TYPE)
  }

  override fun patchGroup(
    id: UUID,
    @Body ioAirbyteApiScimScimPatchRequest: ScimPatchRequest,
    attributes: String?,
    excludedAttributes: String?,
  ): MutableHttpResponse<ScimGroup> {
    val request = currentRequest()
    val context = request.scimAuthenticationContext()
    val baseUri = canonicalBaseUri(request)
    val projection = resourceService.compileProjection(attributes, excludedAttributes)
    val group =
      translateDomain {
        mutationService.execute(context) {
          val current = lifecycleService.get(context.configurationId, context.organizationId.value, id)
          val patched =
            ScimPatchProcessor.applyGroup(
              resourceService.completeResource(current, baseUri),
              ioAirbyteApiScimScimPatchRequest.body,
              ScimTenant(context.configurationId, context.organizationId.value),
              ScimMemberValidator { configurationId, organizationId, memberIds ->
                val parsedIds = runCatching { memberIds.mapTo(linkedSetOf(), UUID::fromString) }.getOrNull()
                parsedIds != null && lifecycleService.areActiveUsers(configurationId, organizationId, parsedIds)
              },
            )
          lifecycleService.replace(
            context.configurationId,
            context.organizationId.value,
            id,
            ScimGroupRequestParser.parse(patched),
          )
        }
      }
    if (attributes == null && excludedAttributes == null) {
      return HttpResponse.noContent()
    }
    return scimUpdated(resourceService.render(group, baseUri, projection))
  }

  override fun replaceGroup(
    id: UUID,
    @Body ioAirbyteApiScimScimGroupRequest: ScimGroupRequest,
    attributes: String?,
    excludedAttributes: String?,
  ): MutableHttpResponse<ScimGroup> {
    val request = currentRequest()
    val context = request.scimAuthenticationContext()
    val input = ScimGroupRequestParser.parse(ioAirbyteApiScimScimGroupRequest.body)
    val projection = resourceService.compileProjection(attributes, excludedAttributes)
    val group =
      translateDomain {
        mutationService.execute(context) {
          lifecycleService.replace(context.configurationId, context.organizationId.value, id, input)
        }
      }
    return scimUpdated(resourceService.render(group, canonicalBaseUri(request), projection))
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

  private fun toDomainFilter(filter: ScimFilter.Equal): ScimGroupFilterClause =
    ScimGroupFilterClause(
      attribute =
        when (filter.attribute) {
          ScimFilter.Attribute.DISPLAY_NAME -> ScimGroupFilterAttribute.DISPLAY_NAME
          ScimFilter.Attribute.MEMBER -> ScimGroupFilterAttribute.MEMBER
          else -> throw ScimErrors.invalidFilter()
        },
      value = filter.value,
    )

  private inline fun <T> translateDomain(block: () -> T): T =
    try {
      block()
    } catch (_: ScimGroupNotFoundException) {
      throw ScimErrors.notFound("Group not found")
    } catch (_: ScimGroupConflictException) {
      throw ScimErrors.uniqueness()
    } catch (_: ScimGroupInvalidMemberException) {
      throw ScimErrors.invalidValue("Group members must be active Users in the authenticated SCIM tenant")
    }
}
