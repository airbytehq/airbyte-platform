/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import io.airbyte.api.scim.generated.models.ScimError
import io.airbyte.api.scim.generated.models.ScimGroup
import io.airbyte.api.scim.generated.models.ScimUser
import io.micronaut.http.HttpHeaders
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.MediaType
import io.micronaut.http.MutableHttpResponse
import java.net.URI

const val SCIM_SERVICE_PROVIDER_CONFIG_SCHEMA = "urn:ietf:params:scim:schemas:core:2.0:ServiceProviderConfig"
const val SCIM_RESOURCE_TYPE_SCHEMA = "urn:ietf:params:scim:schemas:core:2.0:ResourceType"
const val SCIM_SCHEMA_SCHEMA = "urn:ietf:params:scim:schemas:core:2.0:Schema"
const val SCIM_USER_SCHEMA = "urn:ietf:params:scim:schemas:core:2.0:User"
const val SCIM_GROUP_SCHEMA = "urn:ietf:params:scim:schemas:core:2.0:Group"
const val SCIM_LIST_RESPONSE_SCHEMA = "urn:ietf:params:scim:api:messages:2.0:ListResponse"
const val SCIM_PATCH_OP_SCHEMA = "urn:ietf:params:scim:api:messages:2.0:PatchOp"
const val SCIM_ERROR_SCHEMA = "urn:ietf:params:scim:api:messages:2.0:Error"

val SCIM_MEDIA_TYPE: MediaType = MediaType.of("application/scim+json")

fun scimCreated(resource: ScimUser): MutableHttpResponse<ScimUser> =
  createdResponse(
    resource,
    requireNotNull(resource.meta?.location) { "Created SCIM User must include meta.location" },
  )

fun scimCreated(
  resource: ScimUser,
  canonicalLocation: URI,
): MutableHttpResponse<ScimUser> {
  val projectedLocation = resource.meta?.location
  require(projectedLocation == null || projectedLocation.toString() == canonicalLocation.toString()) {
    "Created SCIM User meta.location must match the canonical location"
  }
  return createdResponse(resource, canonicalLocation)
}

fun scimCreated(resource: ScimGroup): MutableHttpResponse<ScimGroup> =
  createdResponse(
    resource,
    requireNotNull(resource.meta?.location) { "Created SCIM Group must include meta.location" },
  )

fun scimCreated(
  resource: ScimGroup,
  canonicalLocation: URI,
): MutableHttpResponse<ScimGroup> {
  val projectedLocation = resource.meta?.location
  require(projectedLocation == null || projectedLocation.toString() == canonicalLocation.toString()) {
    "Created SCIM Group meta.location must match the canonical location"
  }
  return createdResponse(resource, canonicalLocation)
}

fun <T> scimUpdated(resource: T): MutableHttpResponse<T> =
  HttpResponse
    .ok(resource)
    .contentType(SCIM_MEDIA_TYPE)

fun scimGroupPatched(
  resource: ScimGroup,
  attributes: String?,
  excludedAttributes: String?,
): MutableHttpResponse<ScimGroup> =
  if (attributes == null && excludedAttributes == null) {
    HttpResponse.noContent()
  } else {
    scimUpdated(resource)
  }

fun scimDeleted(): MutableHttpResponse<Any> = HttpResponse.noContent()

fun scimError(
  status: HttpStatus,
  detail: String?,
  scimType: String? = null,
): ScimError =
  ScimError(
    schemas = listOf(SCIM_ERROR_SCHEMA),
    status = status.code.toString(),
    scimType = scimType,
    detail = detail,
  )

private fun <T> createdResponse(
  resource: T,
  location: URI,
): MutableHttpResponse<T> =
  HttpResponse
    .created(resource)
    .contentType(SCIM_MEDIA_TYPE)
    .header(HttpHeaders.LOCATION, location.toString())
    .header(HttpHeaders.CONTENT_LOCATION, location.toString())
