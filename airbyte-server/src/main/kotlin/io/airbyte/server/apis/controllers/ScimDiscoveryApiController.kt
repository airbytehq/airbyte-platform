/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.scim.generated.apis.ScimDiscoveryApi
import io.airbyte.api.scim.generated.models.ScimResourceType
import io.airbyte.api.scim.generated.models.ScimResourceTypeListResponse
import io.airbyte.api.scim.generated.models.ScimSchema
import io.airbyte.api.scim.generated.models.ScimSchemaListResponse
import io.airbyte.api.scim.generated.models.ScimServiceProviderConfig
import io.airbyte.server.scim.SCIM_MEDIA_TYPE
import io.airbyte.server.scim.ScimDiscoveryService
import io.micronaut.http.HttpResponse
import io.micronaut.http.MutableHttpResponse
import io.micronaut.http.annotation.Controller
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller
@Secured(SecurityRule.IS_ANONYMOUS)
open class ScimDiscoveryApiController(
  private val discoveryService: ScimDiscoveryService,
) : ScimDiscoveryApi {
  override fun getServiceProviderConfig(
    attributes: String?,
    excludedAttributes: String?,
  ): MutableHttpResponse<ScimServiceProviderConfig> =
    HttpResponse.ok(discoveryService.serviceProviderConfig(attributes, excludedAttributes)).contentType(SCIM_MEDIA_TYPE)

  override fun listResourceTypes(
    attributes: String?,
    excludedAttributes: String?,
  ): MutableHttpResponse<ScimResourceTypeListResponse> =
    HttpResponse.ok(discoveryService.listResourceTypes(attributes, excludedAttributes)).contentType(SCIM_MEDIA_TYPE)

  override fun getResourceType(
    id: String,
    attributes: String?,
    excludedAttributes: String?,
  ): MutableHttpResponse<ScimResourceType> =
    HttpResponse.ok(discoveryService.getResourceType(id, attributes, excludedAttributes)).contentType(SCIM_MEDIA_TYPE)

  override fun listSchemas(
    attributes: String?,
    excludedAttributes: String?,
  ): MutableHttpResponse<ScimSchemaListResponse> =
    HttpResponse.ok(discoveryService.listSchemas(attributes, excludedAttributes)).contentType(SCIM_MEDIA_TYPE)

  override fun getSchema(
    schemaUri: String,
    attributes: String?,
    excludedAttributes: String?,
  ): MutableHttpResponse<ScimSchema> =
    HttpResponse.ok(discoveryService.getSchema(schemaUri, attributes, excludedAttributes)).contentType(SCIM_MEDIA_TYPE)
}
