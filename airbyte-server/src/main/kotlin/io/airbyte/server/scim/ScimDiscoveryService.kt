/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.scim

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import io.airbyte.api.scim.generated.models.ScimAuthenticationScheme
import io.airbyte.api.scim.generated.models.ScimBulkConfiguration
import io.airbyte.api.scim.generated.models.ScimDiscoveryMeta
import io.airbyte.api.scim.generated.models.ScimFilterConfiguration
import io.airbyte.api.scim.generated.models.ScimResourceType
import io.airbyte.api.scim.generated.models.ScimResourceTypeListResponse
import io.airbyte.api.scim.generated.models.ScimSchema
import io.airbyte.api.scim.generated.models.ScimSchemaAttribute
import io.airbyte.api.scim.generated.models.ScimSchemaListResponse
import io.airbyte.api.scim.generated.models.ScimServiceProviderConfig
import io.airbyte.api.scim.generated.models.ScimSupportedFeature
import io.airbyte.commons.jackson.MoreMappers
import io.micronaut.http.HttpStatus
import jakarta.inject.Singleton
import java.net.URI

@Singleton
class ScimDiscoveryService(
  private val objectMapper: ObjectMapper = MoreMappers.initMapper(),
) {
  private val serviceProviderConfig =
    ScimServiceProviderConfig(
      schemas = listOf(SCIM_SERVICE_PROVIDER_CONFIG_SCHEMA),
      patch = ScimSupportedFeature(supported = true),
      bulk =
        ScimBulkConfiguration(
          supported = false,
          maxOperations = 0,
          maxPayloadSize = 0,
        ),
      filter =
        ScimFilterConfiguration(
          supported = true,
          maxResults = 200,
        ),
      changePassword = ScimSupportedFeature(supported = false),
      sort = ScimSupportedFeature(supported = false),
      etag = ScimSupportedFeature(supported = false),
      authenticationSchemes =
        listOf(
          ScimAuthenticationScheme(
            type = "oauthbearertoken",
            name = "Bearer Token",
            description = "Authentication using the HTTP Authorization header with an Airbyte-issued bearer token.",
            specUri = URI.create("https://www.rfc-editor.org/rfc/rfc6750"),
            primary = true,
          ),
        ),
    )

  private val userResourceType =
    ScimResourceType(
      schemas = listOf(SCIM_RESOURCE_TYPE_SCHEMA),
      id = "User",
      name = "User",
      endpoint = "/Users",
      description = "User Account",
      schema = SCIM_USER_SCHEMA,
    )

  private val groupResourceType =
    ScimResourceType(
      schemas = listOf(SCIM_RESOURCE_TYPE_SCHEMA),
      id = "Group",
      name = "Group",
      endpoint = "/Groups",
      description = "Group",
      schema = SCIM_GROUP_SCHEMA,
    )

  private val userSchema =
    ScimSchema(
      schemas = listOf(SCIM_SCHEMA_SCHEMA),
      id = SCIM_USER_SCHEMA,
      name = "User",
      description = "User Account",
      meta =
        ScimDiscoveryMeta(
          resourceType = "Schema",
          location = "/scim/v2/Schemas/$SCIM_USER_SCHEMA",
        ),
      attributes =
        listOf(
          attribute(
            name = "userName",
            type = ScimSchemaAttribute.Type.STRING,
            description = "Unique identifier for the User.",
            required = true,
            uniqueness = ScimSchemaAttribute.Uniqueness.SERVER,
          ),
          attribute(
            name = "name",
            type = ScimSchemaAttribute.Type.COMPLEX,
            description = "Components of the User's name.",
            subAttributes =
              listOf(
                attribute("formatted", ScimSchemaAttribute.Type.STRING, "Full formatted name."),
                attribute("familyName", ScimSchemaAttribute.Type.STRING, "Family name."),
                attribute("givenName", ScimSchemaAttribute.Type.STRING, "Given name."),
                attribute("middleName", ScimSchemaAttribute.Type.STRING, "Middle name."),
                attribute("honorificPrefix", ScimSchemaAttribute.Type.STRING, "Honorific prefix."),
                attribute("honorificSuffix", ScimSchemaAttribute.Type.STRING, "Honorific suffix."),
              ),
          ),
          attribute("displayName", ScimSchemaAttribute.Type.STRING, "Display name for the User."),
          attribute("nickName", ScimSchemaAttribute.Type.STRING, "Casual name for the User."),
          attribute(
            name = "profileUrl",
            type = ScimSchemaAttribute.Type.REFERENCE,
            description = "URL for the User's profile.",
            referenceTypes = listOf("external"),
          ),
          attribute("title", ScimSchemaAttribute.Type.STRING, "Title of the User."),
          attribute("userType", ScimSchemaAttribute.Type.STRING, "Classification of the User."),
          attribute(
            "preferredLanguage",
            ScimSchemaAttribute.Type.STRING,
            "Preferred written or spoken language for the User.",
          ),
          attribute("locale", ScimSchemaAttribute.Type.STRING, "Locale for the User."),
          attribute("timezone", ScimSchemaAttribute.Type.STRING, "Time zone for the User."),
          attribute(
            "active",
            ScimSchemaAttribute.Type.BOOLEAN,
            "Whether the User is active in the SCIM organization.",
          ),
          attribute(
            name = "emails",
            type = ScimSchemaAttribute.Type.COMPLEX,
            description = "Email addresses for the User.",
            multiValued = true,
            required = true,
            subAttributes =
              listOf(
                attribute(
                  name = "value",
                  type = ScimSchemaAttribute.Type.STRING,
                  description = "Email address value.",
                  required = true,
                ),
                attribute(
                  name = "type",
                  type = ScimSchemaAttribute.Type.STRING,
                  description = "Email address type.",
                  canonicalValues = listOf("work", "home", "other"),
                ),
                attribute(
                  "primary",
                  ScimSchemaAttribute.Type.BOOLEAN,
                  "Whether this is the primary email address.",
                ),
              ),
          ),
          attribute(
            name = "groups",
            type = ScimSchemaAttribute.Type.COMPLEX,
            description = "Direct SCIM-managed Group memberships.",
            multiValued = true,
            mutability = ScimSchemaAttribute.Mutability.READ_ONLY,
            subAttributes =
              listOf(
                attribute(
                  name = "value",
                  type = ScimSchemaAttribute.Type.STRING,
                  description = "Group mapping identifier.",
                  required = true,
                  mutability = ScimSchemaAttribute.Mutability.READ_ONLY,
                  returned = ScimSchemaAttribute.Returned.ALWAYS,
                ),
                attribute(
                  name = "\$ref",
                  type = ScimSchemaAttribute.Type.REFERENCE,
                  description = "Canonical Group resource URL.",
                  mutability = ScimSchemaAttribute.Mutability.READ_ONLY,
                  referenceTypes = listOf("Group"),
                ),
                attribute(
                  name = "display",
                  type = ScimSchemaAttribute.Type.STRING,
                  description = "Group display name.",
                  mutability = ScimSchemaAttribute.Mutability.READ_ONLY,
                ),
              ),
          ),
        ),
    )

  private val groupSchema =
    ScimSchema(
      schemas = listOf(SCIM_SCHEMA_SCHEMA),
      id = SCIM_GROUP_SCHEMA,
      name = "Group",
      description = "Group",
      meta =
        ScimDiscoveryMeta(
          resourceType = "Schema",
          location = "/scim/v2/Schemas/$SCIM_GROUP_SCHEMA",
        ),
      attributes =
        listOf(
          attribute(
            name = "displayName",
            type = ScimSchemaAttribute.Type.STRING,
            description = "Human-readable name for the Group.",
            required = true,
            uniqueness = ScimSchemaAttribute.Uniqueness.SERVER,
          ),
          attribute(
            name = "members",
            type = ScimSchemaAttribute.Type.COMPLEX,
            description = "Direct User members of the Group.",
            multiValued = true,
            subAttributes =
              listOf(
                attribute(
                  name = "value",
                  type = ScimSchemaAttribute.Type.STRING,
                  description = "User mapping identifier.",
                  required = true,
                  mutability = ScimSchemaAttribute.Mutability.IMMUTABLE,
                  returned = ScimSchemaAttribute.Returned.ALWAYS,
                ),
                attribute(
                  name = "\$ref",
                  type = ScimSchemaAttribute.Type.REFERENCE,
                  description = "Canonical User resource URL.",
                  mutability = ScimSchemaAttribute.Mutability.READ_ONLY,
                  referenceTypes = listOf("User"),
                ),
                attribute(
                  name = "display",
                  type = ScimSchemaAttribute.Type.STRING,
                  description = "User display name or userName fallback.",
                  mutability = ScimSchemaAttribute.Mutability.READ_ONLY,
                ),
              ),
          ),
        ),
    )

  fun serviceProviderConfig(
    attributes: String? = null,
    excludedAttributes: String? = null,
  ): ScimServiceProviderConfig = project(serviceProviderConfig, attributes, excludedAttributes, ScimProjectionSchemas.SERVICE_PROVIDER_CONFIG)

  fun listResourceTypes(
    attributes: String? = null,
    excludedAttributes: String? = null,
  ): ScimResourceTypeListResponse =
    ScimResourceTypeListResponse(
      schemas = listOf(SCIM_LIST_RESPONSE_SCHEMA),
      totalResults = 2,
      resources =
        listOf(userResourceType, groupResourceType).map {
          project(it, attributes, excludedAttributes, ScimProjectionSchemas.RESOURCE_TYPE)
        },
      startIndex = 1,
      itemsPerPage = 2,
    )

  fun getResourceType(
    id: String,
    attributes: String? = null,
    excludedAttributes: String? = null,
  ): ScimResourceType =
    project(
      when (id) {
        userResourceType.id -> userResourceType
        groupResourceType.id -> groupResourceType
        else ->
          throw ScimException(
            status = HttpStatus.NOT_FOUND,
            detail = "ResourceType not found",
          )
      },
      attributes,
      excludedAttributes,
      ScimProjectionSchemas.RESOURCE_TYPE,
    )

  fun listSchemas(
    attributes: String? = null,
    excludedAttributes: String? = null,
  ): ScimSchemaListResponse =
    ScimSchemaListResponse(
      schemas = listOf(SCIM_LIST_RESPONSE_SCHEMA),
      totalResults = 2,
      resources =
        listOf(userSchema, groupSchema).map {
          project(it, attributes, excludedAttributes, ScimProjectionSchemas.SCHEMA)
        },
      startIndex = 1,
      itemsPerPage = 2,
    )

  fun getSchema(
    schemaUri: String,
    attributes: String? = null,
    excludedAttributes: String? = null,
  ): ScimSchema =
    project(
      when (schemaUri) {
        userSchema.id -> userSchema
        groupSchema.id -> groupSchema
        else ->
          throw ScimException(
            status = HttpStatus.NOT_FOUND,
            detail = "Schema not found",
          )
      },
      attributes,
      excludedAttributes,
      ScimProjectionSchemas.SCHEMA,
    )

  private inline fun <reified T : Any> project(
    resource: T,
    attributes: String?,
    excludedAttributes: String?,
    schema: ScimProjectionSchema,
  ): T {
    val source = objectMapper.valueToTree<ObjectNode>(resource)
    val projected = ScimProjection.parse(attributes, excludedAttributes, schema).apply(source)
    return objectMapper.treeToValue(projected, T::class.java)
  }

  private fun attribute(
    name: String,
    type: ScimSchemaAttribute.Type,
    description: String,
    multiValued: Boolean = false,
    required: Boolean = false,
    caseExact: Boolean = false,
    mutability: ScimSchemaAttribute.Mutability = ScimSchemaAttribute.Mutability.READ_WRITE,
    returned: ScimSchemaAttribute.Returned = ScimSchemaAttribute.Returned.DEFAULT,
    uniqueness: ScimSchemaAttribute.Uniqueness = ScimSchemaAttribute.Uniqueness.NONE,
    canonicalValues: List<String>? = null,
    referenceTypes: List<String>? = null,
    subAttributes: List<ScimSchemaAttribute>? = null,
  ): ScimSchemaAttribute =
    ScimSchemaAttribute(
      name = name,
      type = type,
      multiValued = multiValued,
      description = description,
      required = required,
      caseExact = caseExact,
      mutability = mutability,
      returned = returned,
      uniqueness = uniqueness,
      canonicalValues = canonicalValues,
      referenceTypes = referenceTypes,
      subAttributes = subAttributes,
    )
}
