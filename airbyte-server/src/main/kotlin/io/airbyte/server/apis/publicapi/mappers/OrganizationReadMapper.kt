/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.OrganizationRead
import io.airbyte.publicApi.server.generated.models.OrganizationResponse

/**
 * Mappers that help convert models from the config api to models from the public api.
 */
object OrganizationReadMapper {
  /**
   * Converts OrganizationRead object from the config api to public api response.
   *
   * @param organizationRead Output of an organization get from config api
   * @return organizationResponse Response object with organization details
   */
  fun from(organizationRead: OrganizationRead): OrganizationResponse =
    OrganizationResponse(
      email = organizationRead.email,
      organizationId = organizationRead.organizationId,
      organizationName = organizationRead.organizationName,
    )
}
