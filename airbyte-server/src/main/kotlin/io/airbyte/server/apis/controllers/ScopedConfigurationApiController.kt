/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.generated.ScopedConfigurationApi
import io.airbyte.api.model.generated.ScopedConfigurationContextRequestBody
import io.airbyte.api.model.generated.ScopedConfigurationContextResponse
import io.airbyte.api.model.generated.ScopedConfigurationCreateRequestBody
import io.airbyte.api.model.generated.ScopedConfigurationCreateResponse
import io.airbyte.api.model.generated.ScopedConfigurationDeleteRequestBody
import io.airbyte.api.model.generated.ScopedConfigurationDeleteResponse
import io.airbyte.api.model.generated.ScopedConfigurationListRequestBody
import io.airbyte.api.model.generated.ScopedConfigurationListResponse
import io.airbyte.api.model.generated.ScopedConfigurationRead
import io.airbyte.api.model.generated.ScopedConfigurationReadRequestBody
import io.airbyte.api.model.generated.ScopedConfigurationReadResponse
import io.airbyte.api.model.generated.ScopedConfigurationUpdateRequestBody
import io.airbyte.api.model.generated.ScopedConfigurationUpdateResponse
import io.airbyte.commons.auth.AuthRoleConstants
import io.airbyte.commons.server.handlers.ScopedConfigurationHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.config.ConfigOriginType
import io.airbyte.server.apis.execute
import io.micronaut.context.annotation.Context
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller("/api/v1/scoped_configuration")
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
class ScopedConfigurationApiController(
  private val scopedConfigurationHandler: ScopedConfigurationHandler,
) : ScopedConfigurationApi {
  @Post("/create")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun createScopedConfiguration(
    @Body scopedConfigurationCreateRequestBody: ScopedConfigurationCreateRequestBody,
  ): ScopedConfigurationCreateResponse? =
    execute {
      val createdScopedConfiguration =
        scopedConfigurationHandler.insertScopedConfiguration(
          scopedConfigurationCreateRequestBody,
        )
      val response =
        ScopedConfigurationCreateResponse()
      response.setData(createdScopedConfiguration)
      response
    }

  @Post("/delete")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun deleteScopedConfiguration(
    @Body scopedConfigurationDeleteRequestBody: ScopedConfigurationDeleteRequestBody,
  ): ScopedConfigurationDeleteResponse? =
    execute {
      val scopedConfigurationId = scopedConfigurationDeleteRequestBody.scopedConfigurationId
      scopedConfigurationHandler.deleteScopedConfiguration(scopedConfigurationId)
      ScopedConfigurationDeleteResponse().scopedConfigurationId(scopedConfigurationId)
    }

  @Post("/list")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getScopedConfigurationsList(
    @Body scopedConfigurationListRequestBody: ScopedConfigurationListRequestBody,
  ): ScopedConfigurationListResponse? =
    execute {
      val scopedConfigurations: List<ScopedConfigurationRead>
      if (scopedConfigurationListRequestBody.configKey != null && scopedConfigurationListRequestBody.originType != null) {
        throw RuntimeException(
          "Either a configuration key or origin type is required, but not both. " +
            "Got configKey=${scopedConfigurationListRequestBody.configKey} originType=%${scopedConfigurationListRequestBody.originType}.",
        )
      }

      if (scopedConfigurationListRequestBody.configKey != null) {
        scopedConfigurations =
          scopedConfigurationHandler.listScopedConfigurations(scopedConfigurationListRequestBody.configKey)
      } else if (scopedConfigurationListRequestBody.originType != null) {
        scopedConfigurations =
          scopedConfigurationHandler.listScopedConfigurations(
            ConfigOriginType.valueOf(scopedConfigurationListRequestBody.originType),
          )
      } else {
        throw RuntimeException(
          "Either a configuration key or origin type is required. " +
            "Got configKey=${scopedConfigurationListRequestBody.configKey} originType=%${scopedConfigurationListRequestBody.originType}.",
        )
      }
      ScopedConfigurationListResponse().scopedConfigurations(scopedConfigurations)
    }

  @Post("/get")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getScopedConfigurationById(
    @Body scopedConfigurationReadRequestBody: ScopedConfigurationReadRequestBody,
  ): ScopedConfigurationReadResponse? =
    execute {
      val scopedConfigurationId = scopedConfigurationReadRequestBody.scopedConfigurationId
      val scopedConfiguration =
        scopedConfigurationHandler.getScopedConfiguration(scopedConfigurationId)
      ScopedConfigurationReadResponse().data(scopedConfiguration)
    }

  @Post("/get_context")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun getScopedConfigurationContext(
    @Body scopedConfigurationContextRequestBody: ScopedConfigurationContextRequestBody,
  ): ScopedConfigurationContextResponse? =
    execute {
      scopedConfigurationHandler.getScopedConfigurationContext(
        scopedConfigurationContextRequestBody,
      )
    }

  @Post("/update")
  @Secured(AuthRoleConstants.ADMIN)
  @ExecuteOn(AirbyteTaskExecutors.IO)
  override fun updateScopedConfiguration(
    @Body scopedConfigurationUpdateRequestBody: ScopedConfigurationUpdateRequestBody,
  ): ScopedConfigurationUpdateResponse? =
    execute {
      val scopedConfigurationId = scopedConfigurationUpdateRequestBody.scopedConfigurationId
      val scopedConfigData: ScopedConfigurationCreateRequestBody = scopedConfigurationUpdateRequestBody.data

      val updatedScopedConfiguration =
        scopedConfigurationHandler.updateScopedConfiguration(
          scopedConfigurationId,
          scopedConfigData,
        )
      ScopedConfigurationUpdateResponse().data(updatedScopedConfiguration)
    }
}
