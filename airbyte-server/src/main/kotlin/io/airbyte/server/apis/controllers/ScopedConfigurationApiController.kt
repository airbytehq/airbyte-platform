/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.server.generated.apis.ScopedConfigurationApi
import io.airbyte.api.server.generated.models.ScopedConfigurationContextRequestBody
import io.airbyte.api.server.generated.models.ScopedConfigurationContextResponse
import io.airbyte.api.server.generated.models.ScopedConfigurationCreateRequestBody
import io.airbyte.api.server.generated.models.ScopedConfigurationCreateResponse
import io.airbyte.api.server.generated.models.ScopedConfigurationDeleteRequestBody
import io.airbyte.api.server.generated.models.ScopedConfigurationDeleteResponse
import io.airbyte.api.server.generated.models.ScopedConfigurationListRequestBody
import io.airbyte.api.server.generated.models.ScopedConfigurationListResponse
import io.airbyte.api.server.generated.models.ScopedConfigurationRead
import io.airbyte.api.server.generated.models.ScopedConfigurationReadRequestBody
import io.airbyte.api.server.generated.models.ScopedConfigurationReadResponse
import io.airbyte.api.server.generated.models.ScopedConfigurationUpdateRequestBody
import io.airbyte.api.server.generated.models.ScopedConfigurationUpdateResponse
import io.airbyte.commons.auth.route.AuthorizeOperation
import io.airbyte.commons.auth.route.RouteAccessPolicy
import io.airbyte.commons.auth.route.RouteMigration
import io.airbyte.commons.server.handlers.ScopedConfigurationHandler
import io.airbyte.commons.server.scheduling.AirbyteTaskExecutors
import io.airbyte.config.ConfigOriginType
import io.airbyte.server.apis.execute
import io.airbyte.server.apis.requireBody
import io.micronaut.context.annotation.Context
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.scheduling.annotation.ExecuteOn
import io.micronaut.security.annotation.Secured
import io.micronaut.security.rules.SecurityRule

@Controller
@Context
@Secured(SecurityRule.IS_AUTHENTICATED)
class ScopedConfigurationApiController(
  private val scopedConfigurationHandler: ScopedConfigurationHandler,
) : ScopedConfigurationApi {
  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.ScopedConfigurationCreateRule::class,
  )
  override fun createScopedConfiguration(
    @Body requestBody: ScopedConfigurationCreateRequestBody?,
  ): ScopedConfigurationCreateResponse {
    val scopedConfigurationCreateRequestBody = requestBody.requireBody()
    return execute {
      val createdScopedConfiguration =
        scopedConfigurationHandler.insertScopedConfiguration(
          scopedConfigurationCreateRequestBody,
        )
      ScopedConfigurationCreateResponse(data = createdScopedConfiguration)
    }
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.ScopedConfigurationDeleteRule::class,
  )
  override fun deleteScopedConfiguration(
    @Body requestBody: ScopedConfigurationDeleteRequestBody?,
  ): ScopedConfigurationDeleteResponse {
    val scopedConfigurationDeleteRequestBody = requestBody.requireBody()
    return execute {
      val scopedConfigurationId = scopedConfigurationDeleteRequestBody.scopedConfigurationId
      scopedConfigurationHandler.deleteScopedConfiguration(scopedConfigurationId)
      ScopedConfigurationDeleteResponse(scopedConfigurationId = scopedConfigurationId)
    }
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.ScopedConfigurationListRule::class,
  )
  override fun getScopedConfigurationsList(
    @Body requestBody: ScopedConfigurationListRequestBody?,
  ): ScopedConfigurationListResponse {
    val scopedConfigurationListRequestBody = requestBody.requireBody()
    return execute {
      val scopedConfigurations: List<ScopedConfigurationRead>
      val configKey = scopedConfigurationListRequestBody.configKey
      val originType = scopedConfigurationListRequestBody.originType
      if (configKey != null && originType != null) {
        throw BadRequestProblem()
      }

      if (configKey != null) {
        scopedConfigurations =
          scopedConfigurationHandler.listScopedConfigurations(configKey)
      } else if (originType != null) {
        scopedConfigurations =
          scopedConfigurationHandler.listScopedConfigurations(
            ConfigOriginType.valueOf(originType),
          )
      } else {
        throw BadRequestProblem()
      }
      ScopedConfigurationListResponse(scopedConfigurations = scopedConfigurations)
    }
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.ScopedConfigurationReadRule::class,
  )
  override fun getScopedConfigurationById(
    @Body requestBody: ScopedConfigurationReadRequestBody?,
  ): ScopedConfigurationReadResponse {
    val scopedConfigurationReadRequestBody = requestBody.requireBody()
    return execute {
      val scopedConfigurationId = scopedConfigurationReadRequestBody.scopedConfigurationId
      val scopedConfiguration =
        scopedConfigurationHandler.getScopedConfiguration(scopedConfigurationId)
      ScopedConfigurationReadResponse(data = scopedConfiguration)
    }
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.ScopedConfigurationContextRule::class,
  )
  override fun getScopedConfigurationContext(
    @Body requestBody: ScopedConfigurationContextRequestBody?,
  ): ScopedConfigurationContextResponse {
    val scopedConfigurationContextRequestBody = requestBody.requireBody()
    return execute {
      scopedConfigurationHandler.getScopedConfigurationContext(
        scopedConfigurationContextRequestBody,
      )
    }
  }

  @ExecuteOn(AirbyteTaskExecutors.IO)
  @Secured(SecurityRule.IS_AUTHENTICATED)
  @AuthorizeOperation(
    access = RouteAccessPolicy.PROTECTED,
    migration = RouteMigration.SHARED,
    rule = io.airbyte.server.auth.ScopedConfigurationUpdateRule::class,
  )
  override fun updateScopedConfiguration(
    @Body requestBody: ScopedConfigurationUpdateRequestBody?,
  ): ScopedConfigurationUpdateResponse {
    val scopedConfigurationUpdateRequestBody = requestBody.requireBody()
    return execute {
      val scopedConfigurationId = scopedConfigurationUpdateRequestBody.scopedConfigurationId
      val scopedConfigData: ScopedConfigurationCreateRequestBody = scopedConfigurationUpdateRequestBody.data

      val updatedScopedConfiguration =
        scopedConfigurationHandler.updateScopedConfiguration(
          scopedConfigurationId,
          scopedConfigData,
        )
      ScopedConfigurationUpdateResponse(data = updatedScopedConfiguration)
    }
  }
}
