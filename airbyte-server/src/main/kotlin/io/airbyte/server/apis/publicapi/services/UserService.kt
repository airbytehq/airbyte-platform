/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.services

import io.airbyte.api.model.generated.ListWorkspacesByUserRequestBody
import io.airbyte.api.model.generated.OrganizationIdRequestBody
import io.airbyte.api.model.generated.OrganizationUserReadList
import io.airbyte.api.model.generated.WorkspaceReadList
import io.airbyte.commons.server.handlers.UserHandler
import io.airbyte.commons.server.handlers.WorkspacesHandler
import io.airbyte.publicApi.server.generated.models.UsersResponse
import io.airbyte.server.apis.publicapi.constants.HTTP_RESPONSE_BODY_DEBUG_MESSAGE
import io.airbyte.server.apis.publicapi.errorHandlers.ConfigClientErrorHandler
import io.airbyte.server.apis.publicapi.mappers.UserReadMapper
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Secondary
import jakarta.inject.Singleton
import java.util.UUID

interface UserService {
  fun getAllWorkspaceIdsForUser(userId: UUID): List<UUID>

  fun getUsersInAnOrganization(
    organizationId: UUID,
    ids: List<String>?,
    emails: List<String>?,
  ): UsersResponse
}

private val log = KotlinLogging.logger {}

@Singleton
@Secondary
open class UserServiceImpl(
  private val workspacesHandler: WorkspacesHandler,
  private val userHandler: UserHandler,
) : UserService {
  override fun getAllWorkspaceIdsForUser(userId: UUID): List<UUID> {
    val listWorkspacesByUserRequestBody = ListWorkspacesByUserRequestBody().userId(userId)
    val result =
      kotlin
        .runCatching { workspacesHandler.listWorkspacesByUser(listWorkspacesByUserRequestBody) }
        .onFailure {
          log.error(it) { "Error for listWorkspacesByUser" }
          ConfigClientErrorHandler.handleError(it)
        }

    log.debug { HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result }
    val workspaceReadList: WorkspaceReadList = result.getOrDefault(WorkspaceReadList().workspaces(emptyList()))
    return workspaceReadList.workspaces.map { it.workspaceId }
  }

  override fun getUsersInAnOrganization(
    organizationId: UUID,
    ids: List<String>?,
    emails: List<String>?,
  ): UsersResponse {
    // 1. Get all users in the organization.
    val organizationIdRequestBody = OrganizationIdRequestBody().organizationId(organizationId)
    val result =
      kotlin
        .runCatching {
          userHandler.listUsersInOrganization(organizationIdRequestBody)
        }.onFailure {
          log.error(it) { "Error for getUsersInAnOrganization" }
          ConfigClientErrorHandler.handleError(it)
        }
    log.debug { HTTP_RESPONSE_BODY_DEBUG_MESSAGE + result }
    val userReadList = result.getOrThrow()
    // 2. Filter users on either ids or emails.
    val finalUserReadList = OrganizationUserReadList()
    finalUserReadList.users =
      when {
        !ids.isNullOrEmpty() && !emails.isNullOrEmpty() -> { // Filter on both ids and emails.
          userReadList.users.filter { it.userId.toString() in ids || it.email in emails }
        }
        !ids.isNullOrEmpty() -> { // Filter on ids only.
          userReadList.users.filter { it.userId.toString() in ids }
        }
        !emails.isNullOrEmpty() -> { // Filter on emails only.
          userReadList.users.filter { it.email in emails }
        }
        else -> { // If there is no filters at all, we will list all users.
          userReadList.users
        }
      }
    // 3. Return mapped result.
    return UsersResponse(data = finalUserReadList.users.mapNotNull { UserReadMapper.from(it) })
  }
}
