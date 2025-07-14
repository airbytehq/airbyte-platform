/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.services

import io.airbyte.api.model.generated.TagDeleteRequestBody
import io.airbyte.api.model.generated.TagUpdateRequestBody
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.publicApi.server.generated.models.TagCreateRequest
import io.airbyte.publicApi.server.generated.models.TagPatchRequest
import io.airbyte.publicApi.server.generated.models.TagResponse
import io.airbyte.publicApi.server.generated.models.TagsResponse
import io.airbyte.server.apis.publicapi.errorHandlers.ConfigClientErrorHandler
import io.airbyte.server.apis.publicapi.mappers.TagCreateMapper
import io.airbyte.server.apis.publicapi.mappers.TagResponseMapper
import io.airbyte.server.apis.publicapi.mappers.TagsResponseMapper
import io.airbyte.server.handlers.TagHandler
import io.micronaut.context.annotation.Secondary
import jakarta.inject.Singleton
import org.slf4j.LoggerFactory
import java.util.UUID

interface TagService {
  fun listTags(workspaceIds: List<UUID>): TagsResponse

  fun createTag(tagCreateRequest: TagCreateRequest): TagResponse

  fun getTag(tagId: UUID): TagResponse

  fun deleteTag(
    tagId: UUID,
    workspaceId: UUID,
  )

  fun updateTag(
    tagId: UUID,
    workspaceId: UUID,
    tagPatchRequest: TagPatchRequest,
  ): TagResponse
}

@Singleton
@Secondary
class TagServiceImpl(
  private val userService: UserService,
  private val currentUserService: CurrentUserService,
  private val tagHandler: TagHandler,
) : TagService {
  companion object {
    private val log = LoggerFactory.getLogger(ConnectionServiceImpl::class.java)
  }

  override fun listTags(workspaceIds: List<UUID>): TagsResponse {
    val workspaceIdsToQuery = workspaceIds.ifEmpty { userService.getAllWorkspaceIdsForUser(currentUserService.currentUser.userId) }

    val tags =
      kotlin
        .runCatching { tagHandler.getTagsForWorkspaces(workspaceIdsToQuery) }
        .onFailure {
          log.error("Error for listTags", it)
          ConfigClientErrorHandler.handleError(it)
        }.getOrThrow()

    return TagsResponseMapper.from(tags)
  }

  override fun createTag(tagCreateRequest: TagCreateRequest): TagResponse {
    val tag =
      kotlin
        .runCatching { tagHandler.createTag(TagCreateMapper.from(tagCreateRequest)) }
        .onFailure {
          log.error("Error for createTag", it)
          ConfigClientErrorHandler.handleError(it)
        }.getOrThrow()

    return TagResponseMapper.from(tag)
  }

  override fun getTag(tagId: UUID): TagResponse {
    val tag =
      kotlin
        .runCatching { tagHandler.getTag(tagId) }
        .onFailure {
          log.error("Error for getTag", it)
          ConfigClientErrorHandler.handleError(it)
        }.getOrThrow()

    return TagResponseMapper.from(tag)
  }

  override fun deleteTag(
    tagId: UUID,
    workspaceId: UUID,
  ) {
    kotlin
      .runCatching { tagHandler.deleteTag(TagDeleteRequestBody().tagId(tagId).workspaceId(workspaceId)) }
      .onFailure {
        log.error("Error for deleteTag", it)
        ConfigClientErrorHandler.handleError(it)
      }
  }

  override fun updateTag(
    tagId: UUID,
    workspaceId: UUID,
    tagPatchRequest: TagPatchRequest,
  ): TagResponse {
    val tag =
      kotlin
        .runCatching {
          tagHandler
            .updateTag(
              TagUpdateRequestBody()
                .tagId(tagId)
                .workspaceId(workspaceId)
                .name(tagPatchRequest.name)
                .color(tagPatchRequest.color),
            )
        }.onFailure {
          log.error("Error for updateTag", it)
          ConfigClientErrorHandler.handleError(it)
        }.getOrThrow()

    return TagResponseMapper.from(tag)
  }
}
