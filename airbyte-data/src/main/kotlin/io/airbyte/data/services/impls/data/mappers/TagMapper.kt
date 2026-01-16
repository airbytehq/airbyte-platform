/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

typealias EntityTag = io.airbyte.data.repositories.entities.Tag
typealias ModelTag = io.airbyte.config.Tag

fun EntityTag.toConfigModel(): ModelTag =
  ModelTag()
    .withTagId(this.id)
    .withName(this.name)
    .withColor(this.color)
    .withWorkspaceId(this.workspaceId)

fun ModelTag.toEntity(): EntityTag =
  EntityTag(
    id = this.tagId,
    name = this.name,
    color = this.color,
    workspaceId = this.workspaceId,
  )
