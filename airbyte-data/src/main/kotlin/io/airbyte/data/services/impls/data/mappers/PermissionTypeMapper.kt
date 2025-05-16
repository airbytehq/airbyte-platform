/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

typealias EntityPermissionType = io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType
typealias ModelPermissionType = io.airbyte.config.Permission.PermissionType

fun EntityPermissionType.toConfigModel(): ModelPermissionType =
  when (this) {
    EntityPermissionType.workspace_admin -> ModelPermissionType.WORKSPACE_ADMIN
    EntityPermissionType.workspace_editor -> ModelPermissionType.WORKSPACE_EDITOR
    EntityPermissionType.workspace_runner -> ModelPermissionType.WORKSPACE_RUNNER
    EntityPermissionType.workspace_reader -> ModelPermissionType.WORKSPACE_READER
    EntityPermissionType.organization_admin -> ModelPermissionType.ORGANIZATION_ADMIN
    EntityPermissionType.organization_editor -> ModelPermissionType.ORGANIZATION_EDITOR
    EntityPermissionType.organization_runner -> ModelPermissionType.ORGANIZATION_RUNNER
    EntityPermissionType.organization_reader -> ModelPermissionType.ORGANIZATION_READER
    EntityPermissionType.organization_member -> ModelPermissionType.ORGANIZATION_MEMBER
    EntityPermissionType.instance_admin -> ModelPermissionType.INSTANCE_ADMIN
    EntityPermissionType.dataplane -> ModelPermissionType.DATAPLANE
  }

fun ModelPermissionType.toEntity(): EntityPermissionType =
  when (this) {
    ModelPermissionType.WORKSPACE_OWNER -> EntityPermissionType.workspace_admin
    ModelPermissionType.WORKSPACE_ADMIN -> EntityPermissionType.workspace_admin
    ModelPermissionType.WORKSPACE_EDITOR -> EntityPermissionType.workspace_editor
    ModelPermissionType.WORKSPACE_RUNNER -> EntityPermissionType.workspace_runner
    ModelPermissionType.WORKSPACE_READER -> EntityPermissionType.workspace_reader
    ModelPermissionType.ORGANIZATION_ADMIN -> EntityPermissionType.organization_admin
    ModelPermissionType.ORGANIZATION_EDITOR -> EntityPermissionType.organization_editor
    ModelPermissionType.ORGANIZATION_RUNNER -> EntityPermissionType.organization_runner
    ModelPermissionType.ORGANIZATION_READER -> EntityPermissionType.organization_reader
    ModelPermissionType.ORGANIZATION_MEMBER -> EntityPermissionType.organization_member
    ModelPermissionType.INSTANCE_ADMIN -> EntityPermissionType.instance_admin
    ModelPermissionType.DATAPLANE -> EntityPermissionType.dataplane
  }
