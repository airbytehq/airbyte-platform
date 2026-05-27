/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

typealias EntityPermissionType = io.airbyte.db.instance.configs.jooq.generated.enums.PermissionType
typealias ModelPermissionType = io.airbyte.config.Permission.PermissionType
typealias DomainSsoDefaultRole = io.airbyte.domain.models.SsoDefaultRole

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

fun DomainSsoDefaultRole.toEntity(): EntityPermissionType =
  when (this) {
    DomainSsoDefaultRole.ORGANIZATION_ADMIN -> EntityPermissionType.organization_admin
    DomainSsoDefaultRole.ORGANIZATION_EDITOR -> EntityPermissionType.organization_editor
    DomainSsoDefaultRole.ORGANIZATION_MEMBER -> EntityPermissionType.organization_member
  }

fun DomainSsoDefaultRole.toConfigModel(): ModelPermissionType =
  when (this) {
    DomainSsoDefaultRole.ORGANIZATION_ADMIN -> ModelPermissionType.ORGANIZATION_ADMIN
    DomainSsoDefaultRole.ORGANIZATION_EDITOR -> ModelPermissionType.ORGANIZATION_EDITOR
    DomainSsoDefaultRole.ORGANIZATION_MEMBER -> ModelPermissionType.ORGANIZATION_MEMBER
  }

fun ModelPermissionType.toSsoDefaultRole(): DomainSsoDefaultRole =
  when (this) {
    ModelPermissionType.ORGANIZATION_ADMIN -> DomainSsoDefaultRole.ORGANIZATION_ADMIN
    ModelPermissionType.ORGANIZATION_EDITOR -> DomainSsoDefaultRole.ORGANIZATION_EDITOR
    ModelPermissionType.ORGANIZATION_MEMBER -> DomainSsoDefaultRole.ORGANIZATION_MEMBER
    else -> throw IllegalArgumentException("Permission type is not supported as an SSO default role.")
  }
