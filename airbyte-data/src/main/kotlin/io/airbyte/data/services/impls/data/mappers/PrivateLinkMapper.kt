/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data.mappers

typealias EntityPrivateLink = io.airbyte.data.repositories.entities.PrivateLink
typealias DomainPrivateLink = io.airbyte.domain.models.PrivateLink
typealias EntityPrivateLinkStatus = io.airbyte.db.instance.configs.jooq.generated.enums.PrivateLinkStatus
typealias DomainPrivateLinkStatus = io.airbyte.domain.models.PrivateLinkStatus

fun EntityPrivateLink.toDomainModel(): DomainPrivateLink =
  DomainPrivateLink(
    id = this.id,
    workspaceId = this.workspaceId,
    dataplaneGroupId = this.dataplaneGroupId,
    name = this.name,
    status = this.status.toDomainEnum(),
    serviceRegion = this.serviceRegion,
    serviceName = this.serviceName,
    endpointId = this.endpointId,
    dnsName = this.dnsName,
    scopedConfigurationId = this.scopedConfigurationId,
    createdAt = this.createdAt,
    updatedAt = this.updatedAt,
  )

fun DomainPrivateLink.toEntity(): EntityPrivateLink =
  EntityPrivateLink(
    id = this.id,
    workspaceId = this.workspaceId,
    dataplaneGroupId = this.dataplaneGroupId,
    name = this.name,
    status = this.status.toEntityEnum(),
    serviceRegion = this.serviceRegion,
    serviceName = this.serviceName,
    endpointId = this.endpointId,
    dnsName = this.dnsName,
    scopedConfigurationId = this.scopedConfigurationId,
    createdAt = this.createdAt,
    updatedAt = this.updatedAt,
  )

fun EntityPrivateLinkStatus.toDomainEnum(): DomainPrivateLinkStatus =
  when (this) {
    EntityPrivateLinkStatus.creating -> DomainPrivateLinkStatus.CREATING
    EntityPrivateLinkStatus.pending_acceptance -> DomainPrivateLinkStatus.PENDING_ACCEPTANCE
    EntityPrivateLinkStatus.configuring -> DomainPrivateLinkStatus.CONFIGURING
    EntityPrivateLinkStatus.available -> DomainPrivateLinkStatus.AVAILABLE
    EntityPrivateLinkStatus.create_failed -> DomainPrivateLinkStatus.CREATE_FAILED
    EntityPrivateLinkStatus.deleting -> DomainPrivateLinkStatus.DELETING
    EntityPrivateLinkStatus.delete_failed -> DomainPrivateLinkStatus.DELETE_FAILED
  }

fun DomainPrivateLinkStatus.toEntityEnum(): EntityPrivateLinkStatus =
  when (this) {
    DomainPrivateLinkStatus.CREATING -> EntityPrivateLinkStatus.creating
    DomainPrivateLinkStatus.PENDING_ACCEPTANCE -> EntityPrivateLinkStatus.pending_acceptance
    DomainPrivateLinkStatus.CONFIGURING -> EntityPrivateLinkStatus.configuring
    DomainPrivateLinkStatus.AVAILABLE -> EntityPrivateLinkStatus.available
    DomainPrivateLinkStatus.CREATE_FAILED -> EntityPrivateLinkStatus.create_failed
    DomainPrivateLinkStatus.DELETING -> EntityPrivateLinkStatus.deleting
    DomainPrivateLinkStatus.DELETE_FAILED -> EntityPrivateLinkStatus.delete_failed
  }
