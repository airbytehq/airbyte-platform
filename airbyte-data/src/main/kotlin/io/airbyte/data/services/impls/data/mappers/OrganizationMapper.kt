package io.airbyte.data.services.impls.data.mappers

typealias EntityOrganization = io.airbyte.data.repositories.entities.Organization
typealias ModelOrganization = io.airbyte.config.Organization

fun EntityOrganization.toConfigModel(): ModelOrganization =
  ModelOrganization()
    .withOrganizationId(this.id)
    .withName(this.name)
    .withUserId(this.userId)
    .withEmail(this.email)
    .withPba(this.pba)
    .withOrgLevelBilling(this.orgLevelBilling)

fun ModelOrganization.toEntity(): EntityOrganization =
  EntityOrganization(
    id = this.organizationId,
    name = this.name,
    userId = this.userId,
    email = this.email,
    pba = this.pba,
    orgLevelBilling = this.orgLevelBilling,
  )
