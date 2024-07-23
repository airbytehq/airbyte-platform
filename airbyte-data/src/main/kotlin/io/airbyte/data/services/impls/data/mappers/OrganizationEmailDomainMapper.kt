package io.airbyte.data.services.impls.data.mappers

typealias EntityOrganizationEmailDomain = io.airbyte.data.repositories.entities.OrganizationEmailDomain
typealias ModelOrganizationEmailDomain = io.airbyte.config.OrganizationEmailDomain

fun EntityOrganizationEmailDomain.toConfigModel(): ModelOrganizationEmailDomain {
  return ModelOrganizationEmailDomain()
    .withId(this.id)
    .withOrganizationId(this.organizationId)
    .withEmailDomain(this.emailDomain)
}

fun ModelOrganizationEmailDomain.toEntity(): EntityOrganizationEmailDomain {
  return EntityOrganizationEmailDomain(
    id = this.id,
    organizationId = this.organizationId,
    emailDomain = this.emailDomain,
  )
}
