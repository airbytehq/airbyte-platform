package io.airbyte.data.repositories.entities

import io.micronaut.data.annotation.AutoPopulated
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import java.util.UUID

@MappedEntity("organization_email_domain")
data class OrganizationEmailDomain(
  @field:Id
  @AutoPopulated
  var id: UUID? = null,
  var organizationId: UUID,
  var emailDomain: String,
  @DateCreated
  var createdAt: java.time.OffsetDateTime? = null,
)
