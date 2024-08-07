package io.airbyte.data.repositories.entities

import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity

@MappedEntity("declarative_manifest_image_version")
data class DeclarativeManifestImageVersion(
  @field:Id
  val majorVersion: Int,
  val imageVersion: String,
  val imageSha: String,
  @DateCreated
  val createdAt: java.time.OffsetDateTime? = null,
  @DateUpdated
  val updatedAt: java.time.OffsetDateTime? = null,
)
