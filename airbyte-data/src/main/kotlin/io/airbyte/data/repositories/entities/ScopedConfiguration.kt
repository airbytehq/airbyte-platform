package io.airbyte.data.repositories.entities

import io.airbyte.db.instance.configs.jooq.generated.enums.ConfigOriginType
import io.airbyte.db.instance.configs.jooq.generated.enums.ConfigResourceType
import io.airbyte.db.instance.configs.jooq.generated.enums.ConfigScopeType
import io.micronaut.core.annotation.Nullable
import io.micronaut.data.annotation.DateCreated
import io.micronaut.data.annotation.DateUpdated
import io.micronaut.data.annotation.Id
import io.micronaut.data.annotation.MappedEntity
import io.micronaut.data.annotation.TypeDef
import io.micronaut.data.model.DataType
import java.time.OffsetDateTime
import java.util.Date
import java.util.UUID

@MappedEntity("scoped_configuration")
data class ScopedConfiguration(
  @field:Id
  var id: UUID,
  var key: String,
  var value: String,
  @field:TypeDef(type = DataType.OBJECT)
  var scopeType: ConfigScopeType,
  var scopeId: UUID,
  @field:TypeDef(type = DataType.OBJECT)
  var resourceType: ConfigResourceType,
  var resourceId: UUID,
  @field:TypeDef(type = DataType.OBJECT)
  var originType: ConfigOriginType,
  var origin: String,
  @Nullable
  var description: String? = null,
  @Nullable
  var referenceUrl: String? = null,
  @Nullable
  var expiresAt: Date? = null,
  @DateCreated
  var createdAt: OffsetDateTime? = null,
  @DateUpdated
  var updatedAt: OffsetDateTime? = null,
)
