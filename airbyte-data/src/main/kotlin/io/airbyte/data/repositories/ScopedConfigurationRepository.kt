package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.ScopedConfiguration
import io.airbyte.db.instance.configs.jooq.generated.enums.ConfigResourceType
import io.airbyte.db.instance.configs.jooq.generated.enums.ConfigScopeType
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface ScopedConfigurationRepository : PageableRepository<ScopedConfiguration, UUID> {
  fun getByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeId(
    key: String,
    resourceType: ConfigResourceType,
    resourceId: UUID,
    scopeType: ConfigScopeType,
    scopeId: UUID,
  ): ScopedConfiguration?
}
