package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Job
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface JobsRepository : PageableRepository<Job, Long>
