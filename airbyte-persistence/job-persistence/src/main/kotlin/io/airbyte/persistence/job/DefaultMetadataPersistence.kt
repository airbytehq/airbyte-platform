/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job

import io.airbyte.db.Database
import io.github.oshai.kotlinlogging.KotlinLogging

private val log = KotlinLogging.logger {}

/**
 * Encapsulates jobs db interactions for the metadata domain models.
 */
class DefaultMetadataPersistence(
  private val jobDatabase: Database?,
) : MetadataPersistence {
  override fun placeholder() {
    log.info { "placeholder" }
    log.info { jobDatabase.toString() }
  }
}
