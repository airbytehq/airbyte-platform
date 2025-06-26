/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol

import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

@MicronautTest
internal class MigratorsMicronautTest {
  @Inject
  var messageMigrator: AirbyteMessageMigrator? = null

  @Inject
  var configuredAirbyteCatalogMigrator: ConfiguredAirbyteCatalogMigrator? = null

  @Test
  fun testAirbyteMessageMigrationInjection() {
    Assertions.assertEquals(SUPPORTED_VERSIONS, messageMigrator!!.migrationKeys)
  }

  @Test
  fun testConfiguredAirbyteCatalogMigrationInjection() {
    Assertions.assertEquals(SUPPORTED_VERSIONS, configuredAirbyteCatalogMigrator!!.migrationKeys)
  }

  companion object {
    // This should contain the list of all the supported majors of the airbyte protocol except the most
    // recent one since the migrations themselves are keyed on the lower version.
    private val SUPPORTED_VERSIONS = setOf<String>()
  }
}
