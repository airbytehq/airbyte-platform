/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.utils

import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.utility.MountableFile
import java.io.IOException
import java.util.UUID

/**
 * Helper for doing common postgres test container setup.
 */
object PostgreSQLContainerHelper {
  /**
   * Setup database with a setup script.
   *
   * @param file file to execute
   * @param db database
   */
  @JvmStatic
  fun runSqlScript(
    file: MountableFile?,
    db: PostgreSQLContainer<*>,
  ) {
    try {
      val scriptPath = "/etc/" + UUID.randomUUID() + ".sql"
      db.copyFileToContainer(file, scriptPath)
      db.execInContainer(
        "psql",
        "-d",
        db.databaseName,
        "-U",
        db.username,
        "-a",
        "-f",
        scriptPath,
      )
    } catch (e: InterruptedException) {
      throw RuntimeException(e)
    } catch (e: IOException) {
      throw RuntimeException(e)
    }
  }
}
