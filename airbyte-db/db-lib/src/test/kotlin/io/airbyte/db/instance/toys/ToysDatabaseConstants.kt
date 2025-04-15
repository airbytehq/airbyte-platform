/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.toys

/**
 * Collection of toys database related constants.
 */
object ToysDatabaseConstants {
  /**
   * Logical name of the Toys database.
   */
  const val DATABASE_LOGGING_NAME: String = "toys"

  /**
   * Expected table to be present in the Toys database after creation.
   */
  const val TABLE_NAME: String = "toy_cars"

  /**
   * Path to the script that contains the initial schema definition for the Toys database.
   */
  const val SCHEMA_PATH: String = "toys_database/schema.sql"
}
