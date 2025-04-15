/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance

/**
 * Collection of database related constants.
 */
object DatabaseConstants {
  /**
   * Default Postgres version Airbyte supports.
   */
  const val DEFAULT_DATABASE_VERSION: String = "postgres:15-alpine"

  /**
   * Logical name of the Configurations database.
   */
  const val CONFIGS_DATABASE_LOGGING_NAME: String = "airbyte configs"

  /**
   * Collection of tables expected to be present in the Configurations database after creation.
   */
  val CONFIGS_INITIAL_EXPECTED_TABLES: Set<String> = setOf("airbyte_configs")

  /**
   * Path to the script that contains the initial schema definition for the Configurations database.
   */
  const val CONFIGS_INITIAL_SCHEMA_PATH: String = "configs_database/schema.sql"

  const val CONFIGS_SCHEMA_DUMP_PATH: String = "src/main/resources/configs_database/schema_dump.txt"

  /**
   * Logical name of the Jobs database.
   */
  const val JOBS_DATABASE_LOGGING_NAME: String = "airbyte jobs"

  /**
   * Path to the script that contains the initial schema definition for the Jobs database.
   */
  const val JOBS_INITIAL_SCHEMA_PATH: String = "jobs_database/schema.sql"

  const val JOBS_SCHEMA_DUMP_PATH: String = "src/main/resources/jobs_database/schema_dump.txt"

  /**
   * Default database connection timeout in milliseconds.
   */
  const val DEFAULT_CONNECTION_TIMEOUT_MS: Long = (30 * 1000).toLong()

  /**
   * Default amount of time to wait to assert that a database has been migrated, in milliseconds.
   */
  const val DEFAULT_ASSERT_DATABASE_TIMEOUT_MS: Long = 2 * DEFAULT_CONNECTION_TIMEOUT_MS

  const val CONNECTION_TABLE: String = "connection"

  const val NOTIFICATION_CONFIGURATION_TABLE: String = "notification_configuration"

  const val SCHEMA_MANAGEMENT_TABLE: String = "schema_management"

  const val USER_TABLE: String = "user"
  const val AUTH_USER_TABLE: String = "auth_user"

  const val PERMISSION_TABLE: String = "permission"

  const val WORKSPACE_TABLE: String = "workspace"

  const val ORGANIZATION_TABLE: String = "organization"

  const val USER_INVITATION_TABLE: String = "user_invitation"

  const val ORGANIZATION_EMAIL_DOMAIN_TABLE: String = "organization_email_domain"

  const val SSO_CONFIG_TABLE: String = "sso_config"

  const val ORGANIZATION_PAYMENT_CONFIG_TABLE: String = "organization_payment_config"
}
