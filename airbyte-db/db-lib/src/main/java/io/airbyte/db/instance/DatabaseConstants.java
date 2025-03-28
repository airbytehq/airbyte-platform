/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance;

import java.util.Collections;
import java.util.Set;

/**
 * Collection of database related constants.
 */
public final class DatabaseConstants {

  /**
   * Default Postgres version Airbyte supports.
   */
  public static final String DEFAULT_DATABASE_VERSION = "postgres:15-alpine";

  /**
   * Logical name of the Configurations database.
   */
  public static final String CONFIGS_DATABASE_LOGGING_NAME = "airbyte configs";

  /**
   * Collection of tables expected to be present in the Configurations database after creation.
   */
  public static final Set<String> CONFIGS_INITIAL_EXPECTED_TABLES = Collections.singleton("airbyte_configs");

  /**
   * Path to the script that contains the initial schema definition for the Configurations database.
   */
  public static final String CONFIGS_INITIAL_SCHEMA_PATH = "configs_database/schema.sql";

  public static final String CONFIGS_SCHEMA_DUMP_PATH = "src/main/resources/configs_database/schema_dump.txt";

  /**
   * Logical name of the Jobs database.
   */
  public static final String JOBS_DATABASE_LOGGING_NAME = "airbyte jobs";

  /**
   * Path to the script that contains the initial schema definition for the Jobs database.
   */
  public static final String JOBS_INITIAL_SCHEMA_PATH = "jobs_database/schema.sql";

  public static final String JOBS_SCHEMA_DUMP_PATH = "src/main/resources/jobs_database/schema_dump.txt";

  /**
   * Default database connection timeout in milliseconds.
   */
  public static final long DEFAULT_CONNECTION_TIMEOUT_MS = 30 * 1000;

  /**
   * Default amount of time to wait to assert that a database has been migrated, in milliseconds.
   */
  public static final long DEFAULT_ASSERT_DATABASE_TIMEOUT_MS = 2 * DEFAULT_CONNECTION_TIMEOUT_MS;

  public static final String CONNECTION_TABLE = "connection";

  public static final String NOTIFICATION_CONFIGURATION_TABLE = "notification_configuration";

  public static final String SCHEMA_MANAGEMENT_TABLE = "schema_management";

  public static final String USER_TABLE = "user";
  public static final String AUTH_USER_TABLE = "auth_user";

  public static final String PERMISSION_TABLE = "permission";

  public static final String WORKSPACE_TABLE = "workspace";

  public static final String ORGANIZATION_TABLE = "organization";

  public static final String USER_INVITATION_TABLE = "user_invitation";

  public static final String ORGANIZATION_EMAIL_DOMAIN_TABLE = "organization_email_domain";

  public static final String SSO_CONFIG_TABLE = "sso_config";

  public static final String ORGANIZATION_PAYMENT_CONFIG_TABLE = "organization_payment_config";

  /**
   * Private constructor to prevent instantiation.
   */
  private DatabaseConstants() {}

}
