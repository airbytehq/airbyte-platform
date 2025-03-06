/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.jdbc;

import org.jooq.JSONFormat;

/**
 * Shared JDBC utils.
 */
public class JdbcUtils {

  public static final String HOST_KEY = "host";
  public static final String PORT_KEY = "port";
  public static final String DATABASE_KEY = "database";
  public static final String SCHEMA_KEY = "schema";

  public static final String USERNAME_KEY = "username";
  public static final String PASSWORD_KEY = "password";
  public static final String SSL_KEY = "ssl";
  public static final String JDBC_URL_PARAMS = "jdbc_url_params";

  private static final JSONFormat defaultJSONFormat = new JSONFormat().recordFormat(JSONFormat.RecordFormat.OBJECT);

  public static JSONFormat getDefaultJsonFormat() {
    return defaultJSONFormat;
  }

}
