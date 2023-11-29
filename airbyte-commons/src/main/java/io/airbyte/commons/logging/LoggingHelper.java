/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging;

import com.google.common.annotations.VisibleForTesting;

/**
 * Shared code for handling logs.
 */
public class LoggingHelper {

  public static final String CUSTOM_TRANSFORMATION_LOGGER_PREFIX = "dbt";
  public static final String DESTINATION_LOGGER_PREFIX = "destination";
  public static final String NORMALIZATION_LOGGER_PREFIX = "normalization";
  public static final String SOURCE_LOGGER_PREFIX = "source";
  public static final String PLATFORM_LOGGER_PREFIX = "platform";

  /**
   * Color of log line.
   */
  public enum Color {

    BLUE_BACKGROUND("\u001b[44m"), // source
    YELLOW_BACKGROUND("\u001b[43m"), // destination
    GREEN_BACKGROUND("\u001b[42m"), // normalization
    CYAN_BACKGROUND("\u001b[46m"), // platform applications
    PURPLE_BACKGROUND("\u001b[45m"); // dbt

    private final String ansi;

    Color(final String ansiCode) {
      this.ansi = ansiCode;
    }

    public String getCode() {
      return ansi;
    }

  }

  public static final String LOG_SOURCE_MDC_KEY = "log_source";

  @VisibleForTesting
  public static final String RESET = "\u001B[0m";

  public static String applyColor(final Color color, final String msg) {
    return color.getCode() + msg + RESET;
  }

  public static String platformLogSource() {
    return applyColor(Color.CYAN_BACKGROUND, PLATFORM_LOGGER_PREFIX);
  }

}
