/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging;

import com.google.common.annotations.VisibleForTesting;

/**
 * Shared code for handling logs.
 */
public class LoggingHelper {

  public static final String DESTINATION_LOGGER_PREFIX = "destination";
  public static final String SOURCE_LOGGER_PREFIX = "source";
  public static final String PLATFORM_LOGGER_PREFIX = "platform";

  /**
   * Color of log line.
   */
  public enum Color {

    BLUE_BACKGROUND("\u001b[44m"), // source
    YELLOW_BACKGROUND("\u001b[43m"), // destination
    CYAN_BACKGROUND("\u001b[46m"); // platform applications

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

  public static String destinationSource() {
    return applyColor(Color.YELLOW_BACKGROUND, DESTINATION_LOGGER_PREFIX);
  }

  public static String platformLogSource() {
    return applyColor(Color.CYAN_BACKGROUND, PLATFORM_LOGGER_PREFIX);
  }

  public static String sourceSource() {
    return applyColor(Color.BLUE_BACKGROUND, SOURCE_LOGGER_PREFIX);
  }

}
