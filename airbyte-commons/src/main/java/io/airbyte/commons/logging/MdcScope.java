/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging;

import io.airbyte.commons.logging.LoggingHelper.Color;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.slf4j.MDC;

/**
 * This class is an autoClosable class that will add some specific values into the log MDC. When
 * being close, it will restore the original MDC. It is advised to use it like that:
 *
 * <pre>
 *   <code>
 *     try(final ScopedMDCChange scopedMDCChange = new ScopedMDCChange(
 *      new HashMap&lt;String, String&gt;() {{
 *        put("my", "value");
 *      }}
 *     )) {
 *        ...
 *     }
 *   </code>
 * </pre>
 */
public class MdcScope implements AutoCloseable {

  public static final MdcScope.Builder DEFAULT_BUILDER = new Builder();

  private final Map<String, String> originalContextMap;

  private MdcScope(final Map<String, String> keyValuesToAdd) {
    originalContextMap = MDC.getCopyOfContextMap();

    keyValuesToAdd.forEach(MDC::put);
  }

  @Override
  public void close() {
    MDC.setContextMap(originalContextMap);
  }

  /**
   * Builder for an MdcScope.
   */
  public static class Builder {

    private Optional<String> maybeLogPrefix = Optional.empty();
    private Optional<Color> maybePrefixColor = Optional.empty();
    private final Map<String, String> extraMdcEntries = new HashMap<>();

    /**
     * Set the prefix for log lines in this scope.
     *
     * @param logPrefix prefix for log lines
     * @return the builder
     */
    public Builder setLogPrefix(final String logPrefix) {
      this.maybeLogPrefix = Optional.ofNullable(logPrefix);

      return this;
    }

    /**
     * Set the color for log lines in this scope.
     *
     * @param color of log line
     * @return the builder
     */
    public Builder setPrefixColor(final Color color) {
      this.maybePrefixColor = Optional.ofNullable(color);

      return this;
    }

    public Builder setExtraMdcEntries(final Map<String, String> keyValuesToAdd) {
      this.extraMdcEntries.putAll(keyValuesToAdd);

      return this;
    }

    /**
     * Build the MdcScope.
     *
     * @return the MdcScope
     */
    public MdcScope build() {
      maybeLogPrefix.ifPresent(logPrefix -> {
        final String potentiallyColoredLog = maybePrefixColor
            .map(color -> LoggingHelper.applyColor(color, logPrefix))
            .orElse(logPrefix);

        extraMdcEntries.put(LoggingHelper.LOG_SOURCE_MDC_KEY, potentiallyColoredLog);
      });

      return new MdcScope(extraMdcEntries);
    }

  }

}
