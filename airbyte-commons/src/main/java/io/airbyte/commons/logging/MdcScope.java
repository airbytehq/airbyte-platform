/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging;

import java.util.HashMap;
import java.util.Map;
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

    private final Map<String, String> extraMdcEntries = new HashMap<>();

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
      return new MdcScope(extraMdcEntries);
    }

  }

}
