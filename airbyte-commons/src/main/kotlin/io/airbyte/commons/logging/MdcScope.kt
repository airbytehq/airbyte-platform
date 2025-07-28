/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging

import org.slf4j.MDC
import java.lang.AutoCloseable

/**
 * This class is an autoClosable class that will add some specific values into the log MDC. When
 * being close, it will restore the original MDC. It is advised to use it like that:
 *
 * <pre>
 * `
 * try(final ScopedMDCChange scopedMDCChange = new ScopedMDCChange(
 * new HashMap<String, String>() {{
 * put("my", "value");
 * }}
 * )) {
 * ...
 * }
` *
</pre> *
 */
class MdcScope private constructor(
  keyValuesToAdd: MutableMap<String?, String?>,
) : AutoCloseable {
  private val originalContextMap: MutableMap<String?, String?>?

  init {
    originalContextMap = MDC.getCopyOfContextMap()

    keyValuesToAdd.forEach { (key: String?, `val`: String?) -> MDC.put(key, `val`) }
  }

  override fun close() {
    MDC.setContextMap(originalContextMap)
  }

  /**
   * Builder for an MdcScope.
   */
  class Builder {
    private val extraMdcEntries: MutableMap<String?, String?> = HashMap<String?, String?>()

    fun setExtraMdcEntries(keyValuesToAdd: Map<String?, String?>): Builder {
      this.extraMdcEntries.putAll(keyValuesToAdd)

      return this
    }

    fun setExtraMdcEntriesNonNullable(keyValuesToAdd: Map<String, String>): Builder {
      this.extraMdcEntries.putAll(keyValuesToAdd)

      return this
    }

    /**
     * Build the MdcScope.
     *
     * @return the MdcScope
     */
    fun build(): MdcScope = MdcScope(extraMdcEntries)
  }

  companion object {
    val DEFAULT_BUILDER: Builder = Builder()
  }
}
