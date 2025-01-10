/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.declarative;

import org.apache.commons.text.lookup.StringLookup;

/**
 * The JinjaStringLookup class implements the StringLookup interface and provides a mechanism to
 * look up values based on a given key. If the key contains a specific pipe pattern ("|"), it splits
 * the key using a predefined split pattern ("\\|") and rearranges the parts to form a new key. The
 * new key is then used to perform the lookup. If the key does not contain the pipe pattern, the
 * original key is used for the lookup.
 *
 * This class is designed to work with a base resolver, which is another implementation of the
 * StringLookup interface, to perform the actual lookup operation.
 */
public class JinjaStringLookup implements StringLookup {

  private final StringLookup baseResolver;

  private final String pipePattern = "|";
  private final String splitPattern = "\\|";
  private static final int EXPECTED_PARTS_LENGTH = 2;

  protected JinjaStringLookup(StringLookup baseResolver) {
    this.baseResolver = baseResolver;
  }

  /**
   * Looks up a value based on the provided key. If the key contains a specific pipe pattern, it
   * splits the key using a predefined split pattern and rearranges the parts to form a new key. The
   * new key is then used to perform the lookup. If the key does not contain the pipe pattern, the
   * original key is used for the lookup.
   *
   * @param key the key to look up
   * @return the value associated with the key
   */
  @Override
  public String lookup(String key) {
    if (key.contains(pipePattern)) {
      final String[] parts = key.split(splitPattern);

      if (parts.length == EXPECTED_PARTS_LENGTH) {
        final String translatedKey = String.format("%s:%s", parts[1], parts[0]);
        return baseResolver.lookup(translatedKey);
      }
    }

    return baseResolver.lookup(key);
  }

}
