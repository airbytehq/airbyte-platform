/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.declarative;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import org.apache.commons.text.lookup.StringLookup;

public class CodeChallengeS256Lookup implements StringLookup {

  private final StringLookup defaultResolver;
  private static final String PATTERN = "codeChallengeS256:";

  protected CodeChallengeS256Lookup(StringLookup defaultResolver) {
    this.defaultResolver = defaultResolver;
  }

  private String getCodeChallenge(final String value) throws Exception {
    final MessageDigest digest = MessageDigest.getInstance("SHA-256");
    return Base64.getEncoder().encodeToString(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
  }

  @Override
  public String lookup(String key) {
    if (key.startsWith(PATTERN)) {
      final String value = key.substring(PATTERN.length());

      try {
        return getCodeChallenge(value);
      } catch (Exception e) {
        final String errorMsg = String.format("Failed to get `codeChallengeS256` from: `%s`", value);
        throw new RuntimeException(errorMsg, e);
      }
    }

    return defaultResolver.lookup(key);
  }

}
