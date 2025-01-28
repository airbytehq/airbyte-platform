/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.declarative;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import org.apache.commons.text.lookup.StringLookup;

/**
 * The CodeChallengeS256Lookup class implements the StringLookup interface and provides
 * functionality to generate a code challenge using the SHA-256 hashing algorithm. It checks if the
 * input key starts with a specific pattern and, if so, generates a code challenge for the extracted
 * value. If the key does not start with the pattern, it delegates the lookup to a default resolver.
 *
 * This class is useful for scenarios where you need to generate a code challenge for OAuth 2.0 PKCE
 * (Proof Key for Code Exchange) flow.
 */
public class CodeChallengeS256Lookup implements StringLookup {

  private final StringLookup defaultResolver;
  private static final String PATTERN = "codeChallengeS256:";

  protected CodeChallengeS256Lookup(StringLookup defaultResolver) {
    this.defaultResolver = defaultResolver;
  }

  /**
   * Generates a code challenge using the SHA-256 hashing algorithm. The input value is hashed and
   * then encoded to a Base64 string.
   *
   * @param value the input string to be hashed and encoded
   * @return the Base64 encoded SHA-256 hash of the input value
   * @throws Exception if the SHA-256 algorithm is not available
   */
  private String getCodeChallenge(final String value) throws Exception {
    final MessageDigest digest = MessageDigest.getInstance("SHA-256");
    return Base64.getEncoder().encodeToString(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
  }

  /**
   * Looks up the code challenge for the given key. If the key starts with a specific pattern, it
   * extracts the value after the pattern and attempts to generate a code challenge using the
   * extracted value. If the generation fails, it throws a RuntimeException with an error message. If
   * the key does not start with the pattern, it delegates the lookup to the default resolver.
   *
   * @param key the key to look up the code challenge for
   * @return the code challenge if the key starts with the pattern, otherwise the result of the
   *         default resolver lookup
   * @throws RuntimeException if the code challenge generation fails
   */
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
