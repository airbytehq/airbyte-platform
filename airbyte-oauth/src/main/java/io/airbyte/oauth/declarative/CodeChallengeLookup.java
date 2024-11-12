/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.declarative;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import org.apache.commons.text.lookup.StringLookup;

public class CodeChallengeLookup implements StringLookup {

  private final StringLookup defaultResolver;

  public CodeChallengeLookup(StringLookup defaultResolver) {
    this.defaultResolver = defaultResolver;
  }

  private static final String PATTERN = "codeChallenge:";
  private static final String ALGORITHM = "SHA-256";

  @Override
  public String lookup(String key) {

    if (key.startsWith(PATTERN)) {

      final String value = key.substring(PATTERN.length());

      try {
        final MessageDigest digest = MessageDigest.getInstance(ALGORITHM);
        return Base64.getEncoder().encodeToString(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(String.format("Failed to get `codeChallenge` from: `%s`", value), e);
      }

    }

    return defaultResolver.lookup(key);
  }

}
