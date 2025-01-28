/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.declarative;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.NoSuchAlgorithmException;
import org.apache.commons.text.lookup.StringLookup;
import org.junit.jupiter.api.Test;

class CodeChallengeS256LookupTest {

  private final StringLookup defaultResolver = mock(StringLookup.class);
  private final CodeChallengeS256Lookup lookup = new CodeChallengeS256Lookup(defaultResolver);

  @Test
  void testLookupWithCodeChallenge() throws NoSuchAlgorithmException {
    final String key = "codeChallengeS256:testValue";

    // the expectedHash is the Base64 from the `testValue` string.
    final String expectedHash = "gv4Mg0y+oGkBPF63go5Zmmk+DSQRiH4qsnMnFmKXMII=";
    final String result = lookup.lookup(key);
    assertEquals(expectedHash, result);
  }

  @Test
  void testLookupWithoutCodeChallenge() {
    final String key = "someOtherKey";
    final String expectedValue = "someValue";

    when(defaultResolver.lookup(key)).thenReturn(expectedValue);
    final String result = lookup.lookup(key);
    assertEquals(expectedValue, result);
  }

}
