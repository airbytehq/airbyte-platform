/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.oauth.declarative;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.hubspot.jinjava.Jinjava;
import com.hubspot.jinjava.JinjavaConfig;
import com.hubspot.jinjava.interpret.Context;
import com.hubspot.jinjava.interpret.JinjavaInterpreter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CodeChallengeS256FilterTest {

  private CodeChallengeS256Filter filter;
  private JinjavaInterpreter interpreter;

  @BeforeEach
  void setUp() {
    filter = new CodeChallengeS256Filter();
    final JinjavaConfig config = JinjavaConfig.newBuilder().build();
    interpreter = new JinjavaInterpreter(new Jinjava(), new Context(), config);
  }

  @Test
  void testFilterWithValidString() throws Exception {
    String input = "testValue";
    String expectedHash = "gv4Mg0y+oGkBPF63go5Zmmk+DSQRiH4qsnMnFmKXMII=";
    Object result = filter.filter(input, interpreter);
    assertEquals(expectedHash, result);
  }

  @Test
  void testFilterWithNonString() {
    Object input = 12345;
    Object result = filter.filter(input, interpreter);
    assertEquals(input, result);
  }

  @Test
  void testFilterWithException() {
    String input = "testValue";
    CodeChallengeS256Filter brokenFilter = new CodeChallengeS256Filter() {

      @Override
      protected String getCodeChallenge(String value) {
        throw new RuntimeException("SHA-256 algorithm not available");
      }

    };
    RuntimeException exception = assertThrows(RuntimeException.class, () -> {
      brokenFilter.filter(input, interpreter);
    });
    assertEquals("Failed to get `codechallengeS256` from: `testValue`", exception.getMessage());
  }

}
