/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import io.airbyte.config.init.CdkVersionProvider;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CachedCdkVersionProviderDecoratorTest {

  private static final String CDK_VERSION = "0.0.0";
  private CachedCdkVersionProviderDecorator cachedVersionProvider;
  private CdkVersionProvider delegate;

  @BeforeEach
  void setup() {
    delegate = mock(CdkVersionProvider.class);
    cachedVersionProvider = new CachedCdkVersionProviderDecorator(delegate);
  }

  @Test
  void whenGetCdkVersionThenReturnDelegateResult() {
    when(delegate.getCdkVersion()).thenReturn(CDK_VERSION);
    final String result = cachedVersionProvider.getCdkVersion();
    assertEquals(CDK_VERSION, result);
  }

  @Test
  void givenMultipleCallsWhenGetCdkVersionThenOnlyCallDelegateOnce() {
    when(delegate.getCdkVersion()).thenReturn(CDK_VERSION);
    IntStream.range(0, 10).forEach(unused -> cachedVersionProvider.getCdkVersion());
    verify(delegate, times(1)).getCdkVersion();
  }

}
