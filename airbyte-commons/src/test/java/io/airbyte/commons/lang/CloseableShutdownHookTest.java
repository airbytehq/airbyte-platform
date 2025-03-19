/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.lang;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.InputStream;
import org.junit.jupiter.api.Test;

class CloseableShutdownHookTest {

  @Test
  void testRegisteringShutdownHook() throws Exception {
    final InputStream closeable = mock(InputStream.class);
    final String notCloseable = "Not closeable";

    final Thread thread = CloseableShutdownHook.buildShutdownHookThread(closeable, notCloseable, null);
    thread.run();

    verify(closeable, times(1)).close();
  }

}
