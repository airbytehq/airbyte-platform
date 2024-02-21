/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.process;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashSet;
import java.util.List;
import org.junit.jupiter.api.Test;

class KubePortManagerSingletonTest {

  static final HashSet<Integer> PORTS = new HashSet<>(List.of(1, 2, 3, 4));
  static final HashSet<Integer> DIFFERENT_PORTS = new HashSet<>(List.of(5, 6, 7, 8));

  @Test
  void testInitialization() throws InterruptedException {
    KubePortManagerSingleton.init(PORTS);
    // Can be re-initialized with the same ports.
    assertDoesNotThrow(() -> KubePortManagerSingleton.init(PORTS));
    // Cannot be re-initialized with different ports.
    assertThrows(RuntimeException.class, () -> KubePortManagerSingleton.init(DIFFERENT_PORTS));
    // After we take a port, cannot be re-initialized with the original ports.
    KubePortManagerSingleton.getInstance().take();
    assertEquals(PORTS.size() - 1, KubePortManagerSingleton.getInstance().getNumAvailablePorts());
    assertThrows(RuntimeException.class, () -> KubePortManagerSingleton.init(PORTS));
  }

}
