/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.pod;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.airbyte.config.ResourceRequirements;
import io.fabric8.kubernetes.api.model.Quantity;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class PodUtilsTest {

  private static final String CPU = "cpu";
  private static final String MEMORY = "memory";

  @Test
  @DisplayName("Should build resource requirements.")
  void testBuildResourceRequirements() {
    final var reqs = PodUtils.getResourceRequirementsBuilder(new ResourceRequirements()
        .withCpuRequest("1")
        .withCpuLimit("2")
        .withMemoryRequest("1000Mi")
        .withMemoryLimit("1Gi"));
    final var actualReqs = reqs.build();

    assertEquals(new Quantity("1"), actualReqs.getRequests().get(CPU));
    assertEquals(new Quantity("2"), actualReqs.getLimits().get(CPU));
    assertEquals(new Quantity("1000Mi"), actualReqs.getRequests().get(MEMORY));
    assertEquals(new Quantity("1Gi"), actualReqs.getLimits().get(MEMORY));
  }

  @Test
  @DisplayName("Should build resource requirements with partial infos.")
  void testBuildResourceRequirementsWithPartialInfo() {
    final var reqs = PodUtils.getResourceRequirementsBuilder(new ResourceRequirements()
        .withCpuRequest("5")
        .withMemoryLimit("4Gi"));
    final var actualReqs = reqs.build();

    assertEquals(new Quantity("5"), actualReqs.getRequests().get(CPU));
    assertNull(actualReqs.getLimits().get(CPU));
    assertNull(actualReqs.getRequests().get(MEMORY));
    assertEquals(new Quantity("4Gi"), actualReqs.getLimits().get(MEMORY));
  }

  @Test
  @DisplayName("Should build resource requirements that don't have conflicts.")
  void testBuildResourceRequirementsShouldEnsureRequestFitsWithinLimits() {
    final var reqs = PodUtils.getResourceRequirementsBuilder(new ResourceRequirements()
        .withCpuRequest("1")
        .withCpuLimit("0.5")
        .withMemoryRequest("1000Mi")
        .withMemoryLimit("0.5Gi"));
    final var actualReqs = reqs.build();

    assertEquals(new Quantity("0.5"), actualReqs.getRequests().get(CPU));
    assertEquals(new Quantity("0.5"), actualReqs.getLimits().get(CPU));
    assertEquals(new Quantity("0.5Gi"), actualReqs.getRequests().get(MEMORY));
    assertEquals(new Quantity("0.5Gi"), actualReqs.getLimits().get(MEMORY));
  }

}
