/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.resources;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.Sets;
import io.airbyte.commons.io.IOs;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class MoreResourcesTest {

  private static final String CONTENT_1 = "content1\n";
  private static final String CONTENT_2 = "content2\n";
  private static final String RESOURCE_TEST = "resource_test";

  @Test
  void testResourceRead() throws IOException {
    assertEquals(CONTENT_1, MoreResources.readResource(RESOURCE_TEST));
    assertEquals(CONTENT_2, MoreResources.readResource("subdir/resource_test_sub"));

    assertThrows(IllegalArgumentException.class, () -> MoreResources.readResource("invalid"));
  }

  @Test
  void testResourceReadWithClass() throws IOException {
    assertEquals(CONTENT_1, MoreResources.readResource(MoreResourcesTest.class, RESOURCE_TEST));
    assertEquals(CONTENT_2, MoreResources.readResource(MoreResourcesTest.class, "subdir/resource_test_sub"));

    assertEquals(CONTENT_1, MoreResources.readResource(MoreResourcesTest.class, "/resource_test"));
    assertEquals(CONTENT_2, MoreResources.readResource(MoreResourcesTest.class, "/subdir/resource_test_sub"));

    assertThrows(IllegalArgumentException.class, () -> MoreResources.readResource(MoreResourcesTest.class, "invalid"));
  }

  @Test
  void testReadResourceAsFile() throws URISyntaxException {
    final File file = MoreResources.readResourceAsFile(RESOURCE_TEST);
    assertEquals(CONTENT_1, IOs.readFile(file.toPath()));
  }

  @Test
  void testResourceReadDuplicateName() throws IOException {
    assertEquals(CONTENT_1, MoreResources.readResource("resource_test_a"));
    assertEquals(CONTENT_2, MoreResources.readResource("subdir/resource_test_a"));
  }

  @Test
  void testListResource() throws IOException {
    assertEquals(
        Sets.newHashSet("subdir", "resource_test_sub", "resource_test_sub_2", "resource_test_a"),
        MoreResources.listResources(MoreResourcesTest.class, "subdir")
            .map(Path::getFileName)
            .map(Path::toString)
            .collect(Collectors.toSet()));
  }

}
