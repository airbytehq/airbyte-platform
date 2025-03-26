/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Lists;
import java.util.List;
import org.junit.jupiter.api.Test;

class ApplicationBeanFactoryTest {

  private static final String ROOT_PATH = "/path/to/root";

  @Test
  void testCreatePythonPathFromListOfPaths() {
    List<String> subdirectories = Lists.newArrayList("source-connector", "destination-connector");
    String pythonpath = ApplicationBeanFactory.createPythonPathFromListOfPaths(ROOT_PATH, subdirectories);

    String expectedPythonPath = "/path/to/root/source-connector:/path/to/root/destination-connector";

    assertEquals(expectedPythonPath, pythonpath);
  }

  @Test
  void testCreatePythonPathFromListOfPathsNoSubdirectories() {
    // This test case verifies the scenario where no local files are mounted
    List<String> subdirectories = Lists.newArrayList();
    String pythonpath = ApplicationBeanFactory.createPythonPathFromListOfPaths(ROOT_PATH, subdirectories);

    String expectedPythonPath = "";

    assertEquals(expectedPythonPath, pythonpath);
  }

}
