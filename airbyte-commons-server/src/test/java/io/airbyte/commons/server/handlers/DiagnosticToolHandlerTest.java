/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * DiagnosticToolHandlerTest.
 */
@SuppressWarnings("PMD")
class DiagnosticToolHandlerTest {

  private DiagnosticToolHandler diagnosticToolHandler;

  @BeforeEach
  void beforeEach() {
    diagnosticToolHandler = new DiagnosticToolHandler();
  }

  @Test
  void testGenerateDiagnosticReport() throws IOException {
    final File zipFile = diagnosticToolHandler.generateDiagnosticReport();
    Assertions.assertTrue(zipFile.exists());
    // Check the content of the zip file
    try (FileInputStream fis = new FileInputStream(zipFile);
        ZipInputStream zis = new ZipInputStream(fis)) {

      ZipEntry entry;
      boolean foundApplicationYaml = false;
      boolean foundDeploymentYaml = false;

      // Iterate through the entries in the zip
      while ((entry = zis.getNextEntry()) != null) {
        if (entry.getName().equals(DiagnosticToolHandler.APPLICATION_YAML)) {
          foundApplicationYaml = true;

          // Check the content of application.yaml
          byte[] buffer = new byte[1024];
          int bytesRead;
          StringBuilder content = new StringBuilder();
          while ((bytesRead = zis.read(buffer)) != -1) {
            content.append(new String(buffer, 0, bytesRead));
          }
          Assertions.assertTrue(content.toString().contains("Workspaces"));
        } else if (entry.getName().equals(DiagnosticToolHandler.DEPLOYMENT_YAML)) {
          foundDeploymentYaml = true;

          // Check the content of deployment.yaml
          byte[] buffer = new byte[1024];
          int bytesRead;
          StringBuilder content = new StringBuilder();
          while ((bytesRead = zis.read(buffer)) != -1) {
            content.append(new String(buffer, 0, bytesRead));
          }
          Assertions.assertTrue(content.toString().contains("k8s"));
        }
      }

      // Ensure both files are present in the zip
      Assertions.assertTrue(foundApplicationYaml);
      Assertions.assertTrue(foundDeploymentYaml);
    }
  }

}
