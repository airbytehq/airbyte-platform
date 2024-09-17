/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import jakarta.inject.Singleton;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DiagnosticToolHandler.
 */
@Singleton
public class DiagnosticToolHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobInputHandler.class);
  public static final String APPLICATION_YAML = "application.yaml";
  public static final String DEPLOYMENT_YAML = "deployment.yaml";
  public static final String DIAGNOSTIC_REPORT_FILE_NAME = "diagnostic_report";
  public static final String DIAGNOSTIC_REPORT_FILE_FORMAT = ".zip";

  private byte[] generateZipInMemory() throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    ZipOutputStream zipOut = new ZipOutputStream(byteArrayOutputStream);
    try {
      addApplicationYaml(zipOut);
    } catch (IOException e) {
      LOGGER.error("Error in writing application data.", e);
    }
    try {
      addDeploymentYaml(zipOut);
    } catch (IOException e) {
      LOGGER.error("Error in writing deployment data.", e);
    }
    zipOut.finish();
    return byteArrayOutputStream.toByteArray();
  }

  private void addApplicationYaml(ZipOutputStream zipOut) throws IOException {

    // In-memory construct an entry (application.yaml file) in the final zip output.
    ZipEntry applicationYaml = new ZipEntry(APPLICATION_YAML);
    zipOut.putNextEntry(applicationYaml);

    // Write application information to the zip entry: application.yaml.
    String applicationYamlContent = generateApplicationYaml();
    zipOut.write(applicationYamlContent.getBytes());
    zipOut.closeEntry();
  }

  private String generateApplicationYaml() {
    // Collect workspace information and write it to `application.yaml`.
    return collectWorkspaceInfo();
    // Collect other information here, e.g. logs
  }

  /**
   * Collect workspace information and write it to `application.yaml`.
   */
  private String collectWorkspaceInfo() {
    LOGGER.info("Collecting workspace data...");
    // TODO: implement this
    return "Workspaces:\n...";
  }

  private void addDeploymentYaml(ZipOutputStream zipOut) throws IOException {
    // In-memory construct an entry (deployment.yaml file) in the final zip output.
    ZipEntry zipEntry = new ZipEntry(DEPLOYMENT_YAML);
    zipOut.putNextEntry(zipEntry);
    String deploymentYamlContent = generateDeploymentYaml();
    zipOut.write(deploymentYamlContent.getBytes());
    zipOut.closeEntry();
  }

  /**
   * Collect Kubernetes information and write it to deployment_info.yaml.
   */
  private String generateDeploymentYaml() {
    LOGGER.info("Collecting k8s data...");
    // TODO: implement this
    return "k8s: \n";
  }

  /**
   * Generate diagnostic report by collecting relevant data and zipping them into a single file.
   *
   * @return File - generated zip file as a diagnostic report
   */
  public File generateDiagnosticReport() {
    try {
      // Generate the zip file content in memory as byte[]
      byte[] zipFileContent = generateZipInMemory();

      // Write the byte[] to a temporary file
      File tempFile = File.createTempFile(DIAGNOSTIC_REPORT_FILE_NAME, DIAGNOSTIC_REPORT_FILE_FORMAT);
      try (FileOutputStream fos = new FileOutputStream(tempFile)) {
        fos.write(zipFileContent);
      }

      // Return the temporary file
      return tempFile;

    } catch (IOException e) {
      LOGGER.error("Error generating diagnostic report", e);
      return null;
    }
  }

}
