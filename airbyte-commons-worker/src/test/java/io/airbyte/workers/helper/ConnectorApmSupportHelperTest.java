/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import io.airbyte.commons.constants.WorkerConstants;
import io.fabric8.kubernetes.api.model.EnvVar;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConnectorApmSupportHelperTest {

  private static final String CONNECTOR_NAME = "postgres";
  private static final String CONNECTOR_VERSION = "2.0.5";
  private static final String IMAGE = "postgres:2.0.5";
  private static final String REGISTRY_NAME = "registry.internal:1234";
  private static final String REPOSITORY_ORG = "airbyte";
  private static final String IMAGE_WITH_PORT = String.join("/", REGISTRY_NAME, REPOSITORY_ORG, IMAGE);
  private ConnectorApmSupportHelper supportHelper;

  @BeforeEach
  void setup() {
    supportHelper = new ConnectorApmSupportHelper();
  }

  @Test
  void testExtractAirbyteVersionFromImageName() {
    final String imageName = ConnectorApmSupportHelper.getImageName(IMAGE);
    final String imageVersion = ConnectorApmSupportHelper.getImageVersion(IMAGE);

    assertEquals(CONNECTOR_NAME, imageName);
    assertEquals(CONNECTOR_VERSION, imageVersion);
  }

  @Test
  void testExtractAirbyteVersionFromImageNameWithRegistryPort() {
    final String imageName = ConnectorApmSupportHelper.getImageName(IMAGE_WITH_PORT);
    final String imageVersion = ConnectorApmSupportHelper.getImageVersion(IMAGE_WITH_PORT);

    assertEquals(String.join("/", REGISTRY_NAME, REPOSITORY_ORG, CONNECTOR_NAME), imageName);
    assertEquals(CONNECTOR_VERSION, imageVersion);
  }

  @Test
  void testExtractAirbyteVersionFromBlankImageName() {
    final String blankString = "";
    final String nullString = null;

    assertEquals(blankString, ConnectorApmSupportHelper.getImageName(blankString));
    assertEquals(blankString, ConnectorApmSupportHelper.getImageVersion(blankString));
    assertEquals(nullString, ConnectorApmSupportHelper.getImageName(nullString));
    assertEquals(nullString, ConnectorApmSupportHelper.getImageVersion(nullString));
  }

  @Test
  void testAddServerNameAndVersionToEnvVars() {
    final List<EnvVar> envVars = new ArrayList<>();

    supportHelper.addServerNameAndVersionToEnvVars(IMAGE, envVars);

    assertEquals(2, envVars.size());
    assertTrue(envVars.contains(new EnvVar(io.airbyte.commons.envvar.EnvVar.DD_SERVICE.name(), CONNECTOR_NAME, null)));
    assertTrue(envVars.contains(new EnvVar(io.airbyte.commons.envvar.EnvVar.DD_VERSION.name(), CONNECTOR_VERSION, null)));
  }

  @Test
  void testAddServerNameAndVersionToEnvVarsNullImage() {
    final List<EnvVar> envVars = new ArrayList<>();
    assertThrows(NullPointerException.class, () -> supportHelper.addServerNameAndVersionToEnvVars(null, envVars));
  }

  @Test
  void testAddServerNameAndVersionToEnvVarsNullEnvVars() {
    assertThrows(NullPointerException.class, () -> supportHelper.addServerNameAndVersionToEnvVars(IMAGE, null));
  }

  @Test
  void testAddApmEnvVars() {
    final String ddAgentHost = "fake-agent-host";
    final String ddDogstatsdPort = "12345";
    final List<EnvVar> envVars = new ArrayList<>();
    final ConnectorApmSupportHelper supportHelper = spy(new ConnectorApmSupportHelper());
    when(supportHelper.getEnv()).thenReturn(Map.of(io.airbyte.commons.envvar.EnvVar.DD_AGENT_HOST.name(), ddAgentHost,
        io.airbyte.commons.envvar.EnvVar.DD_DOGSTATSD_PORT.name(), ddDogstatsdPort));

    supportHelper.addApmEnvVars(envVars);

    assertEquals(3, envVars.size());
    assertTrue(envVars.contains(new EnvVar(io.airbyte.commons.envvar.EnvVar.JAVA_OPTS.name(), WorkerConstants.DD_ENV_VAR, null)));
    assertTrue(envVars.contains(new EnvVar(io.airbyte.commons.envvar.EnvVar.DD_AGENT_HOST.name(), ddAgentHost, null)));
    assertTrue(envVars.contains(new EnvVar(io.airbyte.commons.envvar.EnvVar.DD_DOGSTATSD_PORT.name(), ddDogstatsdPort, null)));
  }

  @Test
  void testAddApmEnvVarsMissingEnvVars() {
    final List<EnvVar> envVars = new ArrayList<>();
    final ConnectorApmSupportHelper supportHelper = spy(new ConnectorApmSupportHelper());
    when(supportHelper.getEnv()).thenReturn(Map.of());

    supportHelper.addApmEnvVars(envVars);

    assertEquals(1, envVars.size());
    assertTrue(envVars.contains(new EnvVar(io.airbyte.commons.envvar.EnvVar.JAVA_OPTS.name(), WorkerConstants.DD_ENV_VAR, null)));
  }

}
