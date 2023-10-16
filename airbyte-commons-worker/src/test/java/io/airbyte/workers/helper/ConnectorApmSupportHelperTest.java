/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper;

import static io.airbyte.config.EnvConfigs.DD_AGENT_HOST;
import static io.airbyte.config.EnvConfigs.DD_DOGSTATSD_PORT;
import static io.airbyte.workers.helper.ConnectorApmSupportHelper.DD_SERVICE;
import static io.airbyte.workers.helper.ConnectorApmSupportHelper.DD_VERSION;
import static io.airbyte.workers.helper.ConnectorApmSupportHelper.JAVA_OPTS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import io.airbyte.commons.constants.WorkerConstants;
import io.airbyte.config.EnvConfigs;
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
    assertTrue(envVars.contains(new EnvVar(DD_SERVICE, CONNECTOR_NAME, null)));
    assertTrue(envVars.contains(new EnvVar(DD_VERSION, CONNECTOR_VERSION, null)));
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
    when(supportHelper.getEnv()).thenReturn(Map.of(DD_AGENT_HOST, ddAgentHost, DD_DOGSTATSD_PORT, ddDogstatsdPort));

    supportHelper.addApmEnvVars(envVars);

    assertEquals(3, envVars.size());
    assertTrue(envVars.contains(new EnvVar(JAVA_OPTS, WorkerConstants.DD_ENV_VAR, null)));
    assertTrue(envVars.contains(new EnvVar(DD_AGENT_HOST, ddAgentHost, null)));
    assertTrue(envVars.contains(new EnvVar(EnvConfigs.DD_DOGSTATSD_PORT, ddDogstatsdPort, null)));
  }

  @Test
  void testAddApmEnvVarsMissingEnvVars() {
    final List<EnvVar> envVars = new ArrayList<>();
    final ConnectorApmSupportHelper supportHelper = spy(new ConnectorApmSupportHelper());
    when(supportHelper.getEnv()).thenReturn(Map.of());

    supportHelper.addApmEnvVars(envVars);

    assertEquals(1, envVars.size());
    assertTrue(envVars.contains(new EnvVar(JAVA_OPTS, WorkerConstants.DD_ENV_VAR, null)));
  }

}
