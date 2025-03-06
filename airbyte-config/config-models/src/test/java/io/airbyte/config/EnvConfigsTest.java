/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.airbyte.commons.envvar.EnvVar;
import io.airbyte.commons.version.AirbyteVersion;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.NullAssignment")
class EnvConfigsTest {

  private Map<String, String> envMap;
  private EnvConfigs config;
  private static final String ABC = "abc";
  private static final String DEV = "dev";
  private static final String ABCDEF = "abc/def";
  private static final String ROOT = "root";
  private static final String KEY = "key";
  private static final String ONE = "one";
  private static final String TWO = "two";
  private static final String ONE_EQ_TWO = "one=two";
  private static final String NOTHING = "nothing";
  private static final String SOMETHING = "something";
  private static final String AIRBYTE = "airbyte";
  private static final String SERVER = "server";
  private static final String AIRB_SERV_SOME_NOTHING = "airbyte=server,something=nothing";
  private static final String ENV_STRING = "key=k,,;$%&^#";
  private static final String NODE_SELECTORS = ",,,";

  @BeforeEach
  void setUp() {
    envMap = new HashMap<>();
    config = new EnvConfigs(envMap);
  }

  @Test
  void ensureGetEnvBehavior() {
    assertNull(System.getenv("MY_RANDOM_VAR_1234"));
  }

  @Test
  void testAirbyteRole() {
    envMap.put(EnvVar.AIRBYTE_ROLE.name(), null);
    assertNull(config.getAirbyteRole());

    envMap.put(EnvVar.AIRBYTE_ROLE.name(), DEV);
    assertEquals(DEV, config.getAirbyteRole());
  }

  @Test
  void testAirbyteVersion() {
    envMap.put(EnvVar.AIRBYTE_VERSION.name(), null);
    assertThrows(IllegalArgumentException.class, config::getAirbyteVersion);

    envMap.put(EnvVar.AIRBYTE_VERSION.name(), DEV);
    assertEquals(new AirbyteVersion(DEV), config.getAirbyteVersion());
  }

  @Test
  void testWorkspaceRoot() {
    envMap.put(EnvVar.WORKSPACE_ROOT.name(), null);
    assertThrows(IllegalArgumentException.class, config::getWorkspaceRoot);

    envMap.put(EnvVar.WORKSPACE_ROOT.name(), ABCDEF);
    assertEquals(Paths.get(ABCDEF), config.getWorkspaceRoot());
  }

  @Test
  void testGetDatabaseUser() {
    envMap.put(EnvVar.DATABASE_USER.name(), null);
    assertThrows(IllegalArgumentException.class, config::getDatabaseUser);

    envMap.put(EnvVar.DATABASE_USER.name(), "user");
    assertEquals("user", config.getDatabaseUser());
  }

  @Test
  void testGetDatabasePassword() {
    envMap.put(EnvVar.DATABASE_PASSWORD.name(), null);
    assertThrows(IllegalArgumentException.class, config::getDatabasePassword);

    envMap.put(EnvVar.DATABASE_PASSWORD.name(), "password");
    assertEquals("password", config.getDatabasePassword());
  }

  @Test
  void testGetDatabaseUrl() {
    envMap.put(EnvVar.DATABASE_URL.name(), null);
    assertThrows(IllegalArgumentException.class, config::getDatabaseUrl);

    envMap.put(EnvVar.DATABASE_URL.name(), "url");
    assertEquals("url", config.getDatabaseUrl());
  }

  @Test
  void testworkerKubeTolerations() {
    final String airbyteServer = "airbyte-server";
    final String noSchedule = "NoSchedule";

    envMap.put(EnvVar.JOB_KUBE_TOLERATIONS.name(), null);
    assertEquals(config.getJobKubeTolerations(), List.of());

    envMap.put(EnvVar.JOB_KUBE_TOLERATIONS.name(), ";;;");
    assertEquals(config.getJobKubeTolerations(), List.of());

    envMap.put(EnvVar.JOB_KUBE_TOLERATIONS.name(), "key=k,value=v;");
    assertEquals(config.getJobKubeTolerations(), List.of());

    envMap.put(EnvVar.JOB_KUBE_TOLERATIONS.name(), "key=airbyte-server,operator=Exists,effect=NoSchedule");
    assertEquals(config.getJobKubeTolerations(), List.of(new TolerationPOJO(airbyteServer, noSchedule, null, "Exists")));

    envMap.put(EnvVar.JOB_KUBE_TOLERATIONS.name(), "key=airbyte-server,operator=Equals,value=true,effect=NoSchedule");
    assertEquals(config.getJobKubeTolerations(), List.of(new TolerationPOJO(airbyteServer, noSchedule, "true", "Equals")));

    envMap.put(EnvVar.JOB_KUBE_TOLERATIONS.name(),
        "key=airbyte-server,operator=Exists,effect=NoSchedule;key=airbyte-server,operator=Equals,value=true,effect=NoSchedule");
    assertEquals(config.getJobKubeTolerations(), List.of(
        new TolerationPOJO(airbyteServer, noSchedule, null, "Exists"),
        new TolerationPOJO(airbyteServer, noSchedule, "true", "Equals")));
  }

  @Test
  void testSplitKVPairsFromEnvString() {
    String input = "key1=value1,key2=value2";
    Map<String, String> map = config.splitKVPairsFromEnvString(input);
    assertNotNull(map);
    assertEquals(2, map.size());
    assertEquals(map, Map.of("key1", "value1", "key2", "value2"));

    input = ENV_STRING;
    map = config.splitKVPairsFromEnvString(input);
    assertNotNull(map);
    assertEquals(map, Map.of(KEY, "k"));

    input = null;
    map = config.splitKVPairsFromEnvString(input);
    assertNull(map);

    input = " key1= value1,  key2 =    value2";
    map = config.splitKVPairsFromEnvString(input);
    assertNotNull(map);
    assertEquals(map, Map.of("key1", "value1", "key2", "value2"));

    input = "key1:value1,key2:value2";
    map = config.splitKVPairsFromEnvString(input);
    assertNull(map);
  }

  @Test
  void testJobKubeNodeSelectors() {
    envMap.put(EnvVar.JOB_KUBE_NODE_SELECTORS.name(), null);
    assertNull(config.getJobKubeNodeSelectors());

    envMap.put(EnvVar.JOB_KUBE_NODE_SELECTORS.name(), NODE_SELECTORS);
    assertNull(config.getJobKubeNodeSelectors());

    envMap.put(EnvVar.JOB_KUBE_NODE_SELECTORS.name(), ENV_STRING);
    assertEquals(config.getJobKubeNodeSelectors(), Map.of(KEY, "k"));

    envMap.put(EnvVar.JOB_KUBE_NODE_SELECTORS.name(), ONE_EQ_TWO);
    assertEquals(config.getJobKubeNodeSelectors(), Map.of(ONE, TWO));

    envMap.put(EnvVar.JOB_KUBE_NODE_SELECTORS.name(), AIRB_SERV_SOME_NOTHING);
    assertEquals(config.getJobKubeNodeSelectors(), Map.of(AIRBYTE, SERVER, SOMETHING, NOTHING));
  }

}
