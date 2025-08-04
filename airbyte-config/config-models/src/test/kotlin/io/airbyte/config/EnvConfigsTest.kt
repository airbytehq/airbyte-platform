/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import io.airbyte.commons.envvar.EnvVar
import io.airbyte.commons.version.AirbyteVersion
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import java.nio.file.Paths
import java.util.List
import java.util.Map

internal class EnvConfigsTest {
  private lateinit var envMap: MutableMap<String?, String?>
  private lateinit var config: EnvConfigs

  @BeforeEach
  fun setUp() {
    envMap = HashMap<String?, String?>()
    config = EnvConfigs(envMap)
  }

  @Test
  fun ensureGetEnvBehavior() {
    Assertions.assertNull(System.getenv("MY_RANDOM_VAR_1234"))
  }

  @Test
  fun testAirbyteRole() {
    envMap[EnvVar.AIRBYTE_ROLE.name] = null
    Assertions.assertNull(config.getAirbyteRole())

    envMap[EnvVar.AIRBYTE_ROLE.name] = DEV
    Assertions.assertEquals(DEV, config.getAirbyteRole())
  }

  @Test
  fun testAirbyteVersion() {
    envMap[EnvVar.AIRBYTE_VERSION.name] = null
    Assertions.assertThrows<IllegalArgumentException?>(IllegalArgumentException::class.java, Executable { config.getAirbyteVersion() })

    envMap[EnvVar.AIRBYTE_VERSION.name] = DEV
    Assertions.assertEquals(AirbyteVersion(DEV), config.getAirbyteVersion())
  }

  @Test
  fun testWorkspaceRoot() {
    envMap[EnvVar.WORKSPACE_ROOT.name] = null
    Assertions.assertThrows<IllegalArgumentException?>(IllegalArgumentException::class.java, Executable { config.getWorkspaceRoot() })

    envMap[EnvVar.WORKSPACE_ROOT.name] = ABCDEF
    Assertions.assertEquals(Paths.get(ABCDEF), config.getWorkspaceRoot())
  }

  @Test
  fun testGetDatabaseUser() {
    envMap[EnvVar.DATABASE_USER.name] = null
    Assertions.assertThrows<IllegalArgumentException?>(IllegalArgumentException::class.java, Executable { config.getDatabaseUser() })

    envMap[EnvVar.DATABASE_USER.name] = "user"
    Assertions.assertEquals("user", config.getDatabaseUser())
  }

  @Test
  fun testGetDatabasePassword() {
    envMap[EnvVar.DATABASE_PASSWORD.name] = null
    Assertions.assertThrows<IllegalArgumentException?>(IllegalArgumentException::class.java, Executable { config.getDatabasePassword() })

    envMap[EnvVar.DATABASE_PASSWORD.name] = "password"
    Assertions.assertEquals("password", config.getDatabasePassword())
  }

  @Test
  fun testGetDatabaseUrl() {
    envMap[EnvVar.DATABASE_URL.name] = null
    Assertions.assertThrows<IllegalArgumentException?>(IllegalArgumentException::class.java, Executable { config.getDatabaseUrl() })

    envMap[EnvVar.DATABASE_URL.name] = "url"
    Assertions.assertEquals("url", config.getDatabaseUrl())
  }

  @Test
  fun testworkerKubeTolerations() {
    val airbyteServer = "airbyte-server"
    val noSchedule = "NoSchedule"

    envMap[EnvVar.JOB_KUBE_TOLERATIONS.name] = null
    Assertions.assertEquals(config.getJobKubeTolerations(), mutableListOf<Any?>())

    envMap[EnvVar.JOB_KUBE_TOLERATIONS.name] = ";;;"
    Assertions.assertEquals(config.getJobKubeTolerations(), mutableListOf<Any?>())

    envMap[EnvVar.JOB_KUBE_TOLERATIONS.name] = "key=k,value=v;"
    Assertions.assertEquals(config.getJobKubeTolerations(), mutableListOf<Any?>())

    envMap[EnvVar.JOB_KUBE_TOLERATIONS.name] = "key=airbyte-server,operator=Exists,effect=NoSchedule"
    Assertions.assertEquals(config.getJobKubeTolerations(), List.of<TolerationPOJO?>(TolerationPOJO(airbyteServer, noSchedule, null, "Exists")))

    envMap[EnvVar.JOB_KUBE_TOLERATIONS.name] = "key=airbyte-server,operator=Equals,value=true,effect=NoSchedule"
    Assertions.assertEquals(
      config.getJobKubeTolerations(),
      List.of<TolerationPOJO?>(TolerationPOJO(airbyteServer, noSchedule, "true", "Equals")),
    )

    envMap[EnvVar.JOB_KUBE_TOLERATIONS.name] =
      "key=airbyte-server,operator=Exists,effect=NoSchedule;key=airbyte-server,operator=Equals,value=true,effect=NoSchedule"
    Assertions.assertEquals(
      config.getJobKubeTolerations(),
      List.of<TolerationPOJO?>(
        TolerationPOJO(airbyteServer, noSchedule, null, "Exists"),
        TolerationPOJO(airbyteServer, noSchedule, "true", "Equals"),
      ),
    )
  }

  @Test
  fun testSplitKVPairsFromEnvString() {
    var input: String? = "key1=value1,key2=value2"
    var map = config.splitKVPairsFromEnvString(input)
    Assertions.assertNotNull(map)
    Assertions.assertEquals(2, map!!.size)
    Assertions.assertEquals(map, Map.of<String?, String?>("key1", "value1", "key2", "value2"))

    input = ENV_STRING
    map = config.splitKVPairsFromEnvString(input)
    Assertions.assertNotNull(map)
    Assertions.assertEquals(map, Map.of<String?, String?>(KEY, "k"))

    input = null
    map = config.splitKVPairsFromEnvString(input)
    Assertions.assertNull(map)

    input = " key1= value1,  key2 =    value2"
    map = config.splitKVPairsFromEnvString(input)
    Assertions.assertNotNull(map)
    Assertions.assertEquals(map, Map.of<String?, String?>("key1", "value1", "key2", "value2"))

    input = "key1:value1,key2:value2"
    map = config.splitKVPairsFromEnvString(input)
    Assertions.assertNull(map)
  }

  @Test
  fun testJobKubeNodeSelectors() {
    envMap[EnvVar.JOB_KUBE_NODE_SELECTORS.name] = null
    Assertions.assertNull(config.getJobKubeNodeSelectors())

    envMap[EnvVar.JOB_KUBE_NODE_SELECTORS.name] = NODE_SELECTORS
    Assertions.assertNull(config.getJobKubeNodeSelectors())

    envMap[EnvVar.JOB_KUBE_NODE_SELECTORS.name] = ENV_STRING
    Assertions.assertEquals(config.getJobKubeNodeSelectors(), Map.of<String?, String?>(KEY, "k"))

    envMap[EnvVar.JOB_KUBE_NODE_SELECTORS.name] = ONE_EQ_TWO
    Assertions.assertEquals(config.getJobKubeNodeSelectors(), Map.of<String?, String?>(ONE, TWO))

    envMap[EnvVar.JOB_KUBE_NODE_SELECTORS.name] = AIRB_SERV_SOME_NOTHING
    Assertions.assertEquals(config.getJobKubeNodeSelectors(), Map.of<String?, String?>(AIRBYTE, SERVER, SOMETHING, NOTHING))
  }

  companion object {
    private const val ABC = "abc"
    private const val DEV = "dev"
    private const val ABCDEF = "abc/def"
    private const val ROOT = "root"
    private const val KEY = "key"
    private const val ONE = "one"
    private const val TWO = "two"
    private const val ONE_EQ_TWO = "one=two"
    private const val NOTHING = "nothing"
    private const val SOMETHING = "something"
    private const val AIRBYTE = "airbyte"
    private const val SERVER = "server"
    private const val AIRB_SERV_SOME_NOTHING = "airbyte=server,something=nothing"
    private const val ENV_STRING = "key=k,,;$%&^#"
    private const val NODE_SELECTORS = ",,,"
  }
}
