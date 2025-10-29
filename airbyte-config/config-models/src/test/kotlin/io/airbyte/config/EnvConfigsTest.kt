/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import io.airbyte.commons.envvar.EnvVar
import io.airbyte.commons.version.AirbyteVersion
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.nio.file.Paths

internal class EnvConfigsTest {
  private lateinit var envMap: MutableMap<String?, String?>
  private lateinit var config: EnvConfigs

  @BeforeEach
  fun setUp() {
    envMap = mutableMapOf()
    config = EnvConfigs(envMap)
  }

  @Test
  fun ensureGetEnvBehavior() {
    assertNull(System.getenv("MY_RANDOM_VAR_1234"))
  }

  @Test
  fun testAirbyteVersion() {
    envMap[EnvVar.AIRBYTE_VERSION.name] = null
    assertThrows<IllegalArgumentException?>(IllegalArgumentException::class.java, { config.getAirbyteVersion() })

    envMap[EnvVar.AIRBYTE_VERSION.name] = DEV
    assertEquals(AirbyteVersion(DEV), config.getAirbyteVersion())
  }

  @Test
  fun testWorkspaceRoot() {
    envMap[EnvVar.WORKSPACE_ROOT.name] = null
    assertThrows<IllegalArgumentException?>(IllegalArgumentException::class.java, { config.getWorkspaceRoot() })

    envMap[EnvVar.WORKSPACE_ROOT.name] = ABCDEF
    assertEquals(Paths.get(ABCDEF), config.getWorkspaceRoot())
  }

  @Test
  fun testGetDatabaseUser() {
    envMap[EnvVar.DATABASE_USER.name] = null
    assertThrows<IllegalArgumentException?>(IllegalArgumentException::class.java, { config.getDatabaseUser() })

    envMap[EnvVar.DATABASE_USER.name] = "user"
    assertEquals("user", config.getDatabaseUser())
  }

  @Test
  fun testGetDatabasePassword() {
    envMap[EnvVar.DATABASE_PASSWORD.name] = null
    assertThrows<IllegalArgumentException?>(IllegalArgumentException::class.java, { config.getDatabasePassword() })

    envMap[EnvVar.DATABASE_PASSWORD.name] = "password"
    assertEquals("password", config.getDatabasePassword())
  }

  @Test
  fun testGetDatabaseUrl() {
    envMap[EnvVar.DATABASE_URL.name] = null
    assertThrows<IllegalArgumentException?>(IllegalArgumentException::class.java, { config.getDatabaseUrl() })

    envMap[EnvVar.DATABASE_URL.name] = "url"
    assertEquals("url", config.getDatabaseUrl())
  }

  @Test
  fun testworkerKubeTolerations() {
    val airbyteServer = "airbyte-server"
    val noSchedule = "NoSchedule"

    envMap[EnvVar.JOB_KUBE_TOLERATIONS.name] = null
    assertEquals(config.getJobKubeTolerations(), mutableListOf<Any?>())

    envMap[EnvVar.JOB_KUBE_TOLERATIONS.name] = ";;;"
    assertEquals(config.getJobKubeTolerations(), mutableListOf<Any?>())

    envMap[EnvVar.JOB_KUBE_TOLERATIONS.name] = "key=k,value=v;"
    assertEquals(config.getJobKubeTolerations(), mutableListOf<Any?>())

    envMap[EnvVar.JOB_KUBE_TOLERATIONS.name] = "key=airbyte-server,operator=Exists,effect=NoSchedule"
    assertEquals(config.getJobKubeTolerations(), listOf(TolerationPOJO(airbyteServer, noSchedule, null, "Exists")))

    envMap[EnvVar.JOB_KUBE_TOLERATIONS.name] = "key=airbyte-server,operator=Equals,value=true,effect=NoSchedule"
    assertEquals(
      config.getJobKubeTolerations(),
      listOf(TolerationPOJO(airbyteServer, noSchedule, "true", "Equals")),
    )

    envMap[EnvVar.JOB_KUBE_TOLERATIONS.name] =
      "key=airbyte-server,operator=Exists,effect=NoSchedule;key=airbyte-server,operator=Equals,value=true,effect=NoSchedule"
    assertEquals(
      config.getJobKubeTolerations(),
      listOf(
        TolerationPOJO(airbyteServer, noSchedule, null, "Exists"),
        TolerationPOJO(airbyteServer, noSchedule, "true", "Equals"),
      ),
    )
  }

  @Test
  fun testSplitKVPairsFromEnvString() {
    var input: String? = "key1=value1,key2=value2"
    var map = config.splitKVPairsFromEnvString(input)
    assertNotNull(map)
    assertEquals(2, map!!.size)
    assertEquals(map, mapOf("key1" to "value1", "key2" to "value2"))

    input = ENV_STRING
    map = config.splitKVPairsFromEnvString(input)
    assertNotNull(map)
    assertEquals(map, mapOf(KEY to "k"))

    input = null
    map = config.splitKVPairsFromEnvString(input)
    assertNull(map)

    input = " key1= value1,  key2 =    value2"
    map = config.splitKVPairsFromEnvString(input)
    assertNotNull(map)
    assertEquals(map, mapOf("key1" to "value1", "key2" to "value2"))

    input = "key1:value1,key2:value2"
    map = config.splitKVPairsFromEnvString(input)
    assertNull(map)
  }

  @Test
  fun testJobKubeNodeSelectors() {
    envMap[EnvVar.JOB_KUBE_NODE_SELECTORS.name] = null
    assertNull(config.getJobKubeNodeSelectors())

    envMap[EnvVar.JOB_KUBE_NODE_SELECTORS.name] = NODE_SELECTORS
    assertNull(config.getJobKubeNodeSelectors())

    envMap[EnvVar.JOB_KUBE_NODE_SELECTORS.name] = ENV_STRING
    assertEquals(config.getJobKubeNodeSelectors(), mapOf(KEY to "k"))

    envMap[EnvVar.JOB_KUBE_NODE_SELECTORS.name] = ONE_EQ_TWO
    assertEquals(config.getJobKubeNodeSelectors(), mapOf(ONE to TWO))

    envMap[EnvVar.JOB_KUBE_NODE_SELECTORS.name] = AIRB_SERV_SOME_NOTHING
    assertEquals(config.getJobKubeNodeSelectors(), mapOf(AIRBYTE to SERVER, SOMETHING to NOTHING))
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
