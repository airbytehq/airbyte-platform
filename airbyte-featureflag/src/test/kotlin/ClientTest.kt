/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.featureflag

import com.launchdarkly.sdk.LDContext
import com.launchdarkly.sdk.server.LDClient
import io.micronaut.context.annotation.Bean
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Replaces
import io.micronaut.test.annotation.MockBean
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.called
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.io.path.createTempFile
import kotlin.io.path.writeText
import kotlin.test.Ignore
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** workspaceId used across multiple tests */
private val workspaceId = UUID.randomUUID()

class ConfigFileClientTest {
  @Test
  fun `verify config-file functionality`() {
    val cfg = Path.of("src", "test", "resources", "flags.yml")
    val client: FeatureFlagClient = ConfigFileClient(cfg)

    // defined in flags.yml
    val testTrue = Temporary(key = "test-true", default = false)
    // defined in flags.yml
    val testFalse = Temporary(key = "test-false", default = true)
    // not defined as a boolean value
    val testNotBool = Temporary(key = "test-string", default = false)
    // not defined in flags.yml
    val testBoolDne = Temporary(key = "test-bool-dne", default = false)
    // defined in flags.yml
    val testStringExample = Temporary(key = "test-string", default = "not returned")
    // not defined in flags.yml
    val testStringDne = Temporary(key = "test-string-dne", default = "returned")

    val testIntExample = Temporary(key = "test-int", default = 1)
    // not defined in flags.yml
    val testIntDne = Temporary(key = "test-int-dne", default = 2)

    val ctx = Workspace(workspaceId)

    with(client) {
      assertTrue { boolVariation(testTrue, ctx) }
      assertFalse { boolVariation(testFalse, ctx) }
      assertFalse { boolVariation(testNotBool, ctx) }
      assertFalse { boolVariation(testBoolDne, ctx) }
      assertEquals("example", stringVariation(testStringExample, ctx))
      assertEquals(testStringDne.default, stringVariation(testStringDne, ctx))
      assertEquals(1234, intVariation(testIntExample, ctx))
      assertEquals(testIntDne.default, intVariation(testIntDne, ctx))
    }
  }

  @Test
  fun `verify no-config file returns default flag state`() {
    val client: FeatureFlagClient = ConfigFileClient(null)
    val defaultFalse = Temporary(key = "default-false", default = false)
    val defaultTrue = Temporary(key = "default-true", default = true)
    val defaultResponse = Temporary(key = "default-string", default = "response")
    val defaultInt = Temporary(key = "default-int", default = 4321)

    val ctx = Workspace(workspaceId)
    with(client) {
      assertTrue { boolVariation(defaultTrue, ctx) }
      assertFalse { boolVariation(defaultFalse, ctx) }
      assertEquals(defaultResponse.default, stringVariation(defaultResponse, ctx))
      assertEquals(defaultInt.default, intVariation(defaultInt, ctx))
    }
  }

  @Test
  fun `verify missing file returns default flag state`() {
    val client: FeatureFlagClient = ConfigFileClient(Path.of("src", "test", "resources", "feature-flags-dne-missing.yml"))
    val defaultFalse = Temporary(key = "default-false", default = false)
    val defaultTrue = Temporary(key = "default-true", default = true)
    val defaultResponse = Temporary(key = "default-string", default = "response")
    val defaultInt = Temporary(key = "default-int", default = 5678)

    val ctx = Workspace(workspaceId)
    with(client) {
      assertTrue { boolVariation(defaultTrue, ctx) }
      assertFalse { boolVariation(defaultFalse, ctx) }
      assertEquals(defaultResponse.default, stringVariation(defaultResponse, ctx))
      assertEquals(defaultInt.default, intVariation(defaultInt, ctx))
    }
  }

  @Test
  fun `verify directory instead of file returns default flag state`() {
    val client: FeatureFlagClient = ConfigFileClient(Path.of("src", "test", "resources"))
    val defaultFalse = Temporary(key = "default-false", default = false)
    val defaultTrue = Temporary(key = "default-true", default = true)
    val defaultResponse = Temporary(key = "default-string", default = "response")
    val defaultInt = Temporary(key = "default-int", default = 31254)

    val ctx = Workspace(workspaceId)
    with(client) {
      assertTrue { boolVariation(defaultTrue, ctx) }
      assertFalse { boolVariation(defaultFalse, ctx) }
      assertEquals(defaultResponse.default, stringVariation(defaultResponse, ctx))
      assertEquals(defaultInt.default, intVariation(defaultInt, ctx))
    }
  }

  /**
   * Ignore this test for now as it is unreliable in a unit-test scenario due to the
   * unpredictable nature of knowing when the WatchService (inside the ConfigFileClient) will
   * actually see the changed file.  Currently, this test sleeps for a few seconds, which works 90%
   * of the time, however there has been instances where it has taken over 20 seconds.
   *
   * TODO: move this to a different test suite
   */
  @Test
  @Ignore
  fun `verify config-file reload capabilities`() {
    val contents0 =
      """flags:
            |  - name: reload-test-true
            |    enabled: true
            |  - name: reload-test-false
            |    enabled: false
            |    
      """.trimMargin()
    val contents1 =
      """flags:
            |  - name: reload-test-true
            |    enabled: false
            |  - name: reload-test-false
            |    enabled: true
            |    
      """.trimMargin()

    // write to a temp config
    val tmpConfig =
      createTempFile(prefix = "reload-config", suffix = "yml").apply {
        writeText(contents0)
      }

    val client: FeatureFlagClient = ConfigFileClient(tmpConfig)

    // define the feature-flags
    val testTrue = Temporary(key = "reload-test-true", default = false)
    val testFalse = Temporary(key = "reload-test-false", default = true)
    val testDne = Temporary(key = "reload-test-dne", default = false)
    // and the context
    val ctx = Workspace(workspaceId)

    // verify pre-updated values
    with(client) {
      assertTrue { boolVariation(testTrue, ctx) }
      assertFalse { boolVariation(testFalse, ctx) }
      assertFalse { boolVariation(testDne, ctx) }
    }
    // update the config and wait a few seconds (enough time for the file-watcher to pick up the change)
    tmpConfig.writeText(contents1)
    TimeUnit.SECONDS.sleep(2)

    // verify post-updated values
    with(client) {
      assertFalse { boolVariation(testTrue, ctx) }
      assertTrue { boolVariation(testFalse, ctx) }
      assertFalse("undefined flag should still be false") { boolVariation(testDne, ctx) }
    }
  }

  @Test
  fun `verify env-var flag support`() {
    val cfg = Path.of("src", "test", "resources", "flags.yml")
    val client: FeatureFlagClient = ConfigFileClient(cfg)

    val evTrue = EnvVar(envVar = "env-true").apply { fetcher = { _ -> "true" } }
    val evFalse = EnvVar(envVar = "env-true").apply { fetcher = { _ -> "false" } }
    val evEmpty = EnvVar(envVar = "env-true").apply { fetcher = { _ -> "" } }
    val evNull = EnvVar(envVar = "env-true").apply { fetcher = { _ -> null } }

    val ctx = User("test")

    with(client) {
      assertTrue { boolVariation(evTrue, ctx) }
      assertFalse { boolVariation(evFalse, ctx) }
      assertFalse { boolVariation(evEmpty, ctx) }
      assertFalse { boolVariation(evNull, ctx) }
    }
  }

  @Test
  fun `verify context support`() {
    val cfg = Path.of("src", "test", "resources", "flags.yml")
    val client: FeatureFlagClient = ConfigFileClient(cfg)

    // included in one context override
    val uuidAAAA = UUID.fromString("00000000-aaaa-0000-aaaa-000000000000")
    // included in one context override
    val uuidBBBB = UUID.fromString("00000000-bbbb-0000-bbbb-000000000000")
    // included in no context overrides
    val uuidCCCC = UUID.fromString("00000000-cccc-0000-cccc-000000000000")
    // included in two context overrides
    val uuidDDDD = UUID.fromString("00000000-dddd-0000-dddd-000000000000")

    val flagCtxString = Temporary(key = "test-context-string", default = "default")
    val flagCtxBoolean = Temporary(key = "test-context-boolean", default = false)

    val ctxAAAA = Workspace(uuidAAAA)
    val ctxBBBB = Workspace(uuidBBBB)
    val ctxCCCC = Workspace(uuidCCCC)
    val ctxDDDD = Workspace(uuidDDDD)
    val multi = Multi(listOf(ctxAAAA, ctxBBBB))
    val multiRandom = Multi(listOf(Workspace(UUID.randomUUID()), Workspace(UUID.randomUUID())))
    val multiFindFirst = Multi(listOf(ctxAAAA, ctxBBBB))

    with(client) {
      assertFalse("aaaa should be false") { boolVariation(flagCtxBoolean, ctxAAAA) }
      assertTrue("bbbb should be true") { boolVariation(flagCtxBoolean, ctxBBBB) }
      assertTrue("cccc should be true") { boolVariation(flagCtxBoolean, ctxCCCC) }
      assertEquals("aaaa", stringVariation(flagCtxString, ctxAAAA), "aaab should be bbbb")
      assertEquals("bbbb", stringVariation(flagCtxString, ctxBBBB), "bbbb should be bbbb")
      assertEquals("all", stringVariation(flagCtxString, ctxCCCC), "cccc should be all (not included anywhere)")
      assertEquals("bbbb", stringVariation(flagCtxString, ctxDDDD), "dddd should be bbbb")
      assertEquals("aaaa", stringVariation(flagCtxString, multi), "dddd should be aaaa")
      assertEquals("all", stringVariation(flagCtxString, multiRandom), "dddd should be aaaa")
      assertEquals("aaaa", stringVariation(flagCtxString, multiFindFirst), "aaab should be bbbb")
    }
  }
}

class LaunchDarklyClientTest {
  @Test
  fun `verify cloud functionality`() {
    val testTrue = Temporary(key = "test-true", default = false)
    val testFalse = Temporary(key = "test-false", default = true)
    val testBoolDne = Temporary(key = "test-bool-dne", default = false)

    val ctx = Workspace(workspaceId)

    val ldClient: LDClient = mockk()
    val flag = slot<String>()

    every {
      ldClient.boolVariation(capture(flag), any<LDContext>(), any())
    } answers {
      when (flag.captured) {
        testTrue.key -> true
        testFalse.key, testBoolDne.key -> false
        else -> throw IllegalArgumentException("${flag.captured} was unexpected")
      }
    }

    val testStringExample = Temporary(key = "test-string", default = "not returned")
    val testStringDne = Temporary(key = "test-string-dne", default = "returned")
    every {
      ldClient.stringVariation(capture(flag), any<LDContext>(), any())
    } answers {
      when (flag.captured) {
        testStringExample.key -> "pretend override"
        testStringDne.key -> testStringDne.default
        else -> throw IllegalArgumentException("${flag.captured} was unexpected")
      }
    }

    val testInt = Temporary(key = "test-int", default = 1234)
    val testIntDne = Temporary(key = "test-int-dne", default = 4321)
    val intFlagValue = 32
    every {
      ldClient.intVariation(capture(flag), any<LDContext>(), any())
    } answers {
      when (flag.captured) {
        testInt.key -> intFlagValue
        testIntDne.key -> testIntDne.default
        else -> throw IllegalArgumentException("${flag.captured} was unexpected")
      }
    }

    val client: FeatureFlagClient = LaunchDarklyClient(ldClient)
    with(client) {
      assertTrue { boolVariation(testTrue, ctx) }
      assertFalse { boolVariation(testFalse, ctx) }
      assertFalse { boolVariation(testBoolDne, ctx) }
      assertEquals("pretend override", stringVariation(testStringExample, ctx))
      assertEquals(testStringDne.default, stringVariation(testStringDne, ctx))
      assertEquals(intFlagValue, intVariation(testInt, ctx))
      assertEquals(testIntDne.default, intVariation(testIntDne, ctx))
    }

    verify {
      ldClient.boolVariation(testTrue.key, any<LDContext>(), testTrue.default)
      ldClient.boolVariation(testFalse.key, any<LDContext>(), testFalse.default)
      ldClient.boolVariation(testBoolDne.key, any<LDContext>(), testBoolDne.default)
      ldClient.stringVariation(testStringExample.key, any<LDContext>(), testStringExample.default)
      ldClient.stringVariation(testStringDne.key, any<LDContext>(), testStringDne.default)
      ldClient.intVariation(testInt.key, any<LDContext>(), testInt.default)
      ldClient.intVariation(testIntDne.key, any<LDContext>(), testIntDne.default)
    }
  }

  @Test
  fun `verify env-var flag support`() {
    val ldClient: LDClient = mockk()
    val client: FeatureFlagClient = LaunchDarklyClient(ldClient)

    val evTrue = EnvVar(envVar = "env-true").apply { fetcher = { _ -> "true" } }
    val evFalse = EnvVar(envVar = "env-false").apply { fetcher = { _ -> "false" } }
    val evEmpty = EnvVar(envVar = "env-empty").apply { fetcher = { _ -> "" } }
    val evNull = EnvVar(envVar = "env-null").apply { fetcher = { _ -> null } }

    val ctx = User("test")

    with(client) {
      assertTrue { boolVariation(evTrue, ctx) }
      assertFalse { boolVariation(evFalse, ctx) }
      assertFalse { boolVariation(evEmpty, ctx) }
      assertFalse { boolVariation(evNull, ctx) }
    }

    // EnvVar flags should not interact with the LDClient
    verify { ldClient wasNot called }
  }

  @Test
  fun `verify ANONYMOUS context support`() {
    val testFlag = Temporary(key = "test-true", default = false)
    val ctxAnon = Workspace(ANONYMOUS)

    val ldClient: LDClient = mockk()
    val context = slot<LDContext>()
    every {
      ldClient.boolVariation(testFlag.key, capture(context), any())
    } answers {
      true
    }

    LaunchDarklyClient(ldClient).boolVariation(testFlag, ctxAnon)
    assertTrue(context.captured.isAnonymous)
  }
}

class TestClientTest {
  @Test
  fun `verify mutable functionality`() {
    val testTrue = Pair(Temporary(key = "test-true", default = false), true)
    val testFalse = Pair(Temporary(key = "test-false", default = true), false)
    val testBoolDne = Temporary(key = "test-bool-dne", default = false)
    val testStringExample = Pair(Temporary(key = "test-string", default = "example"), "non-default")
    val testStringDne = Temporary(key = "test-string-dne", default = "default")
    val nonDefaultIntValue = 4321
    val testInt = Pair(Temporary(key = "test-int", default = 1234), nonDefaultIntValue)
    val testIntDne = Temporary(key = "test-int-dne", default = 5678)

    val ctx = Workspace(workspaceId)
    // map of flag.key to value that should be returned
    val values: MutableMap<String, Any> =
      mutableMapOf(testTrue, testFalse, testStringExample, testInt)
        .mapKeys { it.key.key }
        .toMutableMap()

    val client: FeatureFlagClient = TestClient(values)
    with(client) {
      assertTrue { boolVariation(testTrue.first, ctx) }
      assertFalse { boolVariation(testFalse.first, ctx) }
      assertFalse { boolVariation(testBoolDne, ctx) }
      assertEquals("non-default", stringVariation(testStringExample.first, ctx))
      assertEquals(testStringDne.default, stringVariation(testStringDne, ctx))
      assertEquals(nonDefaultIntValue, intVariation(testInt.first, ctx))
      assertEquals(testIntDne.default, intVariation(testIntDne, ctx))
    }

    val anotherNonDefaultIntValue = 87654
    // modify the value, ensure the client reports the new modified value
    values[testTrue.first.key] = false
    values[testFalse.first.key] = true
    values[testStringExample.first.key] = "a different value"
    values[testInt.first.key] = anotherNonDefaultIntValue

    with(client) {
      assertFalse { boolVariation(testTrue.first, ctx) }
      assertTrue { boolVariation(testFalse.first, ctx) }
      assertFalse("undefined flags should always return false") { boolVariation(testBoolDne, ctx) }
      assertEquals("a different value", stringVariation(testStringExample.first, ctx))
      assertEquals(testStringDne.default, stringVariation(testStringDne, ctx))
      assertEquals(anotherNonDefaultIntValue, intVariation(testInt.first, ctx))
      assertEquals(testIntDne.default, intVariation(testIntDne, ctx))
    }
  }

  @Test
  fun `verify env-var flag support`() {
    val evTrue = EnvVar(envVar = "env-true")
    val evFalse = EnvVar(envVar = "env-false")
    val evEmpty = EnvVar(envVar = "env-empty")

    val ctx = User("test")

    val values =
      mutableMapOf(
        evTrue.key to true,
        evFalse.key to false,
      )
    val client: FeatureFlagClient = TestClient(values)

    with(client) {
      assertTrue { boolVariation(evTrue, ctx) }
      assertFalse { boolVariation(evFalse, ctx) }
      assertFalse { boolVariation(evEmpty, ctx) }
    }

    // modify the value, ensure the client reports the new modified value
    values[evTrue.key] = false
    values[evFalse.key] = true

    with(client) {
      assertFalse { boolVariation(evTrue, ctx) }
      assertTrue { boolVariation(evFalse, ctx) }
      assertFalse("undefined flags should always return false") { boolVariation(evEmpty, ctx) }
    }
  }
}

@MicronautTest(rebuildContext = true)
class InjectTest {
  @get:Bean
  @get:Replaces(LDClient::class)
  var ldClient: LDClient = mockk()

  private val flag = Temporary(key = "test-flag", default = true)
  private val context = Workspace("test-context")

  @BeforeEach
  fun setup() {
    clearMocks(ldClient)
  }

  @Inject
  lateinit var featureFlagClient: FeatureFlagClient

  @Test
  fun `ConfigFileClient loads if no client property defined`() {
    assertTrue { featureFlagClient is ConfigFileClient }
    assertTrue { featureFlagClient.boolVariation(flag, context) }
  }

  @Property(name = CONFIG_FF_CLIENT, value = "")
  @Test
  fun `ConfigFileClient loads if client property is empty`() {
    assertTrue { featureFlagClient is ConfigFileClient }
    assertTrue { featureFlagClient.boolVariation(flag, context) }
  }

  @Property(name = CONFIG_FF_CLIENT, value = "not-launchdarkly")
  @Test
  fun `ConfigFileClient loads if client property is not ${CONFIG_FF_CLIENT_VAL_LAUNCHDARKLY}`() {
    assertTrue { featureFlagClient is ConfigFileClient }
    assertTrue { featureFlagClient.boolVariation(flag, context) }
  }

  @Property(name = CONFIG_FF_CLIENT, value = CONFIG_FF_CLIENT_VAL_LAUNCHDARKLY)
  @Test
  fun `LaunchDarklyClient loads if client is defined as ${CONFIG_FF_CLIENT_VAL_LAUNCHDARKLY}`() {
    every { ldClient.boolVariation(flag.key, any<LDContext>(), flag.default) } returns flag.default

    assertTrue { featureFlagClient is LaunchDarklyClient }
    assertTrue { featureFlagClient.boolVariation(flag, context) }
  }
}

@MicronautTest(rebuildContext = true)
class NonMockBeanTest {
  @Singleton
  class Dummy(val ffClient: FeatureFlagClient)

  @Inject
  lateinit var dummy: Dummy

  @Test
  fun `ensure ConfigFileClient is injected when no MockBean is defined`() {
    assertTrue { dummy.ffClient is ConfigFileClient }
  }
}

@MicronautTest(rebuildContext = true)
class MockBeanTest {
  @Singleton
  class Dummy(val ffClient: FeatureFlagClient)

  @Inject
  lateinit var dummy: Dummy

  @Test
  fun `ensure MockBean is injected when @MockBean is defined`() {
    assertTrue { dummy.ffClient is TestClient }
  }

  @MockBean(FeatureFlagClient::class)
  fun featureFlagClient(): TestClient = mockk<TestClient>()
}
