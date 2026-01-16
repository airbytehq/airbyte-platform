/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.featureflag.server

import io.airbyte.featureflag.server.model.Context
import io.airbyte.featureflag.server.model.FeatureFlag
import io.airbyte.featureflag.server.model.Rule
import io.airbyte.micronaut.runtime.AirbyteFeatureFlagConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.nio.file.Path

class FeatureFlagServiceTest {
  private lateinit var airbyteFeatureFlagConfig: AirbyteFeatureFlagConfig
  private lateinit var ffs: FeatureFlagService

  @BeforeEach
  fun setup() {
    airbyteFeatureFlagConfig = AirbyteFeatureFlagConfig()
    ffs = FeatureFlagService(airbyteFeatureFlagConfig)
  }

  @Test
  fun `create and read a feature flag`() {
    val key = "sample-flag"
    val defaultValue = "something"

    val expected = FeatureFlag(key = key, default = defaultValue)
    val putResult = ffs.put(key = key, default = defaultValue)
    assertEquals(expected, putResult)

    val getResult = ffs.get(key)
    assertEquals(expected, getResult)
  }

  @Test
  fun `deleting a flag`() {
    val flag1 = FeatureFlag(key = "flag1", default = "flag1 default")
    val flag2 = FeatureFlag(key = "flag2", default = "flag2 default")

    ffs.put(flag1)
    assertEquals(flag1, ffs.get(flag1.key))
    ffs.put(flag2)
    ffs.delete(flag1.key)
    assertEquals(null, ffs.get(flag1.key))
    ffs.delete(flag1.key)
    assertEquals(null, ffs.get(flag1.key))
    assertEquals(flag2, ffs.get(flag2.key))
  }

  @Test
  fun `eval is a find first evaluation of the list of rules`() {
    val defaultValue = "default-value"
    val w1Value = "value-w1"
    val c1Value = "value-c1"
    val c2Value = "value-c2"

    val evalFlag =
      FeatureFlag(
        key = "eval-test",
        default = defaultValue,
        rules =
          listOf(
            Rule(context = Context(kind = "w", value = "w1"), value = w1Value),
            Rule(context = Context(kind = "c", value = "c1"), value = c1Value),
            Rule(context = Context(kind = "c", value = "c2"), value = c2Value),
          ),
      )
    ffs.put(evalFlag)

    assertEquals(defaultValue, ffs.eval(evalFlag.key, mapOf()))
    assertEquals(defaultValue, ffs.eval(evalFlag.key, mapOf("other" to "val")))
    assertEquals(w1Value, ffs.eval(evalFlag.key, mapOf("w" to "w1", "c" to "c1")))
    assertEquals(c1Value, ffs.eval(evalFlag.key, mapOf("w" to "w2", "c" to "c1")))
    assertEquals(c2Value, ffs.eval(evalFlag.key, mapOf("w" to "w2", "c" to "c2")))
    assertEquals(defaultValue, ffs.eval(evalFlag.key, mapOf("w" to "w2", "c" to "c3")))
    assertEquals(null, ffs.eval("no such flag", mapOf()))
  }

  @Test
  fun `key not found returns null`() {
    assertEquals(null, ffs.get("not found"))
  }

  @Test
  fun `modifying a flag`() {
    val controlFlag =
      FeatureFlag(key = "control", default = "false", rules = listOf(Rule(Context(kind = "workspace", value = "admin"), value = "true")))
    ffs.put(controlFlag)

    val initialFlag = FeatureFlag(key = "iterating", default = "default")
    ffs.put(initialFlag)

    val rule1 = Rule(context = Context(kind = "s", value = "s1"), value = "s1value")
    val expectedAdd1 = initialFlag.copy(rules = listOf(*initialFlag.rules.toTypedArray(), rule1))
    val add1Result = ffs.addRule(key = initialFlag.key, rule = rule1)
    assertEquals(expectedAdd1, add1Result)
    val getAdd1Result = ffs.get(initialFlag.key)
    assertEquals(expectedAdd1, getAdd1Result)

    val rule2 = Rule(context = Context(kind = "s", value = "s2"), value = "s2value")
    val expectedAdd2 = expectedAdd1.copy(rules = listOf(*expectedAdd1.rules.toTypedArray(), rule2))
    ffs.addRule(key = initialFlag.key, rule = rule2)
    val getAdd2Result = ffs.get(initialFlag.key)
    assertEquals(expectedAdd2, getAdd2Result)

    assertThrows<Exception> { ffs.addRule(key = initialFlag.key, rule = rule2) }
    val getAfterFailedAdd = ffs.get(initialFlag.key)
    assertEquals(expectedAdd2, getAfterFailedAdd)

    val rule3 = rule2.copy(value = "new s2value")
    val expectedAdd3 = expectedAdd1.copy(rules = listOf(*expectedAdd1.rules.toTypedArray(), rule3))
    val updateRule3Result = ffs.updateRule(key = initialFlag.key, rule = rule3)
    assertEquals(expectedAdd3, updateRule3Result)
    val getAdd3Result = ffs.get(initialFlag.key)
    assertEquals(expectedAdd3, getAdd3Result)

    assertThrows<Exception> { ffs.updateRule(key = initialFlag.key, rule = rule1.copy(context = Context(kind = "s", value = "s0"))) }
    val getAfterFailedUpdate = ffs.get(initialFlag.key)
    assertEquals(expectedAdd3, getAfterFailedUpdate)

    val removeResult = ffs.removeRule(key = initialFlag.key, context = rule1.context)
    val expectedRemove = initialFlag.copy(rules = listOf(rule3))
    assertEquals(expectedRemove, removeResult)
    val getAfterRemove = ffs.get(initialFlag.key)
    assertEquals(expectedRemove, getAfterRemove)

    val removeResult2 = ffs.removeRule(key = initialFlag.key, context = rule1.context)
    assertEquals(expectedRemove, removeResult2)

    val controlCheck = ffs.get(key = controlFlag.key)
    assertEquals(controlFlag, controlCheck)
  }

  @Test
  fun `put overrides the key`() {
    val controlFlag =
      FeatureFlag(key = "control", default = "false", rules = listOf(Rule(Context(kind = "workspace", value = "admin"), value = "true")))

    val key = "override-test"
    val initialDefault = "my first default"
    val rules =
      listOf(
        Rule(context = Context(kind = "workspace", value = "1"), value = "will get overridden"),
        Rule(context = Context(kind = "connection", value = "1"), value = "will get overridden too"),
      )

    ffs.put(controlFlag)

    val firstPut = ffs.put(key = key, default = initialDefault, rules = rules)
    assertEquals(FeatureFlag(key = key, default = initialDefault, rules = rules), firstPut)

    val overrideDefault = "new default"
    val secondPut = ffs.put(key = key, default = overrideDefault)
    assertEquals(FeatureFlag(key = key, default = overrideDefault), secondPut)

    val getResult = ffs.get(key = key)
    assertEquals(FeatureFlag(key = key, default = overrideDefault), getResult)

    val controlCheck = ffs.get(key = controlFlag.key)
    assertEquals(controlFlag, controlCheck)
  }

  @Test
  fun `verify file loading`() {
    val workspace = "workspace"
    val path = Path.of("src", "test", "resources", "flags.yml")
    val airbyteFeatureFlagConfig = AirbyteFeatureFlagConfig(path = path)
    val ffs = FeatureFlagService(airbyteFeatureFlagConfig = airbyteFeatureFlagConfig)

    assertEquals("true", ffs.get("test-true")?.default)
    assertEquals("false", ffs.get("test-false")?.default)
    assertEquals("example", ffs.get("test-string")?.default)
    assertEquals("1234", ffs.get("test-int")?.default)
    assertEquals("example", ffs.eval("test-string", mapOf(workspace to "any")))
    assertEquals("context", ffs.eval("test-string", mapOf(workspace to "00000000-aaaa-0000-aaaa-000000000000")))
    assertEquals("true", ffs.eval("test-context-boolean", mapOf()))
    assertEquals("false", ffs.eval("test-context-boolean", mapOf(workspace to "00000000-aaaa-0000-aaaa-000000000000")))
    assertEquals("cccc", ffs.eval("test-context-string", mapOf("connection" to "00000000-dddd-0000-dddd-000000000000")))
    assertEquals("aaaa", ffs.eval("test-context-string", mapOf(workspace to "00000000-aaaa-0000-aaaa-000000000000")))
    assertEquals("aaaa", ffs.eval("test-context-string", mapOf(workspace to "00000000-dddd-0000-dddd-000000000000")))
    // this is returning bbbb instead of aaaa because we return the first match
    assertEquals("bbbb", ffs.eval("test-context-string", mapOf(workspace to "00000000-bbbb-0000-bbbb-000000000000")))
  }
}
