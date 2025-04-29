/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.validation.json

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class JsonMergingHelperTest {
  private val objectMapper = ObjectMapper()
  private val helper = JsonMergingHelper()

  @Test
  fun `test combineProperties merges flat configurations correctly`() {
    // Create the template config (just the configuration content)
    val templateConfig = objectMapper.createObjectNode()
    templateConfig.put("host", "template-host")
    templateConfig.put("port", 5432)
    templateConfig.put("database", "template-db")

    // Create the user config (just the configuration content)
    val userConfig = objectMapper.createObjectNode()
    userConfig.put("host", "user-host")
    userConfig.put("username", "user")
    userConfig.put("password", "secret")

    // Combine the configs
    val result = helper.combineProperties(templateConfig, userConfig)

    // Assert the results
    Assertions.assertNotNull(result, "result should not be null")
    Assertions.assertEquals("user-host", result.get("host").asText())
    Assertions.assertEquals(5432, result.get("port").asInt())
    Assertions.assertEquals("template-db", result.get("database").asText())
    Assertions.assertEquals("user", result.get("username").asText())
    Assertions.assertEquals("secret", result.get("password").asText())
  }

  @Test
  fun `test combineProperties merges nested configurations correctly`() {
    // Create the template config with nested properties
    val templateConfig = objectMapper.createObjectNode()
    templateConfig.put("simple", "template-value")
    val templateNestedObj = templateConfig.putObject("nested")
    templateNestedObj.put("prop1", "template-prop1")
    templateNestedObj.put("prop2", "template-prop2")
    val templateDeepNested = templateNestedObj.putObject("deeper")
    templateDeepNested.put("deepProp", "template-deep")

    // Create the user config with nested properties
    val userConfig = objectMapper.createObjectNode()
    userConfig.put("simple", "user-value")
    val userNestedObj = userConfig.putObject("nested")
    userNestedObj.put("prop1", "user-prop1")
    userNestedObj.putArray("newArray").add("item1").add("item2")
    val userDeepNested = userNestedObj.putObject("deeper")
    userDeepNested.put("deepProp", "user-deep")
    userDeepNested.put("newDeepProp", "new-value")

    // Combine the configs
    val result = helper.combineProperties(templateConfig, userConfig)

    // Assert the results
    Assertions.assertNotNull(result, "result should not be null")
    Assertions.assertEquals("user-value", result.get("simple").asText())

    val resultNested = result.get("nested")
    Assertions.assertNotNull(resultNested)
    Assertions.assertTrue(resultNested.isObject)
    Assertions.assertEquals("user-prop1", resultNested.get("prop1").asText())
    Assertions.assertEquals("template-prop2", resultNested.get("prop2").asText())

    val resultArray = resultNested.get("newArray")
    Assertions.assertNotNull(resultArray)
    Assertions.assertTrue(resultArray.isArray)
    Assertions.assertEquals(2, resultArray.size())

    val resultDeeper = resultNested.get("deeper")
    Assertions.assertNotNull(resultDeeper)
    Assertions.assertEquals("user-deep", resultDeeper.get("deepProp").asText())
    Assertions.assertEquals("new-value", resultDeeper.get("newDeepProp").asText())
  }

  @Test
  fun `test combineProperties handles empty configurations`() {
    // Create empty objects for both configs
    val templateConfig = objectMapper.createObjectNode()
    val userConfig = objectMapper.createObjectNode()

    // Combine the configs
    val result = helper.combineProperties(templateConfig, userConfig)

    // Assert the result is an empty object
    Assertions.assertNotNull(result, "result should not be null")
    Assertions.assertTrue(result.isObject)
    Assertions.assertEquals(0, result.size())
  }

  @Test
  fun `test combineProperties handles null configurations`() {
    // Test with null template config
    val userConfig = objectMapper.createObjectNode()
    userConfig.put("key", "value")

    val resultWithNullTemplate = helper.combineProperties(null, userConfig)
    Assertions.assertNotNull(resultWithNullTemplate)
    Assertions.assertEquals("value", resultWithNullTemplate.get("key").asText())

    // Test with null user config
    val templateConfig = objectMapper.createObjectNode()
    templateConfig.put("key", "template-value")

    val resultWithNullUser = helper.combineProperties(templateConfig, null)
    Assertions.assertNotNull(resultWithNullUser)
    Assertions.assertEquals("template-value", resultWithNullUser.get("key").asText())
  }

  @Test
  fun `test combineProperties preserves non-overlapping nested objects`() {
    // Template config with a nested object
    val templateConfig = objectMapper.createObjectNode()
    val templateNestedObj = templateConfig.putObject("templateOnly")
    templateNestedObj.put("prop1", "value1")
    templateNestedObj.put("prop2", "value2")

    // User config with a different nested object
    val userConfig = objectMapper.createObjectNode()
    val userNestedObj = userConfig.putObject("userOnly")
    userNestedObj.put("prop3", "value3")
    userNestedObj.put("prop4", "value4")

    // Combine the configs
    val result = helper.combineProperties(templateConfig, userConfig)

    // Assert both nested objects are preserved
    Assertions.assertTrue(result.has("templateOnly"))
    Assertions.assertTrue(result.has("userOnly"))

    val templateOnlyResult = result.get("templateOnly")
    Assertions.assertEquals("value1", templateOnlyResult.get("prop1").asText())
    Assertions.assertEquals("value2", templateOnlyResult.get("prop2").asText())

    val userOnlyResult = result.get("userOnly")
    Assertions.assertEquals("value3", userOnlyResult.get("prop3").asText())
    Assertions.assertEquals("value4", userOnlyResult.get("prop4").asText())
  }

  @Test
  fun `test combineProperties gives precedence to user config for conflicting keys`() {
    // Template config with some values
    val templateConfig = objectMapper.createObjectNode()
    templateConfig.put("string", "template-string")
    templateConfig.put("number", 100)
    templateConfig.put("boolean", false)
    val templateNestedObj = templateConfig.putObject("nested")
    templateNestedObj.put("sharedProp", "template-value")

    // User config with overlapping values
    val userConfig = objectMapper.createObjectNode()
    userConfig.put("string", "user-string")
    userConfig.put("number", 200)
    userConfig.put("boolean", true)
    val userNestedObj = userConfig.putObject("nested")
    userNestedObj.put("sharedProp", "user-value")

    // Combine the configs
    val result = helper.combineProperties(templateConfig, userConfig)

    // Assert user values take precedence
    Assertions.assertEquals("user-string", result.get("string").asText())
    Assertions.assertEquals(200, result.get("number").asInt())
    Assertions.assertEquals(true, result.get("boolean").asBoolean())

    val nestedResult = result.get("nested")
    Assertions.assertEquals("user-value", nestedResult.get("sharedProp").asText())
  }
}
