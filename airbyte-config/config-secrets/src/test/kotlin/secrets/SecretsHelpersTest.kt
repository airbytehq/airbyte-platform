/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import io.airbyte.config.secrets.persistence.ReadOnlySecretPersistence
import io.airbyte.config.secrets.persistence.SecretPersistence
import io.airbyte.config.secrets.test.cases.ArrayOneOfTestCase
import io.airbyte.config.secrets.test.cases.ArrayTestCase
import io.airbyte.config.secrets.test.cases.NestedObjectTestCase
import io.airbyte.config.secrets.test.cases.NestedOneOfTestCase
import io.airbyte.config.secrets.test.cases.OneOfSecretTestCase
import io.airbyte.config.secrets.test.cases.OneOfTestCase
import io.airbyte.config.secrets.test.cases.OptionalPasswordTestCase
import io.airbyte.config.secrets.test.cases.PostgresSshKeyTestCase
import io.airbyte.config.secrets.test.cases.SimpleTestCase
import io.airbyte.validation.json.JsonSchemaValidator
import io.airbyte.validation.json.JsonValidationException
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.io.IOException
import java.util.UUID
import java.util.function.Consumer
import java.util.regex.Pattern
import java.util.stream.Stream

private const val PROVIDE_TEST_CASES = "provideTestCases"

internal class SecretsHelpersTest {
  private val secretPresistence: SecretPersistence = MemorySecretPersistence()

  @ParameterizedTest
  @MethodSource(PROVIDE_TEST_CASES)
  @Throws(
    JsonValidationException::class,
  )
  fun validateTestCases(testCase: SecretsTestCase) {
    val validator = JsonSchemaValidator()
    val spec: JsonNode = testCase.spec.connectionSpecification
    validator.ensure(spec, testCase.fullConfig)
    validator.ensure(spec, testCase.updateConfig)
  }

  @ParameterizedTest
  @MethodSource(PROVIDE_TEST_CASES)
  fun testSplit(testCase: SecretsTestCase) {
    val uuidIterator = SecretsTestCase.UUIDS.iterator()
    val inputConfig: JsonNode = testCase.fullConfig
    val inputConfigCopy = inputConfig.deepCopy<JsonNode>()
    val splitConfig: SplitSecretConfig =
      SecretsHelpers.splitConfig(
        { uuidIterator.next() },
        SecretsTestCase.WORKSPACE_ID,
        inputConfig,
        testCase.spec.connectionSpecification,
        secretPresistence,
      )
    Assertions.assertEquals(testCase.partialConfig, splitConfig.partialConfig)
    Assertions.assertEquals(testCase.firstSecretMap, splitConfig.getCoordinateToPayload())

    // check that we didn't mutate the input configs
    Assertions.assertEquals(inputConfigCopy, inputConfig)

    // check that keys for Google Secrets Manger fit the requirements:
    // A secret ID is a string with a maximum length of 255 characters and can contain
    // uppercase and lowercase letters, numerals, and the hyphen (-) and underscore (_) characters.
    // https://cloud.google.com/secret-manager/docs/reference/rpc/google.cloud.secretmanager.v1#createsecretrequest
    val gsmKeyCharacterPattern = Pattern.compile("^[a-zA-Z0-9_-]+$")

    // sanity check pattern with a character that isn't allowed
    Assertions.assertFalse(gsmKeyCharacterPattern.matcher("/").matches())

    // check every key for the pattern and max length
    splitConfig.getCoordinateToPayload().keys.forEach(
      Consumer<SecretCoordinate> { key: SecretCoordinate ->
        Assertions.assertTrue(
          gsmKeyCharacterPattern.matcher(key.fullCoordinate).matches(),
          "Invalid character in key: $key",
        )
        Assertions.assertTrue(
          key.toString().length <= 255,
          "Key is too long: " + key.toString().length,
        )
      },
    )
  }

  @ParameterizedTest
  @MethodSource(PROVIDE_TEST_CASES)
  fun testSplitUpdate(testCase: SecretsTestCase) {
    val uuidIterator = SecretsTestCase.UUIDS.iterator()
    val inputPartialConfig: JsonNode = testCase.partialConfig
    val inputUpdateConfig: JsonNode = testCase.updateConfig
    val inputPartialConfigCopy = inputPartialConfig.deepCopy<JsonNode>()
    val inputUpdateConfigCopy = inputUpdateConfig.deepCopy<JsonNode>()
    val secretPersistence = MemorySecretPersistence()
    for ((key, value) in testCase.firstSecretMap.entries) {
      secretPersistence.write(key, value)
    }
    val updatedSplit: SplitSecretConfig =
      SecretsHelpers.splitAndUpdateConfig(
        { uuidIterator.next() },
        SecretsTestCase.WORKSPACE_ID,
        inputPartialConfig,
        inputUpdateConfig,
        testCase.spec.connectionSpecification,
        { coordinate: SecretCoordinate ->
          secretPersistence.read(
            coordinate,
          )
        },
      )
    Assertions.assertEquals(testCase.updatedPartialConfig, updatedSplit.partialConfig)
    Assertions.assertEquals(testCase.secondSecretMap, updatedSplit.getCoordinateToPayload())

    // check that we didn't mutate the input configs
    Assertions.assertEquals(inputPartialConfigCopy, inputPartialConfig)
    Assertions.assertEquals(inputUpdateConfigCopy, inputUpdateConfig)
  }

  @ParameterizedTest
  @MethodSource(PROVIDE_TEST_CASES)
  fun testCombine(testCase: SecretsTestCase) {
    val secretPersistence = MemorySecretPersistence()
    testCase.persistenceUpdater.accept(secretPersistence)
    val inputPartialConfig: JsonNode = testCase.partialConfig
    val inputPartialConfigCopy = inputPartialConfig.deepCopy<JsonNode>()
    val actualCombinedConfig: JsonNode =
      SecretsHelpers.combineConfig(
        testCase.partialConfig,
        secretPersistence,
      )
    Assertions.assertEquals(testCase.fullConfig, actualCombinedConfig)

    // check that we didn't mutate the input configs
    Assertions.assertEquals(inputPartialConfigCopy, inputPartialConfig)
  }

  @Test
  fun testCombineNullPartialConfig() {
    val secretPersistence = MemorySecretPersistence()
    Assertions.assertDoesNotThrow {
      val actualCombinedConfig: JsonNode = SecretsHelpers.combineConfig(null, secretPersistence)
      Assertions.assertNotNull(actualCombinedConfig)
      Assertions.assertEquals(JsonNodeFactory.instance.objectNode(), actualCombinedConfig)
    }
  }

  @Test
  fun testMissingSecretShouldThrowException() {
    val testCase = SimpleTestCase()
    val secretPersistence: ReadOnlySecretPersistence = mockk()
    every { secretPersistence.read(any()) } returns ""

    Assertions.assertThrows(
      RuntimeException::class.java,
    ) {
      SecretsHelpers.combineConfig(
        testCase.partialConfig,
        secretPersistence,
      )
    }
  }

  @Test
  fun testUpdatingSecretsOneAtATime() {
    val uuidIterator = SecretsTestCase.UUIDS.iterator()
    val secretPersistence = MemorySecretPersistence()
    val testCase = NestedObjectTestCase()
    val splitConfig: SplitSecretConfig =
      SecretsHelpers.splitConfig(
        { uuidIterator.next() },
        SecretsTestCase.WORKSPACE_ID,
        testCase.fullConfig,
        testCase.spec.connectionSpecification,
        secretPersistence,
      )
    Assertions.assertEquals(testCase.partialConfig, splitConfig.partialConfig)
    Assertions.assertEquals(testCase.firstSecretMap, splitConfig.getCoordinateToPayload())
    for ((key, value) in splitConfig.getCoordinateToPayload().entries) {
      secretPersistence.write(key, value)
    }
    val updatedSplit1: SplitSecretConfig =
      SecretsHelpers.splitAndUpdateConfig(
        { uuidIterator.next() },
        SecretsTestCase.WORKSPACE_ID,
        testCase.partialConfig,
        testCase.fullConfigUpdate1,
        testCase.spec.connectionSpecification,
        { coordinate: SecretCoordinate ->
          secretPersistence.read(
            coordinate,
          )
        },
      )
    Assertions.assertEquals(testCase.updatedPartialConfigAfterUpdate1, updatedSplit1.partialConfig)
    Assertions.assertEquals(testCase.secretMapAfterUpdate1, updatedSplit1.getCoordinateToPayload())
    for ((key, value) in updatedSplit1.getCoordinateToPayload().entries) {
      secretPersistence.write(key, value)
    }
    val updatedSplit2: SplitSecretConfig =
      SecretsHelpers.splitAndUpdateConfig(
        { uuidIterator.next() },
        SecretsTestCase.WORKSPACE_ID,
        updatedSplit1.partialConfig,
        testCase.fullConfigUpdate2,
        testCase.spec.connectionSpecification,
        { coordinate: SecretCoordinate ->
          secretPersistence.read(
            coordinate,
          )
        },
      )
    Assertions.assertEquals(testCase.updatedPartialConfigAfterUpdate2, updatedSplit2.partialConfig)
    Assertions.assertEquals(testCase.secretMapAfterUpdate2, updatedSplit2.getCoordinateToPayload())
  }

  @Test
  fun testGetSecretCoordinateEmptyOldSecret() {
    val secretPersistence: ReadOnlySecretPersistence = mockk()
    every { secretPersistence.read(any()) } returns ""

    val secretCoordinate =
      SecretsHelpers.getSecretCoordinate(
        "secretBasePrefix",
        "newSecret",
        secretPersistence,
        UUID.randomUUID(),
        { UUID.randomUUID() },
        "oldSecretFullCoordinate_v2",
      )
    Assertions.assertEquals("oldSecretFullCoordinate", secretCoordinate.coordinateBase)
    Assertions.assertEquals(1L, secretCoordinate.version)
  }

  @Test
  fun testGetSecretCoordinateNonEmptyOldSecret() {
    val secretPersistence: ReadOnlySecretPersistence = mockk()
    every { secretPersistence.read(any()) } returns "nonempty"

    val secretCoordinate =
      SecretsHelpers.getSecretCoordinate(
        "secretBasePrefix",
        "newSecret",
        secretPersistence,
        UUID.randomUUID(),
        { UUID.randomUUID() },
        "oldSecretFullCoordinate_v2",
      )
    Assertions.assertEquals("oldSecretFullCoordinate", secretCoordinate.coordinateBase)
    Assertions.assertEquals(3L, secretCoordinate.version)
  }

  @ParameterizedTest
  @MethodSource(PROVIDE_TEST_CASES)
  @Throws(
    IOException::class,
  )
  fun testSecretPath(testCase: SecretsTestCase) {
    val spec: JsonNode = testCase.spec.connectionSpecification
    val secretsPaths: List<String> = SecretsHelpers.getSortedSecretPaths(spec)
    org.assertj.core.api.Assertions.assertThat(secretsPaths)
      .containsExactlyElementsOf(testCase.expectedSecretsPaths)
  }

  companion object {
    /**
     * This is a bit of a non-standard way of specifying test case parameterization for Junit, but it's
     * intended to let you treat most of the JSON involved in the tests as static files.
     */
    @JvmStatic
    fun provideTestCases(): Stream<Arguments> {
      return Stream.of(
        OptionalPasswordTestCase(),
        SimpleTestCase(),
        NestedObjectTestCase(),
        OneOfTestCase(),
        OneOfSecretTestCase(),
        ArrayTestCase(),
        ArrayOneOfTestCase(),
        NestedOneOfTestCase(),
        PostgresSshKeyTestCase(),
      ).map { arguments: SecretsTestCase? ->
        Arguments.of(
          arguments,
        )
      }
    }
  }
}
