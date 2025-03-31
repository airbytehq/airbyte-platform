/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.secrets

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import io.airbyte.config.secrets.SecretCoordinate.AirbyteManagedSecretCoordinate
import io.airbyte.config.secrets.SecretCoordinate.ExternalSecretCoordinate
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
import io.airbyte.protocol.models.Jsons
import io.airbyte.validation.json.JsonSchemaValidator
import io.airbyte.validation.json.JsonValidationException
import io.kotlintest.matchers.string.shouldStartWith
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Nested
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
  private val secretPersistence: SecretPersistence = MemorySecretPersistence()

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
        secretPersistence,
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
      Consumer { key: SecretCoordinate ->
        Assertions.assertTrue(
          gsmKeyCharacterPattern.matcher(key.fullCoordinate).matches(),
          "Invalid character in key: $key",
        )
        Assertions.assertTrue(
          key.fullCoordinate.length <= 255,
          "Key is too long: " + key.fullCoordinate.length,
        )
      },
    )
  }

  @ParameterizedTest
  @MethodSource(PROVIDE_TEST_CASES)
  fun testSplitWithCustomSecretPrefix(testCase: SecretsTestCase) {
    val uuidIterator = SecretsTestCase.UUIDS.iterator()
    val inputConfig: JsonNode = testCase.fullConfig
    val customSecretPrefix = "airbyte_custom_prefix_"
    val splitConfig: SplitSecretConfig =
      SecretsHelpers.splitConfig(
        { uuidIterator.next() },
        SecretsTestCase.WORKSPACE_ID,
        inputConfig,
        testCase.spec.connectionSpecification,
        secretPersistence,
        customSecretPrefix,
      )

    // check every key starts with the custom secret prefix
    splitConfig.getCoordinateToPayload().keys.forEach(
      Consumer { key: SecretCoordinate ->
        key.fullCoordinate.shouldStartWith("airbyte_$customSecretPrefix${SecretsTestCase.WORKSPACE_ID}")
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
      SecretsHelpers.combineInlinedConfig(
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
      SecretsHelpers.combineInlinedConfig(
        testCase.partialConfig,
        secretPersistence,
      )
    }
  }

  @Test
  fun testUpdatingSecretsOneAtATimeShouldAlwaysIncrementAllVersions() {
    val uuidIterator = SecretsTestCase.UUIDS.iterator()
    val secretPersistence = MemorySecretPersistence()
    val testCase = NestedObjectTestCase()

    // First write
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

    // Update 2
    val updatedSplit1: SplitSecretConfig =
      SecretsHelpers.splitAndUpdateConfig(
        { uuidIterator.next() },
        SecretsTestCase.WORKSPACE_ID,
        testCase.partialConfig,
        testCase.fullConfigUpdateTopLevel,
        testCase.spec.connectionSpecification,
        { coordinate: SecretCoordinate -> secretPersistence.read(coordinate) },
      )
    Assertions.assertEquals(testCase.updatedPartialConfigAfterUpdateTopLevel, updatedSplit1.partialConfig)
    Assertions.assertEquals(testCase.secretMapAfterUpdateTopLevel, updatedSplit1.getCoordinateToPayload())
    for ((key, value) in updatedSplit1.getCoordinateToPayload().entries) {
      secretPersistence.write(key, value)
    }

    // Update 3
    val updatedSplit2: SplitSecretConfig =
      SecretsHelpers.splitAndUpdateConfig(
        { uuidIterator.next() },
        SecretsTestCase.WORKSPACE_ID,
        updatedSplit1.partialConfig,
        testCase.fullConfigUpdateNested,
        testCase.spec.connectionSpecification,
        { coordinate: SecretCoordinate -> secretPersistence.read(coordinate) },
      )
    Assertions.assertEquals(testCase.updatedPartialConfigAfterUpdateNested, updatedSplit2.partialConfig)
    Assertions.assertEquals(testCase.secretMapAfterUpdateNested, updatedSplit2.getCoordinateToPayload())
  }

  @Test
  fun testGetAirbyteManagedSecretCoordinateEmptyOldSecret() {
    val secretPersistence: ReadOnlySecretPersistence = mockk()
    every { secretPersistence.read(any()) } returns ""

    val newSecretBaseId = UUID.randomUUID()

    val secretCoordinate =
      SecretsHelpers.getAirbyteManagedSecretCoordinate(
        "newSecretBasePrefix",
        secretPersistence,
        newSecretBaseId,
        { UUID.randomUUID() },
        "airbyte_oldSecretFullCoordinate_v2",
      )
    Assertions.assertTrue(secretCoordinate.coordinateBase.startsWith("airbyte_newSecretBasePrefix$newSecretBaseId"))
    Assertions.assertEquals(SecretCoordinate.AirbyteManagedSecretCoordinate.DEFAULT_VERSION, secretCoordinate.version)
  }

  @Test
  fun testGetAirbyteManagedSecretCoordinateNonEmptyOldSecret() {
    val secretPersistence: ReadOnlySecretPersistence = mockk()
    every { secretPersistence.read(any()) } returns "nonempty"

    val secretCoordinate =
      SecretsHelpers.getAirbyteManagedSecretCoordinate(
        "secretBasePrefix",
        secretPersistence,
        UUID.randomUUID(),
        { UUID.randomUUID() },
        "airbyte_oldSecretFullCoordinate_v2",
      )
    Assertions.assertEquals("airbyte_oldSecretFullCoordinate", secretCoordinate.coordinateBase)
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
    org.assertj.core.api.Assertions
      .assertThat(secretsPaths)
      .containsExactlyElementsOf(testCase.expectedSecretsPaths)
  }

  @Nested
  inner class SecretReferenceHelpersTest {
    @Test
    fun testGetReferenceMapFromInlinedConfig() {
      val storageId = UUID.randomUUID()
      val jsonConfig =
        Jsons.jsonNode(
          mapOf(
            "topLevelSecret" to
              mapOf(
                "_secret" to "airbyte_workspace_123_secret_456_v1",
                "_secret_storage_id" to storageId.toString(),
              ),
            "topLevelExternalSecret" to
              mapOf(
                "_secret" to "my-top-level-ext-secret",
                "_secret_storage_id" to storageId.toString(),
              ),
            "topLevelLegacySecret" to
              mapOf(
                "_secret" to "airbyte_workspace_123_secret_567_v12",
              ),
            "nestedObject" to
              mapOf(
                "nestedSecret" to
                  mapOf(
                    "_secret" to "airbyte_workspace_123_secret_789_v1",
                    "_secret_storage_id" to storageId.toString(),
                  ),
              ),
            "nestedArray" to
              listOf(
                mapOf(
                  "_secret" to "airbyte_workspace_123_secret_890_v2",
                  "_secret_storage_id" to storageId.toString(),
                ),
                mapOf(
                  "_secret" to "my-array-secret-2",
                  "_secret_storage_id" to storageId.toString(),
                ),
              ),
          ),
        )

      val expectedMap =
        mapOf(
          "$.topLevelSecret" to
            SecretReferenceConfig(
              secretCoordinate = AirbyteManagedSecretCoordinate("airbyte_workspace_123_secret_456"),
              secretStorageId = storageId,
            ),
          "$.topLevelExternalSecret" to
            SecretReferenceConfig(
              secretCoordinate = ExternalSecretCoordinate("my-top-level-ext-secret"),
              secretStorageId = storageId,
            ),
          "$.topLevelLegacySecret" to
            SecretReferenceConfig(
              secretCoordinate = AirbyteManagedSecretCoordinate("airbyte_workspace_123_secret_567", 12),
              secretStorageId = null,
            ),
          "$.nestedObject.nestedSecret" to
            SecretReferenceConfig(
              secretCoordinate = AirbyteManagedSecretCoordinate("airbyte_workspace_123_secret_789"),
              secretStorageId = storageId,
            ),
          "$.nestedArray[0]" to
            SecretReferenceConfig(
              secretCoordinate = AirbyteManagedSecretCoordinate("airbyte_workspace_123_secret_890", 2),
              secretStorageId = storageId,
            ),
          "$.nestedArray[1]" to
            SecretReferenceConfig(
              secretCoordinate = ExternalSecretCoordinate("my-array-secret-2"),
              secretStorageId = storageId,
            ),
        )

      val referenceMap = SecretsHelpers.SecretReferenceHelpers.getReferenceMapFromConfig(InlinedConfigWithSecretRefs(jsonConfig))
      Assertions.assertEquals(expectedMap, referenceMap)

      // test conversion is lossless
      val inlinedConfig = SecretsHelpers.SecretReferenceHelpers.inlineSecretReferences(jsonConfig, expectedMap)
      val reExtractedReferenceMap = SecretsHelpers.SecretReferenceHelpers.getReferenceMapFromConfig(inlinedConfig)
      Assertions.assertEquals(referenceMap, reExtractedReferenceMap)
    }

    @Test
    fun testInlineSecretReferences() {
      val storageId = UUID.randomUUID()
      val jsonConfig =
        Jsons.jsonNode(
          mapOf(
            "username" to "bob",
            "password" to mapOf("_ref_id" to "123"),
            "apiKey" to mapOf("_ref_id" to "456"),
            "legacySecret" to mapOf("_secret" to "my-coord"),
          ),
        )
      val referenceConfigs =
        mapOf(
          "$.password" to
            SecretReferenceConfig(
              secretStorageId = storageId,
              secretCoordinate = AirbyteManagedSecretCoordinate("airbyte_workspace_123_secret_123"),
            ),
          "$.apiKey" to
            SecretReferenceConfig(
              secretStorageId = storageId,
              secretCoordinate = ExternalSecretCoordinate("my-external-coordinate"),
            ),
        )

      val expectedInlined =
        InlinedConfigWithSecretRefs(
          Jsons.jsonNode(
            mapOf(
              "username" to "bob",
              "password" to
                mapOf(
                  "_secret" to "airbyte_workspace_123_secret_123_v1",
                  "_secret_storage_id" to storageId.toString(),
                ),
              "apiKey" to
                mapOf(
                  "_secret" to "my-external-coordinate",
                  "_secret_storage_id" to storageId.toString(),
                ),
              "legacySecret" to
                mapOf(
                  "_secret" to "my-coord",
                ),
            ),
          ),
        )

      val inlined = SecretsHelpers.SecretReferenceHelpers.inlineSecretReferences(jsonConfig, referenceConfigs)
      Assertions.assertEquals(expectedInlined, inlined)
    }
  }

  companion object {
    /**
     * This is a bit of a non-standard way of specifying test case parameterization for Junit, but it's
     * intended to let you treat most of the JSON involved in the tests as static files.
     */
    @JvmStatic
    fun provideTestCases(): Stream<Arguments> =
      Stream
        .of(
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
