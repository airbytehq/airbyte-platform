/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.mappers.application

import TEST_OBJECT_MAPPER
import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.mapper.configs.TEST_MAPPER_NAME
import io.airbyte.mappers.mocks.TestMapper
import io.airbyte.mappers.transformations.ConfiguredMapperValidator
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import jakarta.inject.Named
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

@MicronautTest
class ConfiguredMapperValidatorTest {
  @Inject
  lateinit var configuredMapperValidator: ConfiguredMapperValidator

  @Inject
  @Named("TestMapper")
  lateinit var testMapper: TestMapper

  @Test
  fun `test validateMapperConfig with valid config`() {
    val validConfiguredMapper =
      ConfiguredMapper(TEST_MAPPER_NAME, TEST_OBJECT_MAPPER.valueToTree(mapOf("field1" to "value1", "field2" to "value2", "enumField" to "ONE")))

    assertDoesNotThrow {
      configuredMapperValidator.validateMapperConfig(testMapper.spec().jsonSchema(), validConfiguredMapper, TEST_OBJECT_MAPPER)
    }
  }

  @Test
  fun `test validateMapperConfig with invalid config`() {
    val invalidConfiguredMapper = ConfiguredMapper(TEST_MAPPER_NAME, TEST_OBJECT_MAPPER.valueToTree(mapOf("key1" to 123)))

    val exception =
      assertThrows<IllegalArgumentException> {
        configuredMapperValidator.validateMapperConfig(testMapper.spec().jsonSchema(), invalidConfiguredMapper, TEST_OBJECT_MAPPER)
      }
    assertEquals(
      "Mapper Config not valid: \$.config: required property 'enumField' not found,\$.config: " +
        "required property 'field1' not found,\$.config: required property 'field2' not found",
      exception.message,
    )
  }
}
