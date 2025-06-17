/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte

import TEST_OBJECT_MAPPER
import com.fasterxml.jackson.core.type.TypeReference
import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.MapperConfig
import io.airbyte.mappers.transformations.Mapper
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.File
import java.net.URL

@MicronautTest
class MapperConfigDeserializationTest {
  @Inject
  private lateinit var mappers: List<Mapper<out MapperConfig>>

  @Test
  fun `mapper config examples should be correctly deserialized `() {
    val encryptionMapperExamples = loadResource("EncryptionMapperConfigExamples.json")

    val fieldRenamingMapperExamples = loadResource("FieldRenamingMapperConfigExamples.json")

    val hashingMapperExamples = loadResource("HashingMapperConfigExamples.json")

    val rowFilteringMapperExamples = loadResource("RowFilteringMapperConfigExamples.json")

    val encryptionMapperConfigs = encryptionMapperExamples.toConfig()
    val fieldRenamingMapperConfigs = fieldRenamingMapperExamples.toConfig()
    val hashingMapperConfigs = hashingMapperExamples.toConfig()
    val rowFilteringMapperConfigs = rowFilteringMapperExamples.toConfig()

    val mixedMappers = mutableListOf<ConfiguredMapper>()
    mixedMappers.addAll(encryptionMapperConfigs)
    mixedMappers.addAll(fieldRenamingMapperConfigs)
    mixedMappers.addAll(hashingMapperConfigs)
    mixedMappers.addAll(rowFilteringMapperConfigs)

    val mapperNames: Set<String> = mixedMappers.map { it.name }.toSet()
    mappers.map { (it) }.associateBy { it.name }.forEach {
      if (it.key != "test-mapper") {
        assertTrue(
          mapperNames.contains(it.key),
          "${it.key} mapper not present in the Examples",
        )
      }
    }

    val listMapperConfig =
      TEST_OBJECT_MAPPER.readValue(
        TEST_OBJECT_MAPPER.writeValueAsString(mapOf("mappers" to mixedMappers)),
        TestListMapperConfig::class.java,
      )

    assertTrue(mixedMappers.size > 0)
    assertEquals(mixedMappers.size, listMapperConfig.mappers.size)

    for (i in mixedMappers.indices) {
      assertEquals(mixedMappers[i].name, listMapperConfig.mappers[i].name(), "The mapper name at index $i are not equal.")
      assertEquals(
        mixedMappers[i].config,
        TEST_OBJECT_MAPPER.valueToTree(listMapperConfig.mappers[i].config()),
        "The mapper config at index $i are not equal.",
      )
    }
  }

  private fun loadResource(resourceName: String): URL =
    javaClass.classLoader.getResource(resourceName)
      ?: throw IllegalArgumentException("File not found: $resourceName")

  private fun URL.toConfig() = TEST_OBJECT_MAPPER.readValue(File(this.toURI()), object : TypeReference<List<ConfiguredMapper>>() {})

  data class TestListMapperConfig(
    var mappers: List<MapperConfig> = listOf(),
  )
}
