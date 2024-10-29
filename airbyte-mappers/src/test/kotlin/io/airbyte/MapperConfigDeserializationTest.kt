package io.airbyte

import com.fasterxml.jackson.core.type.TypeReference
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfiguredMapper
import io.airbyte.config.MapperConfig
import io.airbyte.mappers.transformations.Mapper
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.File

@MicronautTest
class MapperConfigDeserializationTest {
  @Inject
  private lateinit var mappers: List<Mapper<out MapperConfig>>

  @Test
  fun `mapper config examples should be correctly deserialized `() {
    val fieldRenamingMapperExamples =
      javaClass.classLoader.getResource("FieldRenamingMapperConfigExamples.json")
        ?: throw IllegalArgumentException("File not found: FieldRenamingMapperConfigExamples.json")

    val hashingMapperExamples =
      javaClass.classLoader.getResource("HashingMapperConfigExamples.json")
        ?: throw IllegalArgumentException("File not found: HashingMapperConfigExamples.json")

    val rowFilteringMapperExamples =
      javaClass.classLoader.getResource("RowFilteringMapperConfigExamples.json")
        ?: throw IllegalArgumentException("File not found: RowFilteringMapperConfigExamples.json")

    val fieldRenamingMapperConfigs = Jsons.deserialize(File(fieldRenamingMapperExamples.toURI()), object : TypeReference<List<ConfiguredMapper>>() {})
    val hashingMapperConfigs = Jsons.deserialize(File(hashingMapperExamples.toURI()), object : TypeReference<List<ConfiguredMapper>>() {})
    val rowFilteringMapperConfigs = Jsons.deserialize(File(rowFilteringMapperExamples.toURI()), object : TypeReference<List<ConfiguredMapper>>() {})

    val mixedMappers = mutableListOf<ConfiguredMapper>()
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
      Jsons.deserialize(
        Jsons.serialize(mapOf("mappers" to Jsons.jsonNode(mixedMappers))),
        TestListMapperConfig::class.java,
      )

    assertTrue(mixedMappers.size > 0)
    assertEquals(mixedMappers.size, listMapperConfig.mappers.size)

    for (i in mixedMappers.indices) {
      assertEquals(mixedMappers[i].name, listMapperConfig.mappers[i].name(), "The mapper name at index $i are not equal.")
      assertEquals(mixedMappers[i].config, Jsons.jsonNode(listMapperConfig.mappers[i].config()), "The mapper config at index $i are not equal.")
    }
  }

  data class TestListMapperConfig(
    var mappers: List<MapperConfig> = listOf(),
  )
}
