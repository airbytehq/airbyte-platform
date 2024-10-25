package io.airbyte.mappers.transformations

import com.fasterxml.jackson.databind.JsonNode
import com.networknt.schema.JsonSchemaFactory
import com.networknt.schema.SpecVersion
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ConfiguredMapper
import jakarta.inject.Singleton

@Singleton
class ConfiguredMapperValidator {
  fun validateMapperConfig(
    jsonSchema: JsonNode,
    mapperConfig: ConfiguredMapper,
  ) {
    val schemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7)
    val schema = schemaFactory.getSchema(jsonSchema)
    val configAsJson = Jsons.jsonNode(mapperConfig)

    val validationErrors = schema.validate(configAsJson)

    if (validationErrors.isNotEmpty()) {
      val errorMessage = validationErrors.joinToString(separator = ",") { it.message }
      throw IllegalArgumentException("Mapper Config not valid: $errorMessage")
    }
  }
}
