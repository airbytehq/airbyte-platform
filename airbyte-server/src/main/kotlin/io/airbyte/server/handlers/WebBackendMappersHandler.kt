package io.airbyte.server.handlers

import io.airbyte.api.model.generated.ConnectionIdRequestBody
import io.airbyte.api.model.generated.FieldSpec
import io.airbyte.api.model.generated.MapperValidationError
import io.airbyte.api.model.generated.MapperValidationErrorType
import io.airbyte.api.model.generated.MapperValidationResult
import io.airbyte.api.model.generated.WebBackendValidateMappersRequestBody
import io.airbyte.api.model.generated.WebBackendValidateMappersResponse
import io.airbyte.commons.server.handlers.ConnectionsHandler
import io.airbyte.commons.server.handlers.helpers.CatalogConverter
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.FieldType
import io.airbyte.config.MapperConfig
import io.airbyte.mappers.transformations.DestinationCatalogGenerator
import jakarta.inject.Singleton

/**
 * The web backend is an abstraction that allows the frontend to structure data in such a way that
 * it is easier for a react frontend to consume. It should NOT have direct access to the database.
 * It should operate exclusively by calling other endpoints that are exposed in the API.
 */
@Singleton
class WebBackendMappersHandler(
  private val connectionsHandler: ConnectionsHandler,
  private val catalogConverter: CatalogConverter,
  private val destinationCatalogGenerator: DestinationCatalogGenerator,
) {
  /**
   * Progressively validate mappers and get resulting fields.
   * Mappers are applied one by one to the stream to get the list of fields available for the next mapper.
   */
  fun validateMappers(validateMappersRequest: WebBackendValidateMappersRequestBody): WebBackendValidateMappersResponse {
    val connectionIdRequestBody = ConnectionIdRequestBody().connectionId(validateMappersRequest.connectionId)
    val connection = connectionsHandler.getConnection(connectionIdRequestBody.connectionId)
    val configuredCatalog = catalogConverter.toConfiguredInternal(connection.syncCatalog)

    val stream =
      configuredCatalog.streams.first {
        it.stream.name == validateMappersRequest.streamDescriptor.name &&
          it.stream.namespace == validateMappersRequest.streamDescriptor.namespace
      }

    val primaryKeyFields = stream.primaryKey?.map { it.first() }
    val cursorFields = stream.cursorField?.firstOrNull()

    val initialFields =
      stream.fields!!.map {
        FieldSpec()
          .name(it.name)
          .type(convertFieldType(it.type))
          .isSelectedPrimaryKey(primaryKeyFields?.contains(it.name) ?: false)
          .isSelectedCursor(cursorFields == it.name)
      }.toList()

    val partialMappers = mutableListOf<MapperConfig>()
    stream.mappers = partialMappers

    val mapperValidationResults = mutableListOf<MapperValidationResult>()
    val newMappers = catalogConverter.toConfiguredMappers(validateMappersRequest.mappers)

    // Trim down the catalog so we only process mappers for the stream we're working with
    val slimCatalog = ConfiguredAirbyteCatalog(listOf(stream))

    var lastFieldSet = initialFields
    for (mapper in newMappers) {
      partialMappers.add(mapper)

      val generationResult = destinationCatalogGenerator.generateDestinationCatalog(slimCatalog)
      val newStream = generationResult.catalog.streams.first()

      val validateRes = MapperValidationResult()
      validateRes.id = mapper.id()
      validateRes.inputFields = lastFieldSet
      validateRes.outputFields =
        newStream.fields!!.map {
          FieldSpec()
            .name(it.name)
            .type(convertFieldType(it.type))
            .isSelectedPrimaryKey(primaryKeyFields?.contains(it.name) ?: false)
            .isSelectedCursor(cursorFields == it.name)
        }.toList()

      lastFieldSet = validateRes.outputFields

      val streamErrors = generationResult.errors.entries.firstOrNull()?.value
      val mapperError = streamErrors?.get(mapper)
      if (mapperError != null) {
        validateRes.validationError =
          MapperValidationError()
            .type(convertMapperErrorType(mapperError.type))
            .message(mapperError.message)
      }

      mapperValidationResults.add(validateRes)
    }

    return WebBackendValidateMappersResponse()
      .initialFields(initialFields)
      .outputFields(lastFieldSet)
      .mappers(mapperValidationResults)
  }

  private fun convertMapperErrorType(mapperErrorType: DestinationCatalogGenerator.MapperErrorType): MapperValidationErrorType {
    return when (mapperErrorType) {
      DestinationCatalogGenerator.MapperErrorType.MISSING_MAPPER -> MapperValidationErrorType.MISSING_MAPPER
      DestinationCatalogGenerator.MapperErrorType.INVALID_MAPPER_CONFIG -> MapperValidationErrorType.INVALID_MAPPER_CONFIG
      DestinationCatalogGenerator.MapperErrorType.FIELD_NOT_FOUND -> MapperValidationErrorType.FIELD_NOT_FOUND
      DestinationCatalogGenerator.MapperErrorType.FIELD_ALREADY_EXISTS -> MapperValidationErrorType.FIELD_ALREADY_EXISTS
    }
  }

  private fun convertFieldType(fieldType: FieldType): FieldSpec.TypeEnum {
    return when (fieldType) {
      FieldType.STRING -> FieldSpec.TypeEnum.STRING
      FieldType.BOOLEAN -> FieldSpec.TypeEnum.BOOLEAN
      FieldType.DATE -> FieldSpec.TypeEnum.DATE
      FieldType.TIMESTAMP_WITHOUT_TIMEZONE -> FieldSpec.TypeEnum.TIMESTAMP_WITHOUT_TIMEZONE
      FieldType.TIMESTAMP_WITH_TIMEZONE -> FieldSpec.TypeEnum.TIMESTAMP_WITH_TIMEZONE
      FieldType.TIME_WITHOUT_TIMEZONE -> FieldSpec.TypeEnum.TIME_WITHOUT_TIMEZONE
      FieldType.TIME_WITH_TIMEZONE -> FieldSpec.TypeEnum.TIME_WITH_TIMEZONE
      FieldType.INTEGER -> FieldSpec.TypeEnum.INTEGER
      FieldType.NUMBER -> FieldSpec.TypeEnum.NUMBER
      FieldType.ARRAY -> FieldSpec.TypeEnum.ARRAY
      FieldType.OBJECT -> FieldSpec.TypeEnum.OBJECT
      FieldType.MULTI -> FieldSpec.TypeEnum.MULTI
      FieldType.UNKNOWN -> FieldSpec.TypeEnum.UNKNOWN
    }
  }
}
