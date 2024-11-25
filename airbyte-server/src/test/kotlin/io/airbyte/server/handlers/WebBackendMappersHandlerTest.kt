package io.airbyte.server.handlers

import io.airbyte.api.model.generated.AirbyteCatalog
import io.airbyte.api.model.generated.ConfiguredStreamMapper
import io.airbyte.api.model.generated.ConnectionRead
import io.airbyte.api.model.generated.FieldSpec
import io.airbyte.api.model.generated.MapperValidationErrorType
import io.airbyte.api.model.generated.StreamDescriptor
import io.airbyte.api.model.generated.WebBackendValidateMappersRequestBody
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.handlers.ConnectionsHandler
import io.airbyte.commons.server.handlers.helpers.CatalogConverter
import io.airbyte.config.AirbyteStream
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationSyncMode
import io.airbyte.config.Field
import io.airbyte.config.FieldType
import io.airbyte.config.MapperConfig
import io.airbyte.config.SyncMode
import io.airbyte.mappers.transformations.DestinationCatalogGenerator
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class WebBackendMappersHandlerTest {
  companion object {
    private const val STREAM_NAME = "stream"
    private const val STREAM_NAMESPACE = "namespace"
    private const val USERNAME_FIELD = "username"
    private const val PASSWORD_FIELD = "password"
    private const val PASSWORD_FIELD_HASHED = "password_hashed"
  }

  private val connectionsHandler = mockk<ConnectionsHandler>()
  private val catalogConverter = mockk<CatalogConverter>()
  private val destinationCatalogGenerator = mockk<DestinationCatalogGenerator>()

  private val webBackendMappersHandler = WebBackendMappersHandler(connectionsHandler, catalogConverter, destinationCatalogGenerator)

  @Test
  fun testValidateMappers() {
    val connectionId = UUID.randomUUID()

    val apiCatalog = mockk<AirbyteCatalog>()
    every { connectionsHandler.getConnection(any()) } returns ConnectionRead().syncCatalog(apiCatalog)

    val ogFields = buildFields(USERNAME_FIELD, PASSWORD_FIELD)
    val configuredStream = buildStream(STREAM_NAME, ogFields)
    val configuredCatalog =
      ConfiguredAirbyteCatalog(
        listOf(
          buildStream("otherStream"),
          configuredStream,
          buildStream("yetAnotherStream"),
        ),
      )
    every { catalogConverter.toConfiguredInternal(apiCatalog) } returns configuredCatalog

    val apiMappers = listOf(mockk<ConfiguredStreamMapper>(), mockk<ConfiguredStreamMapper>())
    val allMappers = listOf(mockk<MapperConfig>(), mockk<MapperConfig>())
    every { catalogConverter.toConfiguredMappers(apiMappers) } returns allMappers

    // First mapper hashes the password field
    val streamWithFirstMapper = configuredStream.copy(mappers = listOf(allMappers[0]))
    val outputFields = buildFields(USERNAME_FIELD, PASSWORD_FIELD_HASHED)
    every {
      destinationCatalogGenerator.generateDestinationCatalog(buildCatalog(streamWithFirstMapper))
    } returns
      DestinationCatalogGenerator.CatalogGenerationResult(
        buildCatalog(streamWithFirstMapper, outputFields),
        mapOf(),
      )

    // second mapper has a validation error
    val streamWithBothMappers = configuredStream.copy(mappers = allMappers)
    every {
      destinationCatalogGenerator.generateDestinationCatalog(buildCatalog(streamWithBothMappers))
    } returns
      DestinationCatalogGenerator.CatalogGenerationResult(
        buildCatalog(streamWithBothMappers, outputFields),
        mapOf(
          io.airbyte.config.StreamDescriptor().withName(STREAM_NAME).withNamespace(STREAM_NAMESPACE) to
            mapOf(
              allMappers[1] to
                DestinationCatalogGenerator.MapperError(
                  DestinationCatalogGenerator.MapperErrorType.FIELD_NOT_FOUND,
                  "Field not found",
                ),
            ),
        ),
      )

    val req =
      WebBackendValidateMappersRequestBody()
        .connectionId(connectionId)
        .streamDescriptor(StreamDescriptor().name(STREAM_NAME).namespace(STREAM_NAMESPACE))
        .mappers(apiMappers)

    val res = webBackendMappersHandler.validateMappers(req)

    assertEquals(
      listOf(
        FieldSpec().name(USERNAME_FIELD).type(FieldSpec.TypeEnum.STRING),
        FieldSpec().name(PASSWORD_FIELD).type(FieldSpec.TypeEnum.STRING),
      ),
      res.initialFields,
    )

    assertEquals(2, res.mappers.size)
    assertEquals(
      listOf(
        FieldSpec().name(USERNAME_FIELD).type(FieldSpec.TypeEnum.STRING),
        FieldSpec().name(PASSWORD_FIELD_HASHED).type(FieldSpec.TypeEnum.STRING),
      ),
      res.mappers[0].outputFields,
    )
    assertEquals(
      listOf(
        FieldSpec().name(USERNAME_FIELD).type(FieldSpec.TypeEnum.STRING),
        FieldSpec().name(PASSWORD_FIELD_HASHED).type(FieldSpec.TypeEnum.STRING),
      ),
      res.mappers[1].outputFields,
    )

    assertEquals(MapperValidationErrorType.FIELD_NOT_FOUND, res.mappers[1].validationError.type)
    assertEquals("Field not found", res.mappers[1].validationError.message)

    verify {
      catalogConverter.toConfiguredInternal(apiCatalog)
      catalogConverter.toConfiguredMappers(apiMappers)
    }

    verify(exactly = 2) {
      destinationCatalogGenerator.generateDestinationCatalog(any())
    }
  }

  private fun buildFields(vararg fields: String): List<Field> {
    return fields.map { Field(it, FieldType.STRING) }
  }

  private fun buildStream(
    name: String,
    fields: List<Field>? = null,
  ): ConfiguredAirbyteStream {
    return ConfiguredAirbyteStream.Builder()
      .stream(
        AirbyteStream(name, Jsons.emptyObject(), listOf())
          .withNamespace(STREAM_NAMESPACE),
      )
      .syncMode(SyncMode.FULL_REFRESH)
      .destinationSyncMode(DestinationSyncMode.OVERWRITE)
      .fields(fields)
      .build()
  }

  private fun buildCatalog(
    stream: ConfiguredAirbyteStream,
    fields: List<Field>? = null,
  ): ConfiguredAirbyteCatalog {
    return ConfiguredAirbyteCatalog(listOf(stream.copy(fields = fields ?: stream.fields)))
  }
}
