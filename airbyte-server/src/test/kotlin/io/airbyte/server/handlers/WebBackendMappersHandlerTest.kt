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
    private const val CURSOR_FIELD = "updated_at"
    private const val PK_FIELD = "id"
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

    val ogFields = buildFields(USERNAME_FIELD, PASSWORD_FIELD, CURSOR_FIELD, PK_FIELD)
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
    val allMappers = listOf(mockk<MapperConfig>(relaxed = true), mockk<MapperConfig>(relaxed = true))
    every { catalogConverter.toConfiguredMappers(apiMappers) } returns allMappers

    // First mapper hashes the password field
    val streamWithFirstMapper = configuredStream.copy(mappers = listOf(allMappers[0]))
    val outputFields = buildFields(USERNAME_FIELD, PASSWORD_FIELD_HASHED, PK_FIELD, CURSOR_FIELD)
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

    val expectedInitialFields =
      listOf(
        FieldSpec().name(USERNAME_FIELD).type(FieldSpec.TypeEnum.STRING).isSelectedCursor(false).isSelectedPrimaryKey(false),
        FieldSpec().name(PASSWORD_FIELD).type(FieldSpec.TypeEnum.STRING).isSelectedCursor(false).isSelectedPrimaryKey(false),
        FieldSpec().name(CURSOR_FIELD).type(FieldSpec.TypeEnum.STRING).isSelectedCursor(true).isSelectedPrimaryKey(false),
        FieldSpec().name(PK_FIELD).type(FieldSpec.TypeEnum.STRING).isSelectedCursor(false).isSelectedPrimaryKey(true),
      )
    assertEquals(expectedInitialFields, res.initialFields)

    assertEquals(2, res.mappers.size)
    val expectedFirstMapperOutputFields =
      listOf(
        FieldSpec().name(USERNAME_FIELD).type(FieldSpec.TypeEnum.STRING).isSelectedCursor(false).isSelectedPrimaryKey(false),
        FieldSpec().name(PASSWORD_FIELD_HASHED).type(FieldSpec.TypeEnum.STRING).isSelectedCursor(false).isSelectedPrimaryKey(false),
        FieldSpec().name(PK_FIELD).type(FieldSpec.TypeEnum.STRING).isSelectedCursor(false).isSelectedPrimaryKey(true),
        FieldSpec().name(CURSOR_FIELD).type(FieldSpec.TypeEnum.STRING).isSelectedCursor(true).isSelectedPrimaryKey(false),
      )
    assertEquals(expectedInitialFields, res.mappers[0].inputFields)
    assertEquals(expectedFirstMapperOutputFields, res.mappers[0].outputFields)

    val expectedSecondMapperOutputFields =
      listOf(
        FieldSpec().name(USERNAME_FIELD).type(FieldSpec.TypeEnum.STRING).isSelectedCursor(false).isSelectedPrimaryKey(false),
        FieldSpec().name(PASSWORD_FIELD_HASHED).type(FieldSpec.TypeEnum.STRING).isSelectedCursor(false).isSelectedPrimaryKey(false),
        FieldSpec().name(PK_FIELD).type(FieldSpec.TypeEnum.STRING).isSelectedCursor(false).isSelectedPrimaryKey(true),
        FieldSpec().name(CURSOR_FIELD).type(FieldSpec.TypeEnum.STRING).isSelectedCursor(true).isSelectedPrimaryKey(false),
      )
    assertEquals(expectedFirstMapperOutputFields, res.mappers[1].inputFields)
    assertEquals(expectedSecondMapperOutputFields, res.mappers[1].outputFields)

    assertEquals(MapperValidationErrorType.FIELD_NOT_FOUND, res.mappers[1].validationError.type)
    assertEquals("Field not found", res.mappers[1].validationError.message)

    assertEquals(expectedSecondMapperOutputFields, res.outputFields)

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
      .primaryKey(listOf(listOf(PK_FIELD)))
      .cursorField(listOf(CURSOR_FIELD))
      .build()
  }

  private fun buildCatalog(
    stream: ConfiguredAirbyteStream,
    fields: List<Field>? = null,
  ): ConfiguredAirbyteCatalog {
    return ConfiguredAirbyteCatalog(listOf(stream.copy(fields = fields ?: stream.fields)))
  }
}
