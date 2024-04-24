package io.airbyte.commons.server.validation

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.model.generated.AirbyteCatalog
import io.airbyte.api.model.generated.AirbyteStream
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.model.generated.AirbyteStreamConfiguration
import io.airbyte.api.model.generated.SelectedFieldInfo
import io.airbyte.commons.server.validation.CatalogValidator.Constants.PROPERTIES_KEY
import io.airbyte.commons.server.validation.CatalogValidatorTest.Fixtures.CTX
import io.airbyte.commons.server.validation.CatalogValidatorTest.Fixtures.MAX_FIELD_LIMIT
import io.airbyte.commons.server.validation.CatalogValidatorTest.Fixtures.buildCatalog
import io.airbyte.featureflag.ANONYMOUS
import io.airbyte.featureflag.ConnectionFieldLimitOverride
import io.airbyte.featureflag.TestClient
import io.airbyte.featureflag.Workspace
import io.airbyte.metrics.lib.MetricClient
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.stream.Stream

@ExtendWith(MockKExtension::class)
class CatalogValidatorTest {
  @MockK
  private lateinit var metricClient: MetricClient

  @MockK
  private lateinit var featureFlagClient: TestClient

  private lateinit var validator: CatalogValidator

  @BeforeEach
  fun setup() {
    every { featureFlagClient.intVariation(ConnectionFieldLimitOverride, any()) } returns -1
    every { metricClient.distribution(any(), any(), *anyVararg()) } returns Unit

    validator = CatalogValidator(MAX_FIELD_LIMIT, metricClient, featureFlagClient)
  }

  @ParameterizedTest
  @MethodSource("validSizeCatalogMatrix")
  fun `returns null if catalog field count under limit`(catalog: AirbyteCatalog) {
    val result = validator.fieldCount(catalog, CTX)
    assertNull(result)
  }

  @ParameterizedTest
  @MethodSource("tooLargeCatalogMatrix")
  fun `returns ValidationError if catalog field count over limit`(catalog: AirbyteCatalog) {
    val result = validator.fieldCount(catalog, CTX)
    assertNotNull(result)
  }

  @Test
  fun `allows runtime override for max field limit`() {
    val override = MAX_FIELD_LIMIT * 2
    every { featureFlagClient.intVariation(ConnectionFieldLimitOverride, any()) } returns override

    assertNull(validator.fieldCount(buildCatalog(1, override), CTX))
    assertNotNull(validator.fieldCount(buildCatalog(1, override + 1), CTX))
  }

  companion object {
    @JvmStatic
    fun validSizeCatalogMatrix(): Stream<Arguments> {
      return Stream.of(
        Arguments.of(buildCatalog(1, MAX_FIELD_LIMIT)),
        Arguments.of(buildCatalog(MAX_FIELD_LIMIT, 1)),
        Arguments.of(buildCatalog(10, 1000)),
        Arguments.of(buildCatalog(2, 123)),
        Arguments.of(buildCatalog(2, 1)),
        Arguments.of(buildCatalog(1, 100)),
        Arguments.of(buildCatalog(6, 900)),
        Arguments.of(buildCatalog(300, 33)),
        Arguments.of(buildCatalog(1, MAX_FIELD_LIMIT, true)),
        Arguments.of(buildCatalog(MAX_FIELD_LIMIT, 1, true)),
      )
    }

    @JvmStatic
    fun tooLargeCatalogMatrix(): Stream<Arguments> {
      return Stream.of(
        Arguments.of(buildCatalog(1, MAX_FIELD_LIMIT + 1)),
        Arguments.of(buildCatalog(MAX_FIELD_LIMIT + 1, 1)),
        Arguments.of(buildCatalog(10, 1001)),
        Arguments.of(buildCatalog(2, 6000)),
        Arguments.of(buildCatalog(6, 2000)),
        Arguments.of(buildCatalog(300, 34)),
        Arguments.of(buildCatalog(1, MAX_FIELD_LIMIT + 1, true)),
        Arguments.of(buildCatalog(MAX_FIELD_LIMIT + 1, 1, true)),
      )
    }
  }

  object Fixtures {
    private val MAPPER = ObjectMapper()
    const val MAX_FIELD_LIMIT = 10000
    val CTX = Workspace(ANONYMOUS)

    fun buildCatalog(
      rows: Int,
      cols: Int,
      fieldSelection: Boolean = false,
    ): AirbyteCatalog {
      val streams = mutableListOf<AirbyteStreamAndConfiguration>()
      repeat(rows) {
        val stream =
          if (fieldSelection) {
            buildStreamFieldSelection(cols)
          } else {
            buildStreamNoFieldSelection(cols)
          }

        streams.add(stream)
      }
      return AirbyteCatalog().streams(streams)
    }

    private fun buildStreamFieldSelection(cols: Int): AirbyteStreamAndConfiguration {
      val config = AirbyteStreamConfiguration().fieldSelectionEnabled(true)

      repeat(cols) {
        config.addSelectedFieldsItem(SelectedFieldInfo())
      }

      return AirbyteStreamAndConfiguration()
        .stream(AirbyteStream())
        .config(config)
    }

    private fun buildStreamNoFieldSelection(cols: Int): AirbyteStreamAndConfiguration {
      val stream = AirbyteStream()

      // Unfortunately, we define our data model as a JsonNode in json schema format
      val jsonSchemaNode = MAPPER.createObjectNode()
      // The properties node contains the columns keyed by name
      val propertiesNode = MAPPER.createObjectNode()
      jsonSchemaNode.replace(PROPERTIES_KEY, propertiesNode)
      stream.jsonSchema = jsonSchemaNode

      repeat(cols) {
        val colName = "col-$it"
        // technically we should add a JsonNode here, but for simplicity purposes we just use the int
        propertiesNode.put(colName, it)
      }

      return AirbyteStreamAndConfiguration()
        .stream(stream)
        .config(AirbyteStreamConfiguration().fieldSelectionEnabled(false))
    }
  }
}
