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
import io.airbyte.commons.server.validation.CatalogValidatorTest.Fixtures.buildStreamFieldSelection
import io.airbyte.commons.server.validation.CatalogValidatorTest.Fixtures.buildStreamNoFieldSelection
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

  @Test
  fun `ignores unselected streams`() {
    // build a catalog with 50 fields, 1/5 of which are selected and 1/2 of which use explicit field selection
    // selected fields should be 10 total
    val rows = 10
    val cols = 5
    val streams = mutableListOf<AirbyteStreamAndConfiguration>()
    repeat(rows) {
      val useFieldSelection = it % 2 == 0
      val selectStream = it % 5 == 0
      val stream =
        if (useFieldSelection) {
          buildStreamFieldSelection(cols, selectStream)
        } else {
          buildStreamNoFieldSelection(cols, selectStream)
        }

      streams.add(stream)
    }
    val catalog = AirbyteCatalog().streams(streams)

    val validator1 = CatalogValidator(10, metricClient, featureFlagClient)
    val validator2 = CatalogValidator(11, metricClient, featureFlagClient)
    val validator3 = CatalogValidator(100, metricClient, featureFlagClient)
    val validator4 = CatalogValidator(20000, metricClient, featureFlagClient)
    val validator5 = CatalogValidator(0, metricClient, featureFlagClient)
    val validator6 = CatalogValidator(1, metricClient, featureFlagClient)
    val validator7 = CatalogValidator(9, metricClient, featureFlagClient)
    val validator8 = CatalogValidator(4, metricClient, featureFlagClient)

    assertNull(validator1.fieldCount(catalog, CTX))
    assertNull(validator2.fieldCount(catalog, CTX))
    assertNull(validator3.fieldCount(catalog, CTX))
    assertNull(validator4.fieldCount(catalog, CTX))
    assertNotNull(validator5.fieldCount(catalog, CTX))
    assertNotNull(validator6.fieldCount(catalog, CTX))
    assertNotNull(validator7.fieldCount(catalog, CTX))
    assertNotNull(validator8.fieldCount(catalog, CTX))
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

    fun buildStreamFieldSelection(
      cols: Int,
      selected: Boolean = true,
    ): AirbyteStreamAndConfiguration {
      val stream = AirbyteStream()
      val config =
        AirbyteStreamConfiguration()
          .fieldSelectionEnabled(true)
          .selected(selected)

      repeat(cols) {
        config.addSelectedFieldsItem(SelectedFieldInfo())
      }

      return AirbyteStreamAndConfiguration()
        .stream(stream)
        .config(config)
    }

    fun buildStreamNoFieldSelection(
      cols: Int,
      selected: Boolean = true,
    ): AirbyteStreamAndConfiguration {
      val stream = AirbyteStream()
      val config =
        AirbyteStreamConfiguration()
          .fieldSelectionEnabled(false)
          .selected(selected)

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
        .config(config)
    }
  }
}
