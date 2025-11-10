/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.validation

import com.fasterxml.jackson.databind.ObjectMapper
import io.airbyte.api.model.generated.AirbyteCatalog
import io.airbyte.api.model.generated.AirbyteStream
import io.airbyte.api.model.generated.AirbyteStreamAndConfiguration
import io.airbyte.api.model.generated.AirbyteStreamConfiguration
import io.airbyte.api.model.generated.SelectedFieldInfo
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.runtime.AirbyteServerConfiguration
import io.airbyte.commons.server.runtime.AirbyteServerConfiguration.AirbyteServerConnectionLimitConfiguration.AirbyteServerConnectionLimitsConfiguration
import io.airbyte.commons.server.validation.CatalogValidator.Constants.PROPERTIES_KEY
import io.airbyte.commons.server.validation.CatalogValidatorTest.Fixtures.CTX
import io.airbyte.commons.server.validation.CatalogValidatorTest.Fixtures.MAX_FIELD_LIMIT
import io.airbyte.commons.server.validation.CatalogValidatorTest.Fixtures.buildCatalog
import io.airbyte.commons.server.validation.CatalogValidatorTest.Fixtures.buildStreamFieldSelection
import io.airbyte.commons.server.validation.CatalogValidatorTest.Fixtures.buildStreamNoFieldSelection
import io.airbyte.featureflag.ANONYMOUS
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.ConnectionFieldLimitOverride
import io.airbyte.featureflag.TestClient
import io.airbyte.featureflag.Workspace
import io.airbyte.metrics.MetricClient
import io.micrometer.core.instrument.DistributionSummary
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource

@ExtendWith(MockKExtension::class)
internal class CatalogValidatorTest {
  @MockK
  private lateinit var metricClient: MetricClient

  @MockK
  private lateinit var featureFlagClient: TestClient

  private lateinit var validator: CatalogValidator
  private lateinit var airbyteServerConfiguration: AirbyteServerConfiguration

  @BeforeEach
  internal fun setup() {
    every { featureFlagClient.intVariation(ConnectionFieldLimitOverride, any()) } returns -1
    every { metricClient.distribution(metric = any(), any(), *anyVararg()) } returns mockk<DistributionSummary>()

    airbyteServerConfiguration =
      AirbyteServerConfiguration(
        connectionLimits =
          AirbyteServerConfiguration.AirbyteServerConnectionLimitConfiguration(
            limits =
              AirbyteServerConnectionLimitsConfiguration(
                maxFieldsPerConnection = MAX_FIELD_LIMIT.toLong(),
              ),
          ),
      )

    validator = CatalogValidator(airbyteServerConfiguration, metricClient, featureFlagClient)
  }

  @ParameterizedTest
  @MethodSource("validSizeCatalogMatrix")
  internal fun `returns null if catalog field count under limit`(catalog: AirbyteCatalog) {
    val result = validator.fieldCount(catalog, CTX)
    assertNull(result)
  }

  @ParameterizedTest
  @MethodSource("tooLargeCatalogMatrix")
  internal fun `returns ValidationError if catalog field count over limit`(catalog: AirbyteCatalog) {
    val result = validator.fieldCount(catalog, CTX)
    assertNotNull(result)
  }

  @Test
  internal fun `allows runtime override for max field limit`() {
    val override = MAX_FIELD_LIMIT * 2
    every { featureFlagClient.intVariation(ConnectionFieldLimitOverride, any()) } returns override

    assertNull(validator.fieldCount(buildCatalog(1, override), CTX))
    assertNotNull(validator.fieldCount(buildCatalog(1, override + 1), CTX))
  }

  @Test
  internal fun `ignores unselected streams`() {
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

    val validator1 =
      CatalogValidator(
        airbyteServerConfiguration =
          AirbyteServerConfiguration(
            connectionLimits =
              AirbyteServerConfiguration.AirbyteServerConnectionLimitConfiguration(
                limits =
                  AirbyteServerConnectionLimitsConfiguration(
                    maxFieldsPerConnection = 10L,
                  ),
              ),
          ),
        metricClient,
        featureFlagClient,
      )
    val validator2 =
      CatalogValidator(
        airbyteServerConfiguration =
          AirbyteServerConfiguration(
            connectionLimits =
              AirbyteServerConfiguration.AirbyteServerConnectionLimitConfiguration(
                limits =
                  AirbyteServerConnectionLimitsConfiguration(
                    maxFieldsPerConnection = 11L,
                  ),
              ),
          ),
        metricClient,
        featureFlagClient,
      )
    val validator3 =
      CatalogValidator(
        airbyteServerConfiguration =
          AirbyteServerConfiguration(
            connectionLimits =
              AirbyteServerConfiguration.AirbyteServerConnectionLimitConfiguration(
                limits =
                  AirbyteServerConnectionLimitsConfiguration(
                    maxFieldsPerConnection = 100L,
                  ),
              ),
          ),
        metricClient,
        featureFlagClient,
      )
    val validator4 =
      CatalogValidator(
        airbyteServerConfiguration =
          AirbyteServerConfiguration(
            connectionLimits =
              AirbyteServerConfiguration.AirbyteServerConnectionLimitConfiguration(
                limits =
                  AirbyteServerConnectionLimitsConfiguration(
                    maxFieldsPerConnection = 200L,
                  ),
              ),
          ),
        metricClient,
        featureFlagClient,
      )
    val validator5 =
      CatalogValidator(
        airbyteServerConfiguration =
          AirbyteServerConfiguration(
            connectionLimits =
              AirbyteServerConfiguration.AirbyteServerConnectionLimitConfiguration(
                limits =
                  AirbyteServerConnectionLimitsConfiguration(
                    maxFieldsPerConnection = 0L,
                  ),
              ),
          ),
        metricClient,
        featureFlagClient,
      )
    val validator6 =
      CatalogValidator(
        airbyteServerConfiguration =
          AirbyteServerConfiguration(
            connectionLimits =
              AirbyteServerConfiguration.AirbyteServerConnectionLimitConfiguration(
                limits =
                  AirbyteServerConnectionLimitsConfiguration(
                    maxFieldsPerConnection = 1L,
                  ),
              ),
          ),
        metricClient,
        featureFlagClient,
      )
    val validator7 =
      CatalogValidator(
        airbyteServerConfiguration =
          AirbyteServerConfiguration(
            connectionLimits =
              AirbyteServerConfiguration.AirbyteServerConnectionLimitConfiguration(
                limits =
                  AirbyteServerConnectionLimitsConfiguration(
                    maxFieldsPerConnection = 9L,
                  ),
              ),
          ),
        metricClient,
        featureFlagClient,
      )
    val validator8 =
      CatalogValidator(
        airbyteServerConfiguration =
          AirbyteServerConfiguration(
            connectionLimits =
              AirbyteServerConfiguration.AirbyteServerConnectionLimitConfiguration(
                limits =
                  AirbyteServerConnectionLimitsConfiguration(
                    maxFieldsPerConnection = 4L,
                  ),
              ),
          ),
        metricClient,
        featureFlagClient,
      )

    assertNull(validator1.fieldCount(catalog, CTX))
    assertNull(validator2.fieldCount(catalog, CTX))
    assertNull(validator3.fieldCount(catalog, CTX))
    assertNull(validator4.fieldCount(catalog, CTX))
    assertNotNull(validator5.fieldCount(catalog, CTX))
    assertNotNull(validator6.fieldCount(catalog, CTX))
    assertNotNull(validator7.fieldCount(catalog, CTX))
    assertNotNull(validator8.fieldCount(catalog, CTX))
  }

  @Test
  internal fun `test getting the field count when no fields are present`() {
    val streamAndConfig =
      AirbyteStreamAndConfiguration()
        .stream(AirbyteStream().jsonSchema(Jsons.jsonNode(emptyMap<String, String>())))
        .config(AirbyteStreamConfiguration().selected(true))
    val streams = mutableListOf<AirbyteStreamAndConfiguration>(streamAndConfig)
    val catalog = AirbyteCatalog().streams(streams)
    val fieldCount = validator.fieldCount(catalog = catalog, ctx = mockk<Connection>())
    assertNull(fieldCount)
  }

  companion object {
    @JvmStatic
    fun validSizeCatalogMatrix() =
      listOf(
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

    @JvmStatic
    fun tooLargeCatalogMatrix() =
      listOf(
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
