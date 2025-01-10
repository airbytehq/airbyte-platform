/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.data.services.impls.jooq

import com.google.common.io.Resources
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.protocol.DefaultProtocolSerializer
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.nio.charset.Charset
import io.airbyte.config.ConfiguredAirbyteCatalog as InternalConfiguredAirbyteCatalog
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog as ProtocolConfiguredAirbyteCatalog

internal class DbConverterTest {
  @ValueSource(
    strings = [
      "non-reg-configured-catalog/facebook.json",
      "non-reg-configured-catalog/ga.json",
      "non-reg-configured-catalog/hubspot.json",
      "non-reg-configured-catalog/internal-pg.json",
      "non-reg-configured-catalog/stripe.json",
    ],
  )
  @ParameterizedTest
  fun testConfiguredAirbyteCatalogDeserNonReg(resourceName: String) {
    val rawCatalog = Resources.toString(Resources.getResource(resourceName), Charset.defaultCharset())

    val protocolCatalog = parseConfiguredAirbyteCatalogAsProtocol(rawCatalog)
    val internalCatalog = parseConfiguredAirbyteCatalog(rawCatalog)

    assertCatalogsAreEqual(internalCatalog, protocolCatalog)
  }

  private fun assertCatalogsAreEqual(
    internal: InternalConfiguredAirbyteCatalog,
    protocol: ProtocolConfiguredAirbyteCatalog,
  ) {
    val internalToProtocol = parseConfiguredAirbyteCatalogAsProtocol(DefaultProtocolSerializer().serialize(internal, false))
    Assertions.assertEquals(protocol, internalToProtocol)
  }

  companion object {
    // This is hardcoded here on purpose for regression tests
    // Until we defined our internal model (<=0.58), this was how we were loading catalog strings from
    // our persistence layer.
    // This is to ensure we remain backward compatible so it should remain as is until we drop support
    private fun parseConfiguredAirbyteCatalogAsProtocol(catalogString: String): ProtocolConfiguredAirbyteCatalog {
      return Jsons.deserialize(catalogString, ProtocolConfiguredAirbyteCatalog::class.java)
    }

    private fun parseConfiguredAirbyteCatalog(catalogString: String): InternalConfiguredAirbyteCatalog {
      // TODO this should be using the proper SerDe stack once migrated to support our internal models
      // This is making sure our new format can still load older serialization format
      return Jsons.deserialize(catalogString, InternalConfiguredAirbyteCatalog::class.java)
    }
  }
}
