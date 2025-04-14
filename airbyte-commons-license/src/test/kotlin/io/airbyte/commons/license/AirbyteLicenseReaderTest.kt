/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.license

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.time.ZoneOffset
import java.util.Base64
import java.util.UUID

internal class AirbyteLicenseReaderTest {
  fun testDeserializeValidLicenseNoIsEmbedded() {
    val payload =
      Jsons.deserialize(
        """
      {
        "license": "pro",
        "maxNodes": 5,
        "maxEditors": 5,
        "enterpriseConnectorIds": ["3f17c355-5717-446b-8857-8ce7c7144531"],
        "iat": 1725388175,
        "iss": "Airbyte",
        "aud": "Airbyte!",
        "sub": "malik@airbyte.io",
        "exp": 1756328296731
      }
      """,
      )
    val licenseKey = makeDummyKey(payload)

    val license = AirbyteLicenseReader(licenseKey).extractLicense()
    Assertions.assertEquals(5, license.maxEditors)
    Assertions.assertEquals(5, license.maxNodes)
    Assertions.assertEquals(1, license.enterpriseConnectorIds.size)
    Assertions.assertEquals(
      UUID.fromString("3f17c355-5717-446b-8857-8ce7c7144531"),
      license.enterpriseConnectorIds.first(),
    )
    Assertions.assertEquals(
      2025,
      license.expirationDate!!
        .toInstant()
        .atOffset(ZoneOffset.UTC)
        .year,
    )
    Assertions.assertEquals(
      8,
      license.expirationDate!!
        .toInstant()
        .atOffset(ZoneOffset.UTC)
        .month.value,
    )
    Assertions.assertSame(AirbyteLicense.LicenseType.PRO, license.type)
    Assertions.assertFalse(license.isEmbedded)
  }

  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun testDeserializeValidLicenseWithIsEmbedded(isEmbedded: Boolean) {
    val payload =
      Jsons.deserialize(
        """
      {
        "license": "pro",
        "maxNodes": 5,
        "maxEditors": 5,
        "enterpriseConnectorIds": ["3f17c355-5717-446b-8857-8ce7c7144531"],
        "iat": 1725388175,
        "iss": "Airbyte",
        "aud": "Airbyte!",
        "sub": "malik@airbyte.io",
        "exp": 1756328296731,
        "isEmbedded": $isEmbedded
      }
      """,
      )
    val licenseKey = makeDummyKey(payload)

    val license = AirbyteLicenseReader(licenseKey).extractLicense()
    Assertions.assertEquals(5, license.maxEditors)
    Assertions.assertEquals(5, license.maxNodes)
    Assertions.assertEquals(1, license.enterpriseConnectorIds.size)
    Assertions.assertEquals(
      UUID.fromString("3f17c355-5717-446b-8857-8ce7c7144531"),
      license.enterpriseConnectorIds.first(),
    )
    Assertions.assertEquals(
      2025,
      license.expirationDate!!
        .toInstant()
        .atOffset(ZoneOffset.UTC)
        .year,
    )
    Assertions.assertEquals(
      8,
      license.expirationDate!!
        .toInstant()
        .atOffset(ZoneOffset.UTC)
        .month.value,
    )
    Assertions.assertSame(AirbyteLicense.LicenseType.PRO, license.type)
    Assertions.assertEquals(license.isEmbedded, isEmbedded)
  }

  @Test
  fun testDeserializeInvalidJwt() {
    val licenseKey =
      (
        "eyJhbGciOiJIUzI1NiJ9.eyJsaWNlbnNlIjoicHJvIiwibWF4Tm9kZXMiOjUsIm1heEVkaXRvcnMiOjUsImlhdC" +
          "I6MTcyNTM4ODE3NSwiaXNzIjoiQWlyYnl0ZSIsImF1ZCI6IkFpcmJ5dGUhIiwic3ViIjoibWFsaWtAYWlyYnl0" +
          "ZS5pbyIsImV4cCI6MTc1NjMyODI5NjczMX0"
      )
    val license = AirbyteLicenseReader(licenseKey).extractLicense()
    Assertions.assertSame(AirbyteLicense.LicenseType.INVALID, license.type)
  }

  @Test
  fun testDeserializeInvalidLicense() {
    val payload =
      Jsons.deserialize(
        """
      {
        "maxNodes": 5,
        "maxEditors": 5,
        "iat": 1725388175,
        "iss": "Airbyte",
        "aud": "Airbyte!",
        "sub": "malik@airbyte.io",
        "exp": 1756328296731
      }
      """,
      )
    val licenseKey = makeDummyKey(payload)
    val license = AirbyteLicenseReader(licenseKey).extractLicense()
    Assertions.assertSame(AirbyteLicense.LicenseType.INVALID, license.type)
  }

  private fun makeDummyKey(payload: JsonNode): String {
    val encodedPayload = Base64.getEncoder().encodeToString(Jsons.serialize(payload).toByteArray())
    return "HEADER.$encodedPayload.SIGNATURE"
  }
}
