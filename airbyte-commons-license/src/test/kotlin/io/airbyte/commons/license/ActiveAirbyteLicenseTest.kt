/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.license

import io.airbyte.config.Configs.AirbyteEdition
import io.micronaut.context.annotation.Bean
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Replaces
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.time.ZoneOffset
import java.util.Base64
import java.util.UUID

// Base64 encoded version of:
//    {
//      "license": "pro",
//      "maxNodes": 5,
//      "maxEditors": 5,
//      "enterpriseConnectorIds": ["3f17c355-5717-446b-8857-8ce7c7144531"],
//      "iat": 1725388175,
//      "iss": "Airbyte",
//      "aud": "Airbyte!",
//      "sub": "malik@airbyte.io",
//      "exp": 1756328296731
//    }
@Suppress("ktlint:standard:max-line-length")
private const val TEST_PAYLOAD_ENCODED =
  "ewogICAgICAgICJsaWNlbnNlIjogInBybyIsCiAgICAgICAgIm1heE5vZGVzIjogNSwKICAgICAgICAibWF4RWRpdG9ycyI6IDUsCiAgICAgICAgImVudGVycHJpc2VDb25uZWN0b3JJZHMiOiBbIjNmMTdjMzU1LTU3MTctNDQ2Yi04ODU3LThjZTdjNzE0NDUzMSJdLAogICAgICAgICJpYXQiOiAxNzI1Mzg4MTc1LAogICAgICAgICJpc3MiOiAiQWlyYnl0ZSIsCiAgICAgICAgImF1ZCI6ICJBaXJieXRlISIsCiAgICAgICAgInN1YiI6ICJtYWxpa0BhaXJieXRlLmlvIiwKICAgICAgICAiZXhwIjogMTc1NjMyODI5NjczMQogICAgICB9"

private const val TEST_LICENSE_KEY = "HEADER.$TEST_PAYLOAD_ENCODED.SIGNATURE"

@MicronautTest
@Property(
  name = "airbyte.license-key",
  value = TEST_LICENSE_KEY,
)
internal class ActiveAirbyteLicenseTest {
  companion object {
    val TEST_ENTERPRISE_CONNECTOR_ID: UUID = UUID.fromString("3f17c355-5717-446b-8857-8ce7c7144531")
  }

  @Inject
  private lateinit var activeAirbyteLicense: ActiveAirbyteLicense

  @Test
  fun testLicenseLoadsFromProperty() {
    val license = activeAirbyteLicense.license

    Assertions.assertEquals(5, license.maxEditors)
    Assertions.assertEquals(5, license.maxNodes)
    Assertions.assertEquals(1, license.enterpriseConnectorIds.size)
    Assertions.assertEquals(TEST_ENTERPRISE_CONNECTOR_ID, license.enterpriseConnectorIds.first())
    Assertions.assertEquals(
      2025,
      license.expirationDate
        ?.toInstant()
        ?.atOffset(ZoneOffset.UTC)
        ?.year,
    )
    Assertions.assertEquals(
      8,
      license.expirationDate
        ?.toInstant()
        ?.atOffset(ZoneOffset.UTC)
        ?.month
        ?.value,
    )
    Assertions.assertEquals(AirbyteLicense.LicenseType.PRO, license.type)
    Assertions.assertFalse(license.isEmbedded)
  }

  @Test
  fun testIsProProperty() {
    Assertions.assertTrue(activeAirbyteLicense.isPro)
  }

  @Test
  fun testDirectConstructorStillWorks() {
    // Verify direct construction still works and produces the same result as the Micronaut managed bean.
    val directLicense = ActiveAirbyteLicense(TEST_LICENSE_KEY)

    Assertions.assertEquals(activeAirbyteLicense.license.type, directLicense.license.type)
    Assertions.assertEquals(activeAirbyteLicense.license.maxEditors, directLicense.license.maxEditors)
    Assertions.assertEquals(activeAirbyteLicense.license.maxNodes, directLicense.license.maxNodes)
    Assertions.assertEquals(activeAirbyteLicense.isPro, directLicense.isPro)
  }

  @Test
  fun testInvalidLicenseKeyWithWrongFragmentCount() {
    // License key with only 2 fragments instead of 3 (HEADER.PAYLOAD.SIGNATURE)
    val invalidLicense = ActiveAirbyteLicense("HEADER.PAYLOAD")

    Assertions.assertEquals(AirbyteLicense.LicenseType.INVALID, invalidLicense.license.type)
    Assertions.assertFalse(invalidLicense.isPro)
    Assertions.assertNull(invalidLicense.license.expirationDate)
    Assertions.assertNull(invalidLicense.license.maxEditors)
    Assertions.assertNull(invalidLicense.license.maxNodes)
    Assertions.assertTrue(invalidLicense.license.enterpriseConnectorIds.isEmpty())
  }

  @Test
  fun testInvalidLicenseKeyWithMalformedBase64() {
    // License key with invalid Base64 encoding
    val invalidLicense = ActiveAirbyteLicense("HEADER.INVALID_BASE64!@#.SIGNATURE")

    Assertions.assertEquals(AirbyteLicense.LicenseType.INVALID, invalidLicense.license.type)
    Assertions.assertFalse(invalidLicense.isPro)
  }

  @Test
  fun testInvalidLicenseKeyWithMalformedJson() {
    // Create a Base64 encoded invalid JSON
    val invalidJson = "{ invalid json structure"
    val encodedInvalidJson = Base64.getEncoder().encodeToString(invalidJson.toByteArray())
    val invalidLicense = ActiveAirbyteLicense("HEADER.$encodedInvalidJson.SIGNATURE")

    Assertions.assertEquals(AirbyteLicense.LicenseType.INVALID, invalidLicense.license.type)
    Assertions.assertFalse(invalidLicense.isPro)
  }

  @Test
  fun testLicenseWithMissingRequiredFields() {
    // JSON missing required fields (license and exp)
    val incompleteJson = """{"maxNodes": 5, "maxEditors": 3}"""
    val encodedIncompleteJson = Base64.getEncoder().encodeToString(incompleteJson.toByteArray())
    val incompleteLicense = ActiveAirbyteLicense("HEADER.$encodedIncompleteJson.SIGNATURE")

    Assertions.assertEquals(AirbyteLicense.LicenseType.INVALID, incompleteLicense.license.type)
  }

  @Bean
  @Singleton
  @Replaces(AirbyteEdition::class)
  fun airbyteEdition(): AirbyteEdition = AirbyteEdition.ENTERPRISE
}
