/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.commons.license

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.time.ZoneOffset

internal class AirbyteLicenseReaderTest {
  @Test
  fun testDeserializeValidLicense() {
    val licenseKey =
      (
        "eyJhbGciOiJIUzI1NiJ9.eyJsaWNlbnNlIjoicHJvIiwibWF4Tm9kZXMiOjUsIm1heEVkaXRvcnMiOjUsImlhd" +
          "CI6MTcyNTM4ODE3NSwiaXNzIjoiQWlyYnl0ZSIsImF1ZCI6IkFpcmJ5dGUhIiwic3ViIjoibWFsaWtAYWlyYn" +
          "l0ZS5pbyIsImV4cCI6MTc1NjMyODI5NjczMX0.yoIedwLBWFySl5zZxLWQ4FdtVot7IbGOspzLhpIFhZo"
      )
    val license = AirbyteLicenseReader(licenseKey).extractLicense()
    Assertions.assertEquals(5, license.maxEditors)
    Assertions.assertEquals(5, license.maxNodes)
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
    val licenseKey =
      (
        "eyJhbGciOiJIUzI1NiJ9.eyJtYXhOb2RlcyI6NSwibWF4RWRpdG9ycyI6NSwiaWF0IjoxNzI1Mzg4MTc1LCJpc3M" +
          "iOiJBaXJieXRlIiwiYXVkIjoiQWlyYnl0ZSEiLCJzdWIiOiJtYWxpa0BhaXJieXRlLmlvIiwiZXhwIjoxNzU2MzI" +
          "4Mjk2NzMxfQ.FM_kuEhbA0SGgIZBPDjN9B_hX-a0LBSNvcgeIh4RCyg"
      )
    val license = AirbyteLicenseReader(licenseKey).extractLicense()
    Assertions.assertSame(AirbyteLicense.LicenseType.INVALID, license.type)
  }
}
