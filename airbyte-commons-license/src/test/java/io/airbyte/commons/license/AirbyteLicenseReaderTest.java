/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.license;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import org.junit.jupiter.api.Test;

class AirbyteLicenseReaderTest {

  @Test
  void testDeserializeValidLicense() {
    final String licenseKey =
        "eyJhbGciOiJIUzI1NiJ9.eyJsaWNlbnNlIjoicHJvIiwibWF4Tm9kZXMiOjUsIm1heEVkaXRvcnMiOjUsImlhd"
            + "CI6MTcyNTM4ODE3NSwiaXNzIjoiQWlyYnl0ZSIsImF1ZCI6IkFpcmJ5dGUhIiwic3ViIjoibWFsaWtAYWlyYn"
            + "l0ZS5pbyIsImV4cCI6MTc1NjMyODI5NjczMX0.yoIedwLBWFySl5zZxLWQ4FdtVot7IbGOspzLhpIFhZo";
    final AirbyteLicense license = new AirbyteLicenseReader(licenseKey).extractLicense();
    assertEquals(5, license.maxEditors().get());
    assertEquals(5, license.maxNodes().get());
    assertEquals(2025, license.expirationDate().get().toInstant().atOffset(ZoneOffset.UTC).get(ChronoField.YEAR));
    assertEquals(8, license.expirationDate().get().toInstant().atOffset(ZoneOffset.UTC).get(ChronoField.MONTH_OF_YEAR));
    assertSame(AirbyteLicense.LicenseType.PRO, license.type());
  }

  @Test
  void testDeserializeInvalidJwt() {
    final String licenseKey =
        "eyJhbGciOiJIUzI1NiJ9.eyJsaWNlbnNlIjoicHJvIiwibWF4Tm9kZXMiOjUsIm1heEVkaXRvcnMiOjUsImlhdC"
            + "I6MTcyNTM4ODE3NSwiaXNzIjoiQWlyYnl0ZSIsImF1ZCI6IkFpcmJ5dGUhIiwic3ViIjoibWFsaWtAYWlyYnl0"
            + "ZS5pbyIsImV4cCI6MTc1NjMyODI5NjczMX0";
    final AirbyteLicense license = new AirbyteLicenseReader(licenseKey).extractLicense();
    assertSame(AirbyteLicense.LicenseType.INVALID, license.type());
  }

  @Test
  void testDeserializeInvalidLicense() {
    final String licenseKey =
        "eyJhbGciOiJIUzI1NiJ9.eyJtYXhOb2RlcyI6NSwibWF4RWRpdG9ycyI6NSwiaWF0IjoxNzI1Mzg4MTc1LCJpc3M"
            + "iOiJBaXJieXRlIiwiYXVkIjoiQWlyYnl0ZSEiLCJzdWIiOiJtYWxpa0BhaXJieXRlLmlvIiwiZXhwIjoxNzU2MzI"
            + "4Mjk2NzMxfQ.FM_kuEhbA0SGgIZBPDjN9B_hX-a0LBSNvcgeIh4RCyg";
    final AirbyteLicense license = new AirbyteLicenseReader(licenseKey).extractLicense();
    assertSame(AirbyteLicense.LicenseType.INVALID, license.type());
  }

}
