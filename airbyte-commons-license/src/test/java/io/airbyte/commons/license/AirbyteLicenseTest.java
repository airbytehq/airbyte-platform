/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.license;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airbyte.commons.json.Jsons;
import org.junit.jupiter.api.Test;

class AirbyteLicenseTest {

  @Test
  void testDeserializeTypeOnlyLicense() {
    final var result = Jsons.deserialize("""
                                         {
                                           "type": "pro"
                                         }
                                         """, AirbyteLicense.class);
    assertEquals(result.type(), AirbyteLicense.LicenseType.PRO);
    assertTrue(result.expirationDate().isEmpty());
    assertTrue(result.maxEditors().isEmpty());
    assertTrue(result.maxNodes().isEmpty());

  }

}
