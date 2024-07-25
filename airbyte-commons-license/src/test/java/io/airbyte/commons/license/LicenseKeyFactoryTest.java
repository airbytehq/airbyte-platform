/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.license;

import io.micronaut.context.annotation.Property;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
@MicronautTest(rebuildContext = true)
class LicenseKeyFactoryTest {

  @Inject
  @Named("licenseKey")
  Optional<String> licenseKey;

  @Test
  @Property(name = "airbyte.license-key",
            value = "testLicenseKey")
  void testLicenseKey() {
    Assertions.assertEquals("testLicenseKey", licenseKey.orElseThrow());
  }

  @Test
  void testEmptyLicenseKey() {
    Assertions.assertTrue(licenseKey.isEmpty());
  }

  @Test
  @Property(name = "airbyte.license-key",
            value = "testLicenseKey")
  @Property(name = "airbyte-yml.license-key",
            value = "testYmlLicenseKey")
  void testLicenseKeyPrefersNonYmlVersion() {
    Assertions.assertEquals("testLicenseKey", licenseKey.orElseThrow());
  }

  @Test
  @Property(name = "airbyte-yml.license-key",
            value = "testYmlLicenseKey")
  void testLicenseKeyFallbackToYmlVersion() {
    Assertions.assertEquals("testYmlLicenseKey", licenseKey.orElseThrow());
  }

}
