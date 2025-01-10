/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.commons.license

import io.micronaut.context.ApplicationContext
import io.micronaut.context.annotation.Property
import io.micronaut.context.exceptions.NoSuchBeanException
import io.micronaut.kotlin.context.getBean
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import jakarta.inject.Named
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

@MicronautTest(rebuildContext = true)
internal class LicenseKeyFactoryTest {
  @Inject
  @Named("licenseKey")
  var licenseKey: String? = null

  @Test
  @Property(name = "airbyte.license-key", value = "testLicenseKey")
  fun testLicenseKey() {
    Assertions.assertEquals("testLicenseKey", licenseKey)
  }

  @Test
  @Property(name = "airbyte.license-key", value = "testLicenseKey")
  @Property(name = "airbyte-yml.license-key", value = "testYmlLicenseKey")
  fun testLicenseKeyPrefersNonYmlVersion() {
    Assertions.assertEquals("testLicenseKey", licenseKey)
  }

  @Test
  @Property(name = "airbyte-yml.license-key", value = "testYmlLicenseKey")
  fun testLicenseKeyFallbackToYmlVersion() {
    Assertions.assertEquals("testYmlLicenseKey", licenseKey)
  }
}

@MicronautTest
internal class MissingLicenseKeyFactoryTest {
  @Inject
  lateinit var appCtx: ApplicationContext

  @Test
  fun `missing properties throws an exception`() {
    val exception =
      assertThrows(NoSuchBeanException::class.java) {
        appCtx.getBean<String>("licenseKey")
      }

    assertTrue(exception.message?.contains("License key not provided.")?.let { true } ?: false)
  }
}
