/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.auth.config

import io.micronaut.context.BeanContext
import io.micronaut.context.annotation.Property
import io.micronaut.context.exceptions.BeanInstantiationException
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

@MicronautTest(rebuildContext = true)
class InitialUserConfigFactoryTest {
  @Inject
  lateinit var beanContext: BeanContext

  @Test
  fun `test defaultInitialUserConfig with no properties set`() {
    val initialUserConfig = beanContext.findBean(InitialUserConfig::class.java)
    Assertions.assertTrue(initialUserConfig.isEmpty)
  }

  @Test
  // factory should prefer these properties
  @Property(name = "airbyte.auth.initial-user.email", value = "test@airbyte.io")
  @Property(name = "airbyte.auth.initial-user.password", value = "myPassword")
  @Property(name = "airbyte.auth.initial-user.first-name", value = "Firstname")
  @Property(name = "airbyte.auth.initial-user.last-name", value = "Lastname")
  fun `test defaultInitialUserConfig with all properties set`() {
    val initialUserConfig = beanContext.findBean(InitialUserConfig::class.java)
    Assertions.assertTrue(initialUserConfig.isPresent)
    Assertions.assertEquals("test@airbyte.io", initialUserConfig.get().email)
    Assertions.assertEquals("myPassword", initialUserConfig.get().password)
    Assertions.assertEquals("Firstname", initialUserConfig.get().firstName)
    Assertions.assertEquals("Lastname", initialUserConfig.get().lastName)
  }

  @Test
  @Property(name = "airbyte.auth.initial-user.email", value = "test@airbyte.io")
  @Property(name = "airbyte.auth.initial-user.password", value = "myPassword")
  fun `test defaultInitialUserConfig works without username or password`() {
    val initialUserConfig = beanContext.findBean(InitialUserConfig::class.java)
    Assertions.assertTrue(initialUserConfig.isPresent)
    Assertions.assertEquals("test@airbyte.io", initialUserConfig.get().email)
    Assertions.assertEquals("myPassword", initialUserConfig.get().password)
    Assertions.assertNull(initialUserConfig.get().firstName)
    Assertions.assertNull(initialUserConfig.get().lastName)
  }

  @Test
  @Property(name = "airbyte.auth.initial-user.email", value = "test@airbyte.io")
  fun `test defaultInitialUserConfig with missing properties empty bean`() {
    Assertions.assertThrows(BeanInstantiationException::class.java) {
      beanContext.findBean(InitialUserConfig::class.java)
    }
  }
}
