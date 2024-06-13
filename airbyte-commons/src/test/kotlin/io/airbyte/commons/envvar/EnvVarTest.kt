package io.airbyte.commons.envvar

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

class EnvVarTest {
  @Test
  fun `entries are in alphabetical order`() {
    // if this fails, the output is difficult to parse
    assertEquals(EnvVar.entries.sortedBy { it.name }, EnvVar.entries)
  }

  @Test
  fun `fetch returns the correct value when set`() {
    assertEquals("value-defined", EnvVar.Z_TESTING_PURPOSES_ONLY_1.fetch())
    assertEquals("value-defined", EnvVar.Z_TESTING_PURPOSES_ONLY_1.fetch(default = "not this value"))
  }

  @Test
  fun `fetch returns the default value if missing or blank`() {
    val default = "defined as blank, so should return this value instead"
    assertEquals(default, EnvVar.Z_TESTING_PURPOSES_ONLY_2.fetch(default = default))
  }

  @Test
  fun `fetch returns null when unset and no default defined`() {
    assertNull(EnvVar.Z_TESTING_PURPOSES_ONLY_3.fetch())
  }
}
