/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol

import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

@MicronautTest
internal class AirbyteMessageSerDeProviderMicronautTest {
  @Inject
  var serDeProvider: AirbyteMessageSerDeProvider? = null

  @Test
  fun testSerDeInjection() {
    // This should contain the list of all the supported majors of the airbyte protocol
    val expectedVersions: Set<String> = HashSet(listOf("0"))

    Assertions.assertEquals(expectedVersions, serDeProvider!!.deserializerKeys)
    Assertions.assertEquals(expectedVersions, serDeProvider!!.serializerKeys)
  }
}
