/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol

import io.airbyte.commons.protocol.serde.AirbyteMessageDeserializer
import io.airbyte.commons.protocol.serde.AirbyteMessageSerializer
import io.airbyte.commons.version.Version
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito

internal class AirbyteMessageSerDeProviderTest {
  var serDeProvider: AirbyteMessageSerDeProvider? = null
  var deserV0: AirbyteMessageDeserializer<String>? = null
  var deserV1: AirbyteMessageDeserializer<String>? = null

  var serV0: AirbyteMessageSerializer<String>? = null
  var serV1: AirbyteMessageSerializer<String>? = null

  @BeforeEach
  fun beforeEach() {
    serDeProvider = AirbyteMessageSerDeProvider()

    deserV0 = buildDeserializer(Version("0.1.0"))
    deserV1 = buildDeserializer(Version("1.1.0"))
    serDeProvider!!.registerDeserializer(deserV0!!)
    serDeProvider!!.registerDeserializer(deserV1!!)

    serV0 = buildSerializer(Version("0.2.0"))
    serV1 = buildSerializer(Version("1.0.0"))
    serDeProvider!!.registerSerializer(serV0!!)
    serDeProvider!!.registerSerializer(serV1!!)
  }

  @Test
  fun testGetDeserializer() {
    Assertions.assertEquals(deserV0!!, serDeProvider!!.getDeserializer(Version("0.1.0")))
    Assertions.assertEquals(deserV0!!, serDeProvider!!.getDeserializer(Version("0.2.0")))
    Assertions.assertEquals(deserV1!!, serDeProvider!!.getDeserializer(Version("1.1.0")))
    Assertions.assertNull(serDeProvider!!.getDeserializer(Version("2.0.0")))
  }

  @Test
  fun testGetSerializer() {
    Assertions.assertEquals(serV0!!, serDeProvider!!.getSerializer(Version("0.1.0")))
    Assertions.assertEquals(serV1!!, serDeProvider!!.getSerializer(Version("1.0.0")))
    Assertions.assertNull(serDeProvider!!.getSerializer(Version("3.2.0")))
  }

  @Test
  fun testRegisterDeserializerShouldFailOnVersionCollision() {
    val deser: AirbyteMessageDeserializer<*> = buildDeserializer<Any>(Version("0.2.0"))
    Assertions.assertThrows(RuntimeException::class.java) {
      serDeProvider!!.registerDeserializer(deser)
    }
  }

  @Test
  fun testRegisterSerializerShouldFailOnVersionCollision() {
    val ser: AirbyteMessageSerializer<*> = buildSerializer<Any>(Version("0.5.0"))
    Assertions.assertThrows(RuntimeException::class.java) {
      serDeProvider!!.registerSerializer(ser)
    }
  }

  private fun <T : Any> buildDeserializer(version: Version): AirbyteMessageDeserializer<T> {
    val deser: AirbyteMessageDeserializer<T> = Mockito.mock()
    Mockito.`when`(deser.getTargetVersion()).thenReturn(version)
    return deser
  }

  private fun <T : Any> buildSerializer(version: Version): AirbyteMessageSerializer<T> {
    val ser: AirbyteMessageSerializer<T> = Mockito.mock()
    Mockito.`when`(ser.getTargetVersion()).thenReturn(version)
    return ser
  }
}
