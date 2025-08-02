/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal

import io.airbyte.commons.json.Jsons.deserialize
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.commons.json.Jsons.serialize
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.workers.testutils.AirbyteMessageUtils.createRecordMessage
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class AirbyteProtocolPredicateTest {
  private var predicate: AirbyteProtocolPredicate? = null

  @BeforeEach
  fun setup() {
    predicate = AirbyteProtocolPredicate()
  }

  @Test
  fun testValid() {
    Assertions.assertTrue(predicate!!.test(jsonNode<AirbyteMessage?>(createRecordMessage(STREAM_NAME, FIELD_NAME, GREEN))))
  }

  @Test
  fun testInValid() {
    Assertions.assertFalse(predicate!!.test(deserialize("{ \"fish\": \"tuna\"}")))
  }

  @Test
  fun testConcatenatedValid() {
    val concatenated =
      (
        serialize<AirbyteMessage?>(createRecordMessage(STREAM_NAME, FIELD_NAME, GREEN)) +
          serialize<AirbyteMessage?>(createRecordMessage(STREAM_NAME, FIELD_NAME, "yellow"))
      )

    Assertions.assertTrue(predicate!!.test(deserialize(concatenated)))
  }

  @Test
  fun testMissingNewLineAndLineStartsWithValidRecord() {
    val concatenated =
      (
        serialize<AirbyteMessage?>(createRecordMessage(STREAM_NAME, FIELD_NAME, GREEN)) +
          "{ \"fish\": \"tuna\"}"
      )

    Assertions.assertTrue(predicate!!.test(deserialize(concatenated)))
  }

  @Test
  fun testMissingNewLineAndLineStartsWithInvalidRecord() {
    val concatenated =
      (
        "{ \"fish\": \"tuna\"}" +
          serialize<AirbyteMessage?>(createRecordMessage(STREAM_NAME, FIELD_NAME, GREEN))
      )

    Assertions.assertFalse(predicate!!.test(deserialize(concatenated)))
  }

  companion object {
    private const val STREAM_NAME = "user_preferences"
    private const val FIELD_NAME = "favorite_color"
    private const val GREEN = "green"
  }
}
