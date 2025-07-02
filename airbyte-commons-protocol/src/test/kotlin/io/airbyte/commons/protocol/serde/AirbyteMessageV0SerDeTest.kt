/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.serde

import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.ConnectorSpecification
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.net.URI
import java.net.URISyntaxException

internal class AirbyteMessageV0SerDeTest {
  @Test
  @Throws(URISyntaxException::class)
  fun v0SerDeRoundTripTest() {
    val deser = AirbyteMessageV0Deserializer()
    val ser = AirbyteMessageV0Serializer()

    val message =
      AirbyteMessage()
        .withType(AirbyteMessage.Type.SPEC)
        .withSpec(
          ConnectorSpecification()
            .withProtocolVersion("0.3.0")
            .withDocumentationUrl(URI("file:///tmp/doc")),
        )

    val serializedMessage = ser.serialize(message)
    val deserializedMessage = deser.deserializeExact(serializedMessage)

    Assertions.assertEquals(message, deserializedMessage.get())
  }
}
