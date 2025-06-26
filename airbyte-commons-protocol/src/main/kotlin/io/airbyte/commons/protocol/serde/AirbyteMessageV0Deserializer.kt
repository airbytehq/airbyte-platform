/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol.serde

import io.airbyte.commons.version.AirbyteProtocolVersion
import io.airbyte.protocol.models.v0.AirbyteMessage
import jakarta.inject.Singleton

/**
 * Deserializer for Protocol V0.
 */
@Singleton
class AirbyteMessageV0Deserializer : AirbyteMessageGenericDeserializer<AirbyteMessage>(AirbyteProtocolVersion.V0, AirbyteMessage::class.java)
