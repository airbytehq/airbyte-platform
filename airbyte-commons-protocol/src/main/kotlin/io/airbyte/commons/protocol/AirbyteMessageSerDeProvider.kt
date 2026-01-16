/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.protocol

import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.commons.protocol.serde.AirbyteMessageDeserializer
import io.airbyte.commons.protocol.serde.AirbyteMessageSerializer
import io.airbyte.commons.version.Version
import jakarta.annotation.PostConstruct
import jakarta.inject.Singleton
import java.util.function.Consumer

/**
 * AirbyteProtocol Message Serializer/Deserializer provider
 *
 * This class is intended to help access the serializer/deserializer for a given version of the
 * Airbyte Protocol.
 */
@Singleton
class AirbyteMessageSerDeProvider
  @JvmOverloads
  constructor(
    private val deserializersToRegister: List<AirbyteMessageDeserializer<*>> = emptyList(),
    private val serializersToRegister: List<AirbyteMessageSerializer<*>> = emptyList(),
  ) {
    private val deserializers: MutableMap<String, AirbyteMessageDeserializer<*>> = HashMap()
    private val serializers: MutableMap<String, AirbyteMessageSerializer<*>> = HashMap()

    @PostConstruct
    fun initialize() {
      deserializersToRegister.forEach(Consumer { deserializer: AirbyteMessageDeserializer<*> -> this.registerDeserializer(deserializer) })
      serializersToRegister.forEach(Consumer { serializer: AirbyteMessageSerializer<*> -> this.registerSerializer(serializer) })
    }

    /**
     * Returns the Deserializer for the version if known else empty.
     */
    fun getDeserializer(version: Version): AirbyteMessageDeserializer<*>? = deserializers[version.getMajorVersion()]

    /**
     * Returns the Serializer for the version if known else empty.
     */
    fun getSerializer(version: Version): AirbyteMessageSerializer<*>? = serializers[version.getMajorVersion()]

    @InternalForTesting
    fun registerDeserializer(deserializer: AirbyteMessageDeserializer<*>) {
      val key = deserializer.getTargetVersion().getMajorVersion()!!
      if (!deserializers.containsKey(key)) {
        deserializers[key] = deserializer
      } else {
        throw RuntimeException(
          String.format(
            "Trying to register a deserializer for protocol version {} when {} already exists",
            deserializer.getTargetVersion().serialize(),
            deserializers[key]!!.getTargetVersion().serialize(),
          ),
        )
      }
    }

    @InternalForTesting
    fun registerSerializer(serializer: AirbyteMessageSerializer<*>) {
      val key = serializer.getTargetVersion()!!.getMajorVersion()!!
      if (!serializers.containsKey(key)) {
        serializers[key] = serializer
      } else {
        throw RuntimeException(
          String.format(
            "Trying to register a serializer for protocol version {} when {} already exists",
            serializer.getTargetVersion()!!.serialize(),
            serializers[key]!!.getTargetVersion()!!.serialize(),
          ),
        )
      }
    }

    @get:InternalForTesting
    val deserializerKeys: Set<String>
      // Used for inspection of the injection
      get() = deserializers.keys

    @get:InternalForTesting
    val serializerKeys: Set<String>
      // Used for inspection of the injection
      get() = serializers.keys
  }
