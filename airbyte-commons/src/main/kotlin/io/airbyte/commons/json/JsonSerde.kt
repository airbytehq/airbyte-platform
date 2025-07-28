/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.json

/**
 * Serde: _Ser_ialization + _de_serialization
 *
 * Singleton wrapper around Jsons for use with DI and allow testability via mocking. Add methods here as prudent.
 */
class JsonSerde {
  fun <T> serialize(obj: T): String = Jsons.serialize(obj)

  fun <T> deserialize(
    json: String,
    target: Class<T>,
  ): T? = Jsons.deserialize(json, target)
}
