/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.validation.json;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Set;

/**
 * This class is syntactic sugar on JsonSchemaValidator to reduce boilerplate code that needs to be
 * written to set up and validate objects declared in ConfigSchema.
 *
 * @param <T> Enum where the object is declared.
 */
public interface ConfigSchemaValidator<T extends Enum<T>> {

  Set<String> validate(T configType, JsonNode objectJson);

  boolean test(T configType, JsonNode objectJson);

  void ensure(T configType, JsonNode objectJson) throws JsonValidationException;

  void ensureAsRuntime(T configType, JsonNode objectJson);

}
