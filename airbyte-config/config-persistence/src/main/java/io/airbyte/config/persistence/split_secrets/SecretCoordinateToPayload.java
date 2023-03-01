/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.persistence.split_secrets;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * POJO to hold secret coordinate and the secret.
 *
 * @param secretCoordinate secret coordinate
 * @param payload the secret
 * @param secretCoordinateForDB json coordinate
 */
// todo (cgardens) - i don't understand these params.
public record SecretCoordinateToPayload(SecretCoordinate secretCoordinate,
                                        String payload,
                                        JsonNode secretCoordinateForDB) {

}
