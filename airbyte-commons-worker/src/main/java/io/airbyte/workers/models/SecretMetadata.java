/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.models;

/**
 * Secret metadata used to determine the secret name & key. Used to build a secret key selector to
 * push in an env var to worker pods.
 *
 * @param secretName name of the secret
 * @param secretKey key of the secret which holds the value.
 */
public record SecretMetadata(String secretName, String secretKey) {

}
