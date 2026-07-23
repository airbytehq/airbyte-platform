/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.annotation

/**
 * Marker annotation useful for indicating that an item in kotlin which should be considered private, is internal for testing purposes only.
 * Any item marked with this annotation must be treated as though it is private.
 *
 * Similar to the VisibleForTesting annotation that everyone is familiar with.
 */
annotation class InternalForTesting
