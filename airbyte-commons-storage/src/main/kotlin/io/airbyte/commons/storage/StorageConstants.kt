/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.storage

/** The mount point used for local storage. */
const val STORAGE_MOUNT = "/storage"

/** The name of the volume used for local storage. */
const val STORAGE_VOLUME_NAME = "airbyte-storage"

/** The name of the persistent volume claim (PVC) used for local storage. */
const val STORAGE_CLAIM_NAME = "airbyte-storage-pvc"
