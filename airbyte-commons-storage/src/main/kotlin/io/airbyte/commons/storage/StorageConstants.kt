/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.storage

/** The root node which contains the configuration settings for the storage client. */
internal const val STORAGE_ROOT = "airbyte.cloud.storage"

/** Specific settings for Azure Blob Storage are found here. Only used if `[STORAGE_ROOT].type` is `azure`. */
internal const val STORAGE_AZURE = "$STORAGE_ROOT.azure"

/** Specific settings for Google Cloud Storage are found here. Only used if `[STORAGE_ROOT].type` is `gcs`. */
internal const val STORAGE_GCS = "$STORAGE_ROOT.gcs"

/** Specific settings for MinIO are found here. Only used if `[STORAGE_ROOT].type` is `minio`. */
internal const val STORAGE_MINIO = "$STORAGE_ROOT.minio"

/** Specific settings for S3 are found here. Only used if `[STORAGE_ROOT].type` is `s3`. */
internal const val STORAGE_S3 = "$STORAGE_ROOT.s3"

/** Specific settings for buckets, specifically their names. */
internal const val STORAGE_BUCKET = "$STORAGE_ROOT.bucket"

/** The setting that contains what storage type the client represents. */
const val STORAGE_TYPE = "$STORAGE_ROOT.type"

/** The mount point used for local storage. */
const val STORAGE_MOUNT = "/storage"

/** The name of the volume used for local storage. */
const val STORAGE_VOLUME_NAME = "airbyte-storage"

/** The name of the persistent volume claim (PVC) used for local storage. */
const val STORAGE_CLAIM_NAME = "airbyte-storage-pvc"
