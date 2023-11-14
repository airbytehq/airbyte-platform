/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.config.cloud

class MinioCloudConfig(val accessKey: String, val bucket: String, val endpoint: String, val secretAccessKey: String) {
  class Builder {
    private var accessKey: String = ""
    private var bucket: String = ""
    private var endpoint: String = ""
    private var secretAccessKey: String = ""

    fun withAccessKey(accessKey: String): Builder {
      this.accessKey = accessKey
      return this
    }

    fun withBucket(bucket: String): Builder {
      this.bucket = bucket
      return this
    }

    fun withEndpoint(endpoint: String): Builder {
      this.endpoint = endpoint
      return this
    }

    fun withSecretAccessKey(secretAccessKey: String): Builder {
      this.secretAccessKey = secretAccessKey
      return this
    }

    fun build(): MinioCloudConfig {
      return MinioCloudConfig(accessKey, bucket, endpoint, secretAccessKey)
    }
  }

  companion object {
    fun builder(): Builder {
      return Builder()
    }
  }
}
