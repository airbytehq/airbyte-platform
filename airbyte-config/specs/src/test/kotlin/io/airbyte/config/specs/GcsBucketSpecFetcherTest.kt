/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.specs

import com.google.cloud.storage.Blob
import com.google.cloud.storage.Storage
import io.airbyte.commons.json.Jsons
import io.airbyte.config.Configs
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.util.Optional

internal class GcsBucketSpecFetcherTest {
  lateinit var storage: Storage
  lateinit var defaultSpecBlob: Blob
  lateinit var cloudSpecBlob: Blob
  private val defaultSpec: ConnectorSpecification =
    ConnectorSpecification()
      .withConnectionSpecification(Jsons.jsonNode(mapOf("foo" to "bar", "mode" to "oss")))
  private val cloudSpec: ConnectorSpecification =
    ConnectorSpecification()
      .withConnectionSpecification(Jsons.jsonNode(mapOf("foo" to "bar", "mode" to "cloud")))

  @BeforeEach
  fun setup() {
    storage = mockk()

    defaultSpecBlob = mockk()
    every { defaultSpecBlob.getContent() } returns Jsons.toBytes(Jsons.jsonNode(defaultSpec))
    cloudSpecBlob = mockk()
    every { cloudSpecBlob.getContent() } returns Jsons.toBytes(Jsons.jsonNode(cloudSpec))
  }

  @Test
  fun testGetsSpecIfPresent() {
    every { storage[BUCKET_NAME, DEFAULT_SPEC_PATH] } returns defaultSpecBlob

    val bucketSpecFetcher = GcsBucketSpecFetcher(storage, BUCKET_NAME)
    val returnedSpec = bucketSpecFetcher.attemptFetch(DOCKER_IMAGE)

    Assertions.assertTrue(returnedSpec.isPresent)
    Assertions.assertEquals(defaultSpec, returnedSpec.get())
  }

  @Test
  fun testReturnsEmptyIfNotPresent() {
    every { storage[BUCKET_NAME, DEFAULT_SPEC_PATH] } returns null

    val bucketSpecFetcher = GcsBucketSpecFetcher(storage, BUCKET_NAME)
    val returnedSpec = bucketSpecFetcher.attemptFetch(DOCKER_IMAGE)

    Assertions.assertTrue(returnedSpec.isEmpty)
  }

  @Test
  fun testReturnsEmptyIfInvalidSpec() {
    val invalidSpecBlob = mockk<Blob>()
    every { invalidSpecBlob.getContent() } returns "{\"notASpec\": true}".toByteArray(StandardCharsets.UTF_8)
    every { storage[BUCKET_NAME, DEFAULT_SPEC_PATH] } returns invalidSpecBlob

    val bucketSpecFetcher = GcsBucketSpecFetcher(storage, BUCKET_NAME)
    val returnedSpec = bucketSpecFetcher.attemptFetch(DOCKER_IMAGE)

    Assertions.assertTrue(returnedSpec.isEmpty)
  }

  /**
   * Test [GcsBucketSpecFetcher.getSpecAsBlob].
   */
  @Test
  fun testDynamicGetSpecAsBlob() {
    every { storage[BUCKET_NAME, DEFAULT_SPEC_PATH] } returns defaultSpecBlob
    every { storage[BUCKET_NAME, CLOUD_SPEC_PATH] } returns cloudSpecBlob

    // under deploy deployment mode, cloud spec file will be ignored even when it exists
    val defaultBucketSpecFetcher = GcsBucketSpecFetcher(storage, BUCKET_NAME)
    Assertions.assertEquals(
      Optional.of(defaultSpecBlob),
      defaultBucketSpecFetcher.getSpecAsBlob(DOCKER_REPOSITORY, DOCKER_IMAGE_TAG),
    )

    // under OSS deployment mode, cloud spec file will be ignored even when it exists
    val ossBucketSpecFetcher = GcsBucketSpecFetcher(storage, BUCKET_NAME, Configs.AirbyteEdition.COMMUNITY)
    Assertions.assertEquals(
      Optional.of(defaultSpecBlob),
      ossBucketSpecFetcher.getSpecAsBlob(DOCKER_REPOSITORY, DOCKER_IMAGE_TAG),
    )

    val enterpriseBucketSpecFetcher = GcsBucketSpecFetcher(storage, BUCKET_NAME, Configs.AirbyteEdition.ENTERPRISE)
    Assertions.assertEquals(
      Optional.of(defaultSpecBlob),
      enterpriseBucketSpecFetcher.getSpecAsBlob(DOCKER_REPOSITORY, DOCKER_IMAGE_TAG),
    )

    val cloudBucketSpecFetcher = GcsBucketSpecFetcher(storage, BUCKET_NAME, Configs.AirbyteEdition.CLOUD)
    Assertions.assertEquals(
      Optional.of(cloudSpecBlob),
      cloudBucketSpecFetcher.getSpecAsBlob(DOCKER_REPOSITORY, DOCKER_IMAGE_TAG),
    )
  }

  /**
   * Test
   * [GcsBucketSpecFetcher.getSpecAsBlob].
   */
  @Test
  fun testBasicGetSpecAsBlob() {
    every { storage[BUCKET_NAME, DEFAULT_SPEC_PATH] } returns defaultSpecBlob
    every { storage[BUCKET_NAME, CLOUD_SPEC_PATH] } returns cloudSpecBlob

    val bucketSpecFetcher = GcsBucketSpecFetcher(storage, BUCKET_NAME)
    Assertions.assertEquals(
      Optional.of(defaultSpecBlob),
      bucketSpecFetcher.getSpecAsBlob(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
        GcsBucketSpecFetcher.DEFAULT_SPEC_FILE,
        Configs.AirbyteEdition.COMMUNITY,
      ),
    )
    Assertions.assertEquals(
      Optional.of(defaultSpecBlob),
      bucketSpecFetcher.getSpecAsBlob(
        DOCKER_REPOSITORY,
        DOCKER_IMAGE_TAG,
        GcsBucketSpecFetcher.DEFAULT_SPEC_FILE,
        Configs.AirbyteEdition.ENTERPRISE,
      ),
    )
    Assertions.assertEquals(
      Optional.of(cloudSpecBlob),
      bucketSpecFetcher.getSpecAsBlob(DOCKER_REPOSITORY, DOCKER_IMAGE_TAG, GcsBucketSpecFetcher.CLOUD_SPEC_FILE, Configs.AirbyteEdition.CLOUD),
    )
  }

  companion object {
    private const val BUCKET_NAME = "bucket"
    private const val DOCKER_REPOSITORY = "image"
    private const val DOCKER_IMAGE_TAG = "0.1.0"
    private const val DOCKER_IMAGE = "$DOCKER_REPOSITORY:$DOCKER_IMAGE_TAG"
    private val DEFAULT_SPEC_PATH =
      Path
        .of("specs")
        .resolve(DOCKER_REPOSITORY)
        .resolve(DOCKER_IMAGE_TAG)
        .resolve(GcsBucketSpecFetcher.DEFAULT_SPEC_FILE)
        .toString()
    private val CLOUD_SPEC_PATH =
      Path
        .of("specs")
        .resolve(DOCKER_REPOSITORY)
        .resolve(DOCKER_IMAGE_TAG)
        .resolve(GcsBucketSpecFetcher.CLOUD_SPEC_FILE)
        .toString()
  }
}
