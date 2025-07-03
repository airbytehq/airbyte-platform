/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.specs

import com.google.cloud.storage.Blob
import com.google.cloud.storage.Storage
import io.airbyte.commons.json.Jsons
import io.airbyte.config.Configs
import io.airbyte.protocol.models.v0.ConnectorSpecification
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.util.Map
import java.util.Optional

internal class GcsBucketSpecFetcherTest {
  lateinit var storage: Storage
  lateinit var defaultSpecBlob: Blob
  lateinit var cloudSpecBlob: Blob
  private val defaultSpec: ConnectorSpecification =
    ConnectorSpecification()
      .withConnectionSpecification(Jsons.jsonNode(Map.of("foo", "bar", "mode", "oss")))
  private val cloudSpec: ConnectorSpecification =
    ConnectorSpecification()
      .withConnectionSpecification(Jsons.jsonNode(Map.of("foo", "bar", "mode", "cloud")))

  @BeforeEach
  fun setup() {
    storage = Mockito.mock(Storage::class.java)

    defaultSpecBlob = Mockito.mock(Blob::class.java)
    Mockito.`when`(defaultSpecBlob.getContent()).thenReturn(Jsons.toBytes(Jsons.jsonNode(defaultSpec)))
    cloudSpecBlob = Mockito.mock(Blob::class.java)
    Mockito.`when`(cloudSpecBlob.getContent()).thenReturn(Jsons.toBytes(Jsons.jsonNode(cloudSpec)))
  }

  @Test
  fun testGetsSpecIfPresent() {
    Mockito.`when`(storage[BUCKET_NAME, DEFAULT_SPEC_PATH]).thenReturn(defaultSpecBlob)

    val bucketSpecFetcher = GcsBucketSpecFetcher(storage, BUCKET_NAME)
    val returnedSpec = bucketSpecFetcher.attemptFetch(DOCKER_IMAGE)

    Assertions.assertTrue(returnedSpec.isPresent)
    Assertions.assertEquals(defaultSpec, returnedSpec.get())
  }

  @Test
  fun testReturnsEmptyIfNotPresent() {
    Mockito.`when`(storage[BUCKET_NAME, DEFAULT_SPEC_PATH]).thenReturn(null)

    val bucketSpecFetcher = GcsBucketSpecFetcher(storage, BUCKET_NAME)
    val returnedSpec = bucketSpecFetcher.attemptFetch(DOCKER_IMAGE)

    Assertions.assertTrue(returnedSpec.isEmpty)
  }

  @Test
  fun testReturnsEmptyIfInvalidSpec() {
    val invalidSpecBlob = Mockito.mock(Blob::class.java)
    Mockito.`when`(invalidSpecBlob.getContent()).thenReturn("{\"notASpec\": true}".toByteArray(StandardCharsets.UTF_8))
    Mockito.`when`(storage[BUCKET_NAME, DEFAULT_SPEC_PATH]).thenReturn(invalidSpecBlob)

    val bucketSpecFetcher = GcsBucketSpecFetcher(storage, BUCKET_NAME)
    val returnedSpec = bucketSpecFetcher.attemptFetch(DOCKER_IMAGE)

    Assertions.assertTrue(returnedSpec.isEmpty)
  }

  /**
   * Test [GcsBucketSpecFetcher.getSpecAsBlob].
   */
  @Test
  fun testDynamicGetSpecAsBlob() {
    Mockito.`when`(storage[BUCKET_NAME, DEFAULT_SPEC_PATH]).thenReturn(defaultSpecBlob)
    Mockito.`when`(storage[BUCKET_NAME, CLOUD_SPEC_PATH]).thenReturn(cloudSpecBlob)

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
    Mockito.`when`(storage[BUCKET_NAME, DEFAULT_SPEC_PATH]).thenReturn(defaultSpecBlob)
    Mockito.`when`(storage[BUCKET_NAME, CLOUD_SPEC_PATH]).thenReturn(cloudSpecBlob)

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
    private const val DOCKER_IMAGE = DOCKER_REPOSITORY + ":" + DOCKER_IMAGE_TAG
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
