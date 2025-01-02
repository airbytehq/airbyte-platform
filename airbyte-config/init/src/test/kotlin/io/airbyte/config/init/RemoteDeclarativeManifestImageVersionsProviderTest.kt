/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.config.init
import io.airbyte.data.repositories.entities.DeclarativeManifestImageVersion
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.justRun
import io.mockk.mockk
import io.mockk.verify
import okhttp3.OkHttpClient
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.IOException

internal class RemoteDeclarativeManifestImageVersionsProviderTest {
  private val okHttpClient: OkHttpClient = mockk(relaxed = true)
  private lateinit var declarativeManifestImageVersionsProvider: RemoteDeclarativeManifestImageVersionsProvider

  @BeforeEach
  fun setup() {
    declarativeManifestImageVersionsProvider = RemoteDeclarativeManifestImageVersionsProvider(okHttpClient)
  }

  @Test
  fun `test getLatestDeclarativeManifestImageVersions`() {
    every { okHttpClient.newCall(any()).execute() } returns
      successfulResponse(
        """
              {
                "count": 8,
                "next": null,
                "previous": null,
                "results": [
                  {"name": "0.90.0", "digest": "sha256:d4b897be4f4c9edc5073b60f625cadb6853d8dc7e6178b19c414fe9b743fde33"},
                  {"name": "1.0.0", "digest": "sha256:a54aad18cf460173f753fe938e254a667dac97b703fc05cf6de8c839caf62ef4"},
                  {"name": "1.0.1", "digest": "sha256:26f3d6b7dcbfa43504709e42d859c12f8644b7c7bbab0ecac99daa773f7dd35c"},
                  {"name": "2.0.0", "digest": "sha256:26f3d6b7dcbfa43504709e42d859c12f8644b7c7bbab0ecac99daa773f7dd35c"},
                  {"name": "2.0.1-dev123456", "digest": "sha256:26f3d6b7dcbfa43504709e42d859c12f8644b7c7bbab0ecac99daa773f7dd35c"},
                  {"name": "3.0.0", "digest": "sha256:26f3d6b7dcbfa43504709e42d859c12f8644b7c7bbab0ecac99daa773f7dd35c"}
                ]
              }
              """,
      )

    val latestMajorVersions = declarativeManifestImageVersionsProvider.getLatestDeclarativeManifestImageVersions()
    val expectedLatestVersions =
      listOf(
        DeclarativeManifestImageVersion(0, "0.90.0", "sha256:d4b897be4f4c9edc5073b60f625cadb6853d8dc7e6178b19c414fe9b743fde33"),
        DeclarativeManifestImageVersion(1, "1.0.1", "sha256:26f3d6b7dcbfa43504709e42d859c12f8644b7c7bbab0ecac99daa773f7dd35c"),
        DeclarativeManifestImageVersion(2, "2.0.0", "sha256:26f3d6b7dcbfa43504709e42d859c12f8644b7c7bbab0ecac99daa773f7dd35c"),
        DeclarativeManifestImageVersion(3, "3.0.0", "sha256:26f3d6b7dcbfa43504709e42d859c12f8644b7c7bbab0ecac99daa773f7dd35c"),
      )
    assertEquals(expectedLatestVersions, latestMajorVersions)

    verify(exactly = 1) { okHttpClient.newCall(any()).execute() }
    confirmVerified(okHttpClient)
  }

  @Test
  fun `test pagination`() {
    every { okHttpClient.newCall(any()).execute() } returns
      successfulResponse(
        """
          {
            "count": 3,
            "next": "https://hub.docker.com/v2/repositories/airbyte/source-declarative-manifest/tags?page_size=1&page=2",
            "previous": null,
            "results": [
              {"name": "1.0.1", "digest": "sha256:26f3d6b7dcbfa43504709e42d859c12f8644b7c7bbab0ecac99daa773f7dd35c"}
            ]
          }
        """,
      ) andThen
      successfulResponse(
        """
          {
            "count": 3,
            "next": "https://hub.docker.com/v2/repositories/airbyte/source-declarative-manifest/tags?page_size=1&page=3",
            "previous": "https://hub.docker.com/v2/repositories/airbyte/source-declarative-manifest/tags?page_size=1",
            "results": [
              {"name": "1.0.0", "digest": "sha256:a54aad18cf460173f753fe938e254a667dac97b703fc05cf6de8c839caf62ef4"}
            ]
          }
        """,
      ) andThen
      successfulResponse(
        """
          {
            "count": 3,
            "next": null,
            "previous": "https://hub.docker.com/v2/repositories/airbyte/source-declarative-manifest/tags?page_size=1&page=2",
            "results": [
              {"name": "0.90.0", "digest": "sha256:d4b897be4f4c9edc5073b60f625cadb6853d8dc7e6178b19c414fe9b743fde33"}
            ]
          }
        """,
      )

    val latestMajorVersions = declarativeManifestImageVersionsProvider.getLatestDeclarativeManifestImageVersions()
    val expectedLatestVersions =
      listOf(
        DeclarativeManifestImageVersion(1, "1.0.1", "sha256:26f3d6b7dcbfa43504709e42d859c12f8644b7c7bbab0ecac99daa773f7dd35c"),
        DeclarativeManifestImageVersion(0, "0.90.0", "sha256:d4b897be4f4c9edc5073b60f625cadb6853d8dc7e6178b19c414fe9b743fde33"),
      )
    assertEquals(expectedLatestVersions, latestMajorVersions)

    verify(exactly = 3) { okHttpClient.newCall(any()).execute() }
    confirmVerified(okHttpClient)
  }

  @Test
  fun `test versions are compared correctly via semver and not string comparison`() {
    every { okHttpClient.newCall(any()).execute() } returns
      successfulResponse(
        """
              {
                "count": 8,
                "next": null,
                "previous": null,
                "results": [
                  {"name": "3.0.0", "digest": "sha256:26f3d6b7dcbfa43504709e42d859c12f8644b7c7bbab0ecac99daa773f7dd35c"},
                  {"name": "3.9.0", "digest": "sha256:26f3d6b7dcbfa43504709e42d859c12f8644b7c7bbab0ecac99daa773f7dd35c"},
                  {"name": "3.10.0", "digest": "sha256:26f3d6b7dcbfa43504709e42d859c12f8644b7c7bbab0ecac99daa773f7dd35c"}
                ]
              }
              """,
      )

    val latestMajorVersions = declarativeManifestImageVersionsProvider.getLatestDeclarativeManifestImageVersions()
    val expectedLatestVersions =
      listOf(
        DeclarativeManifestImageVersion(3, "3.10.0", "sha256:26f3d6b7dcbfa43504709e42d859c12f8644b7c7bbab0ecac99daa773f7dd35c"),
      )
    assertEquals(expectedLatestVersions, latestMajorVersions)

    verify(exactly = 1) { okHttpClient.newCall(any()).execute() }
    confirmVerified(okHttpClient)
  }

  @Test
  fun `test no tags available`() {
    every { okHttpClient.newCall(any()).execute() } returns
      successfulResponse(
        """
          {
            "count": 0,
            "next": null,
            "previous": null,
            "results": []
          }
        """,
      )

    val latestMajorVersions = declarativeManifestImageVersionsProvider.getLatestDeclarativeManifestImageVersions()
    assertEquals(emptyList<DeclarativeManifestImageVersion>(), latestMajorVersions)

    verify(exactly = 1) { okHttpClient.newCall(any()).execute() }
    confirmVerified(okHttpClient)
  }

  @Test
  fun `test error handling`() {
    every { okHttpClient.newCall(any()).execute() } returns
      mockk {
        every { isSuccessful } returns false
        every { code } returns 500
        every { message } returns "Internal Server Error"
        justRun { close() }
      }

    val exception =
      assertThrows(IOException::class.java) {
        declarativeManifestImageVersionsProvider.getLatestDeclarativeManifestImageVersions()
      }
    assertEquals("Unexpected response from DockerHub API: 500 Internal Server Error", exception.message)

    verify(exactly = 1) { okHttpClient.newCall(any()).execute() }
    confirmVerified(okHttpClient)
  }

  fun successfulResponse(responseBody: String): okhttp3.Response {
    return mockk {
      every { isSuccessful } returns true
      every { body } returns
        mockk {
          every { string() } returns responseBody.trimIndent()
        }
      justRun { close() }
    }
  }
}
