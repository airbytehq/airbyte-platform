/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
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
                  {"name": "0.90.0"},
                  {"name": "1.0.0"},
                  {"name": "1.0.1"},
                  {"name": "2.0.0"},
                  {"name": "2.0.1-dev123456"},
                  {"name": "3.0.0"}
                ]
              }
              """,
      )

    val latestMajorVersions = declarativeManifestImageVersionsProvider.getLatestDeclarativeManifestImageVersions()
    val expectedLatestVersions =
      listOf(
        DeclarativeManifestImageVersion(0, "0.90.0"),
        DeclarativeManifestImageVersion(1, "1.0.1"),
        DeclarativeManifestImageVersion(2, "2.0.0"),
        DeclarativeManifestImageVersion(3, "3.0.0"),
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
              {"name": "1.0.1"}
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
              {"name": "1.0.0"}
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
              {"name": "0.90.0"}
            ]
          }
        """,
      )

    val latestMajorVersions = declarativeManifestImageVersionsProvider.getLatestDeclarativeManifestImageVersions()
    val expectedLatestVersions =
      listOf(
        DeclarativeManifestImageVersion(1, "1.0.1"),
        DeclarativeManifestImageVersion(0, "0.90.0"),
      )
    assertEquals(expectedLatestVersions, latestMajorVersions)

    verify(exactly = 3) { okHttpClient.newCall(any()).execute() }
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
