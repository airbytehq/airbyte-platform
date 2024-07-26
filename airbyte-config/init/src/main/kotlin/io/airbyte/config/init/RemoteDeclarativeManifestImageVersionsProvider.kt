package io.airbyte.config.init

import io.airbyte.commons.json.Jsons
import io.airbyte.data.repositories.entities.DeclarativeManifestImageVersion
import jakarta.inject.Named
import jakarta.inject.Singleton
import okhttp3.OkHttpClient
import okhttp3.Request
import org.slf4j.LoggerFactory
import java.io.IOException

@Singleton
@Named("remoteDeclarativeManifestImageVersionsProvider")
class RemoteDeclarativeManifestImageVersionsProvider(
  @Named("dockerHubOkHttpClient") val okHttpClient: OkHttpClient,
) : DeclarativeManifestImageVersionsProvider {
  companion object {
    private val log = LoggerFactory.getLogger(RemoteDeclarativeManifestImageVersionsProvider::class.java)
  }

  override fun getLatestDeclarativeManifestImageVersions(): List<DeclarativeManifestImageVersion> {
    val repository = "airbyte/source-declarative-manifest"
    val items = getTagsAndShasForRepository(repository)

    val semverStandardVersionTags = items.filter { (imageVersion, _) -> imageVersion.matches(Regex("""^\d+\.\d+\.\d+$""")) }
    val semverStandardDeclarativeManifestImageVersions =
      semverStandardVersionTags.map { (imageVersion, imageSha) ->
        DeclarativeManifestImageVersion(getMajorVersion(imageVersion), imageVersion, imageSha)
      }

    val latestVersionsByMajor =
      semverStandardDeclarativeManifestImageVersions
        .groupBy { it.majorVersion }
        .map { entry ->
          entry.value.maxBy { version -> version.imageVersion }
        }
    log.info("Latest versions for $repository: ${latestVersionsByMajor.map { it.imageVersion }}")
    return latestVersionsByMajor
  }

  private fun getTagsAndShasForRepository(
    @Suppress("SameParameterValue") repository: String,
  ): Map<String, String> {
    val tagsAndShas = mutableMapOf<String, String>()

    // 100 is max allowed page size for DockerHub
    var nextUrl: String? = "https://hub.docker.com/v2/repositories/$repository/tags?page_size=100"

    log.info("Fetching image tags and SHAs for $repository...")
    while (nextUrl != null) {
      val request = Request.Builder().url(nextUrl).build()
      okHttpClient.newCall(request).execute().use { response ->
        if (!response.isSuccessful || response.body == null) {
          throw IOException(
            "Unexpected response from DockerHub API: ${response.code} ${response.message}",
          )
        }
        val body = Jsons.deserialize(response.body!!.string())
        body.get("results").elements().forEach { result ->
          val tag = result.get("name").asText()
          val sha = result.get("digest").asText()
          tagsAndShas[tag] = sha
        }
        nextUrl = if (!body.get("next").isNull) body.get("next").asText() else null
      }
    }
    log.info("DockerHub tags and SHAs for $repository: $tagsAndShas")
    return tagsAndShas
  }

  private fun getMajorVersion(version: String): Int {
    return version.split(".")[0].toInt()
  }
}
