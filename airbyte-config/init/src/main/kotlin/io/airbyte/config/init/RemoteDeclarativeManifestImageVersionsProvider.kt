package io.airbyte.config.init

import io.airbyte.commons.json.Jsons
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

  override fun getLatestDeclarativeManifestImageVersions(): Map<Int, String> {
    val repository = "airbyte/source-declarative-manifest"
    val tags = getTagsForRepository(repository)

    val semverStandardVersionTags = tags.filter { it.matches(Regex("""^\d+\.\d+\.\d+$""")) }.toList()
    val latestVersionsByMajor =
      semverStandardVersionTags
        .groupBy { it.split(".")[0].toInt() }
        .mapValues { (_, versionsByMajor) -> versionsByMajor.maxBy { it } }

    log.info("Latest versions for $repository: $latestVersionsByMajor")
    return latestVersionsByMajor
  }

  private fun getTagsForRepository(repository: String): List<String> {
    val tags = mutableListOf<String>()

    // 100 is max allowed page size for DockerHub
    var nextUrl: String? = "https://hub.docker.com/v2/repositories/$repository/tags?page_size=100"

    log.info("Fetching image tags for $repository...")
    while (nextUrl != null) {
      val request = Request.Builder().url(nextUrl).build()
      okHttpClient.newCall(request).execute().use { response ->
        if (!response.isSuccessful || response.body == null) {
          throw IOException(
            "Unexpected response from DockerHub API: ${response.code} ${response.message}",
          )
        }
        val body = Jsons.deserialize(response.body!!.string())
        tags.addAll(body.get("results").elements().asSequence().mapNotNull { it.get("name").asText() })
        nextUrl = if (!body.get("next").isNull) body.get("next").asText() else null
      }
    }
    log.info("DockerHub tags for $repository: $tags")
    return tags
  }
}
