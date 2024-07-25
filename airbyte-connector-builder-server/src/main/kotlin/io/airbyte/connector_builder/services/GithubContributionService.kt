@file:Suppress("ktlint:standard:package-name")

package io.airbyte.connector_builder.services

import org.kohsuke.github.GHContent
import org.kohsuke.github.GHFileNotFoundException
import org.kohsuke.github.GHRepository
import org.kohsuke.github.GitHub
import org.kohsuke.github.GitHubBuilder
import org.yaml.snakeyaml.Yaml

class GithubContributionService(var connectorId: String) {
  var githubService: GitHub? = null
  val repositoryName = "connector-archive" // TODO - change to airbyte before release
  val repoOwner = "airbytehq"
  val connectorDirectory = "airbyte-integrations/connectors"

  init {
    githubService = GitHubBuilder().build()
  }

  // PROPERTIES

  val repository: GHRepository
    get() = githubService!!.getRepository("$repoOwner/$repositoryName")

  val connectorDirectoryPath: String
    get() = "$connectorDirectory/$connectorId"

  val connectorMetadataPath: String
    get() = "$connectorDirectoryPath/metadata.yaml"

  // PRIVATE METHODS

  fun safeReadFileContent(path: String): GHContent? {
    return try {
      repository.getFileContent(path)
    } catch (e: GHFileNotFoundException) {
      null
    }
  }

  fun checkFileExistsOnMain(path: String): Boolean {
    val fileInfo = safeReadFileContent(path)
    return fileInfo != null
  }

  // PUBLIC METHODS

  fun checkConnectorExistsOnMain(): Boolean {
    return checkFileExistsOnMain(connectorMetadataPath)
  }

  fun readConnectorMetadataName(): String? {
    val metadataFile = safeReadFileContent(connectorMetadataPath) ?: return null

    val rawYamlString = metadataFile.content

    // parse yaml
    val yaml = Yaml()
    val parsedYaml = yaml.load<Map<String, Any>>(rawYamlString)

    // get name from the path "data.name"
    return parsedYaml["data"]?.let { (it as Map<*, *>)["name"] } as String? ?: ""
  }
}
