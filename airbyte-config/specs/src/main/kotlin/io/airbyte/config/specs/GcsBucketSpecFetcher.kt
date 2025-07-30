/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.specs

import com.fasterxml.jackson.databind.JsonNode
import com.google.api.client.util.Preconditions
import com.google.cloud.storage.Blob
import com.google.cloud.storage.Storage
import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.json.Jsons
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.protocol.models.AirbyteProtocolSchema
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.validation.json.JsonSchemaValidator
import io.airbyte.validation.json.JsonValidationException
import io.github.oshai.kotlinlogging.KotlinLogging
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.util.Optional

/**
 * Fetch connector specs from Airbyte GCS Bucket where specs are stored when connectors are
 * published.
 */
class GcsBucketSpecFetcher {
  private val storage: Storage

  /**
   * Get name of bucket where specs are stored.
   *
   * @return name of bucket where specs are stored
   */
  val bucketName: String
  private val airbyteEdition: AirbyteEdition

  constructor(storage: Storage, bucketName: String) {
    this.storage = storage
    this.bucketName = bucketName
    this.airbyteEdition = AirbyteEdition.COMMUNITY
  }

  /**
   * This constructor is used by airbyte-cloud to fetch cloud-specific spec files.
   */
  constructor(storage: Storage, bucketName: String, airbyteEdition: AirbyteEdition) {
    this.storage = storage
    this.bucketName = bucketName
    this.airbyteEdition = airbyteEdition
  }

  /**
   * Fetch spec for docker image.
   *
   * @param dockerImage of a connector
   * @return connector spec of docker image. if not found, empty optional.
   */
  fun attemptFetch(dockerImage: String): Optional<ConnectorSpecification> {
    val dockerImageComponents = dockerImage.split(":".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
    Preconditions.checkArgument(dockerImageComponents.size == 2, "Invalidate docker image: $dockerImage")
    val dockerImageName = dockerImageComponents[0]
    val dockerImageTag = dockerImageComponents[1]

    val specAsBlob = getSpecAsBlob(dockerImageName, dockerImageTag)

    if (specAsBlob.isEmpty) {
      log.debug { "Spec not found in bucket storage" }
      return Optional.empty()
    }

    val specAsString = String(specAsBlob.get().getContent(), StandardCharsets.UTF_8)
    try {
      validateConfig(Jsons.deserialize(specAsString))
    } catch (e: JsonValidationException) {
      log.error(e) { "Received invalid spec from bucket store." }
      return Optional.empty()
    }
    return Optional.of(
      Jsons.deserialize(
        specAsString,
        ConnectorSpecification::class.java,
      ),
    )
  }

  @VisibleForTesting
  fun getSpecAsBlob(
    dockerImageName: String,
    dockerImageTag: String,
  ): Optional<Blob> {
    if (airbyteEdition == AirbyteEdition.CLOUD) {
      val cloudSpecAsBlob = getSpecAsBlob(dockerImageName, dockerImageTag, CLOUD_SPEC_FILE, AirbyteEdition.CLOUD)
      if (cloudSpecAsBlob.isPresent) {
        log.info { "Found cloud specific spec: {} $bucketName, cloudSpecAsBlob" }
        return cloudSpecAsBlob
      }
    }
    return getSpecAsBlob(dockerImageName, dockerImageTag, DEFAULT_SPEC_FILE, AirbyteEdition.COMMUNITY)
  }

  @VisibleForTesting
  fun getSpecAsBlob(
    dockerImageName: String,
    dockerImageTag: String,
    specFile: String,
    airbyteEdition: AirbyteEdition,
  ): Optional<Blob> {
    val specPath =
      Path
        .of("specs")
        .resolve(dockerImageName)
        .resolve(dockerImageTag)
        .resolve(specFile)
    log.debug { "Checking path for cached {} spec: {} $airbyteEdition.name, bucketName, specPath" }
    val specAsBlob = storage[bucketName, specPath.toString()]
    if (specAsBlob != null) {
      return Optional.of(specAsBlob)
    }
    return Optional.empty()
  }

  companion object {
    private val log = KotlinLogging.logger {}

    // these filenames must match default_spec_file and cloud_spec_file in manage.sh
    const val DEFAULT_SPEC_FILE: String = "spec.json"
    const val CLOUD_SPEC_FILE: String = "spec.cloud.json"

    @Throws(JsonValidationException::class)
    private fun validateConfig(json: JsonNode) {
      val jsonSchemaValidator = JsonSchemaValidator()
      val specJsonSchema = JsonSchemaValidator.getSchema(AirbyteProtocolSchema.PROTOCOL.file, "ConnectorSpecification")
      jsonSchemaValidator.ensure(specJsonSchema, json)
    }
  }
}
