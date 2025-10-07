/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.acceptance

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.airbyte.api.client.model.generated.ConfiguredStreamMapper
import io.airbyte.api.client.model.generated.ConnectionStatus
import io.airbyte.api.client.model.generated.JobStatus
import io.airbyte.api.client.model.generated.SelectedFieldInfo
import io.airbyte.api.client.model.generated.StreamMapperType
import io.airbyte.test.AtcConfig
import io.airbyte.test.AtcData
import io.airbyte.test.AtcDataMovies
import io.airbyte.test.AtcDataProperty
import io.airbyte.test.Movie
import io.github.oshai.kotlinlogging.KotlinLogging
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertNotNull
import kotlin.time.Duration.Companion.minutes

private val log = KotlinLogging.logger {}

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tag("sync")
internal class SyncAcceptanceTest {
  private val atClient = AcceptanceTestClient()

  @BeforeAll
  fun setup() {
    atClient.setup()
  }

  @AfterAll
  fun tearDownAll() {
    atClient.tearDownAll()
  }

  @AfterEach
  fun tearDown() {
    atClient.tearDown()
  }

  @Test
  fun `sync passes correctly`() {
    val connectionId = atClient.admin.createAtcConnection()

    val jobId = atClient.admin.syncConnection(connectionId)

    val status = atClient.admin.jobWatchUntilTerminal(jobId, duration = 5.minutes)
    if (status != JobStatus.SUCCEEDED) {
      atClient.admin.jobLogs(jobId, log)
    }

    assertEquals(JobStatus.SUCCEEDED, status)
  }

  @Test
  fun `sync fails correctly`() {
    val dstConfig =
      AtcConfig(
        data =
          object : AtcData by AtcDataMovies {
            // is only going to expect the first record
            override fun records(): List<Any> = listOf(AtcDataMovies.records().first())
          },
      )

    val connectionId = atClient.admin.createAtcConnection(dstCfg = dstConfig)
    val jobId = atClient.admin.syncConnection(connectionId)

    atClient.admin.jobCancel(jobId)
    val status = atClient.admin.jobWatchUntilTerminal(jobId)
    assertNotEquals(JobStatus.SUCCEEDED, status)
  }

  @Test
  fun `sync can be cancelled`() {
    val connectionId = atClient.admin.createAtcConnection()
    val jobId = atClient.admin.syncConnection(connectionId)

    atClient.admin.jobCancel(jobId)
    val status = atClient.admin.jobWatchUntilTerminal(jobId)
    if (status != JobStatus.CANCELLED) {
      atClient.admin.jobLogs(jobId, log)
    }

    assertEquals(JobStatus.CANCELLED, status)
  }

  @Test
  fun `column selection`() {
    data class Film(
      val film: String,
    )

    val atcCfg =
      AtcConfig(
        data =
          object : AtcData {
            override fun cursor() = listOf("film")

            override fun required() = listOf("film")

            override fun properties() = mapOf("film" to AtcDataProperty("string"))

            override fun records(): List<Any> =
              AtcDataMovies
                .records()
                .filterIsInstance<Movie>()
                .map { Film(it.film) }
                .toList()
          },
      )
    // By default, the source config uses the AtcDataMovies data. We are telling the source to only sync specific fields by explicitly setting the
    // `fieldSelectionEnabled` to `true` and selecting just the `film` field.  Finally, we configure the destination config
    // to expect only the film in the resulting data.
    val connectionId =
      atClient.admin.createAtcConnection(dstCfg = atcCfg) { catalog ->
        catalog.copy(
          streams =
            catalog.streams.mapIndexed { index, streamAndConfig ->
              if (index == 0) {
                streamAndConfig.copy(
                  config =
                    streamAndConfig.config?.copy(
                      fieldSelectionEnabled = true,
                      selectedFields = listOf(SelectedFieldInfo(fieldPath = listOf("film"))),
                    ),
                )
              } else {
                streamAndConfig
              }
            },
        )
      }
    assertNotNull(connectionId)

    val jobId = atClient.admin.syncConnection(connectionId)
    assertNotNull(jobId)

    val status = atClient.admin.jobWatchUntilTerminal(jobId, duration = 5.minutes)
    if (status != JobStatus.SUCCEEDED) {
      atClient.admin.jobLogs(jobId, log)
    }

    assertEquals(JobStatus.SUCCEEDED, status)
  }

  @OptIn(ExperimentalStdlibApi::class)
  @Test
  fun `verify mapper`() {
    val mapper = jacksonObjectMapper()
    val mapperFilterRecords =
      """
      {
        "conditions": {
          "fieldName": "year",
          "type": "EQUAL",
          "comparisonValue": 2020
        }
      }
      """.trimIndent()
    val mapperRenameField =
      """
      {
        "originalFieldName": "director",
        "newFieldName": "directed_by"
      }
      """.trimIndent()
    val mapperHashField =
      """
      {
        "method": "SHA-256",
        "targetField": "directed_by",
        "fieldNameSuffix": "_hash"
      }
      """.trimIndent()

    // TODO(cole): figure out the best way to test encryption
    //    val publicKey =
    //      Resources
    //        .readResourceAsFile("keys/public")
    //        .readBytes()
    //        .let { X509EncodedKeySpec(it) }
    //        .let { KeyFactory.getInstance("RSA").generatePublic(it) }
    //
    //    val mapperEncryptField =
    //      """
    //      {
    //        "algorithm": "RSA",
    //        "fieldNameSuffix": "_rsa",
    //        "targetField": "directed_by_hash",
    //        "publicKey": "${publicKey.encoded.toHexString()}"
    //      }
    //      """.trimIndent()

    val dstCfgNew =
      AtcConfig(
        data =
          object : AtcData by AtcDataMovies {
            // replace `director with directed_by
            override fun required() = AtcDataMovies.required().toMutableList().map { if (it == "director") "directed_by" else it }

            // remove director, add directed_by and directed_by_hash
            override fun properties() =
              AtcDataMovies.properties().toMutableMap().let {
                it.remove("director")
                it["directed_by_"] = AtcDataProperty(type = "string")
                it["directed_by_hash"] = AtcDataProperty(type = "string")
                it
              }

            override fun records(): List<Any> {
              val first = AtcDataMovies.records().first() as Movie
              return listOf(
                mapOf(
                  "year" to first.year,
                  "film" to first.film,
                  "publisher" to first.publisher,
                  "directed_by_hash" to "260638b42c6e881e9a970bf843fce23856092d92af1133f5fdf63e72c41ac9b0",
                  "distributor" to first.distributor,
                  "worldwide_gross" to first.worldwideGross,
                ),
              )
            }
          },
      )

    val connectionId =
      atClient.admin.createAtcConnection(dstCfg = dstCfgNew) { catalog ->
        catalog.copy(
          streams =
            catalog.streams.mapIndexed { index, streamAndConfig ->
              if (index == 0) {
                streamAndConfig.copy(
                  config =
                    streamAndConfig.config?.copy(
                      mappers =
                        listOf(
                          // remove all records except for those where the film field is "Sonic the Hedgehog"
                          ConfiguredStreamMapper(StreamMapperType.ROW_MINUS_FILTERING, mapper.readTree(mapperFilterRecords)),
                          // rename the "film" field to "movie_title"
                          ConfiguredStreamMapper(StreamMapperType.FIELD_MINUS_RENAMING, mapper.readTree(mapperRenameField)),
                          // hash the "movie_title" field and add a suffix of "_hash" to the field name
                          ConfiguredStreamMapper(StreamMapperType.HASHING, mapper.readTree(mapperHashField)),
                          // encrypt the "movie_title_hash" field and add a suffix of "_rsa" to the field name
                          // ConfiguredStreamMapper(StreamMapperType.ENCRYPTION, mapper.readTree(mapperEncryptField)),
                        ),
                    ),
                )
              } else {
                streamAndConfig
              }
            },
        )
      }

    val jobId = atClient.admin.syncConnection(connectionId)
    assertNotNull(jobId)

    val status = atClient.admin.jobWatchUntilTerminal(jobId, duration = 5.minutes)
    if (status != JobStatus.SUCCEEDED) {
      atClient.admin.jobLogs(jobId, log)
    }

    assertEquals(JobStatus.SUCCEEDED, status)
  }

  @Test
  fun `delete running connection`() {
    val connectionId = atClient.admin.createAtcConnection()
    assertEquals(ConnectionStatus.ACTIVE, atClient.admin.getConnectionStatus(connectionId))

    atClient.admin.syncConnection(connectionId)

    atClient.admin.deleteConnection(connectionId)
    assertEquals(ConnectionStatus.DEPRECATED, atClient.admin.getConnectionStatus(connectionId))

    // verify calling delete twice is ok
    atClient.admin.deleteConnection(connectionId)
    assertEquals(ConnectionStatus.DEPRECATED, atClient.admin.getConnectionStatus(connectionId))
  }

  @Test
  fun `delete running connection in a bad temporal state`() {
    // TODO
  }
}
