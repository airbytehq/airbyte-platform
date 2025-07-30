/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.logging.MdcScope
import io.airbyte.commons.protocol.AirbyteMessageSerDeProvider
import io.airbyte.commons.protocol.AirbyteMessageVersionedMigrator
import io.airbyte.commons.protocol.AirbyteProtocolVersionedMigratorFactory
import io.airbyte.commons.protocol.serde.AirbyteMessageDeserializer
import io.airbyte.commons.protocol.serde.AirbyteMessageV0Deserializer
import io.airbyte.commons.protocol.serde.AirbyteMessageV0Serializer
import io.airbyte.commons.version.Version
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.protocol.models.v0.AirbyteLogMessage
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.workers.helper.GsonPksExtractor
import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.core.util.StringUtils
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.slf4j.MDCContext
import kotlinx.coroutines.withContext
import org.slf4j.MDC
import java.io.BufferedReader
import java.io.IOException
import java.util.Locale
import java.util.Optional
import java.util.UUID
import java.util.function.Consumer
import java.util.stream.Stream

const val CONNECTION_ID_NOT_PRESENT: String = "not present"

fun malformedNonAirbyteRecordLogMessage(
  connectionId: String,
  line: String,
): String = "Malformed non-Airbyte record (connectionId = $connectionId): $line"

fun malformedAirbyteRecordLogMessage(
  connectionId: String,
  line: String,
): String = "Malformed Airbyte record (connectionId = $connectionId): $line"

const val MESSAGES_LOOK_AHEAD_FOR_DETECTION: Int = 10

private const val TYPE_FIELD_NAME: String = "type"
private const val MAXIMUM_CHARACTERS_ALLOWED: Int = 20000000

internal val DEFAULT_MDC_SCOPE: MdcScope.Builder = MdcScope.DEFAULT_BUILDER
val fallbackVersion: Version = Version("0.2.0")

// Buffer size to use when detecting the protocol version.
// Given that BufferedReader::reset fails if we try to reset if we go past its buffer size, this
// buffer has to be big enough to contain our longest spec and whatever messages get emitted before
// the SPEC.
private const val BUFFER_READ_AHEAD_LIMIT: Int = 2 * 1024 * 1024 // 2 megabytes

/**
 * Creates a stream from an input stream. The produced stream attempts to parse each line of the
 * InputStream into a [AirbyteMessage]. If the line cannot be parsed into a [AirbyteMessage] it is
 * dropped. Each record MUST be new line separated.
 *
 * If a line starts with a [AirbyteMessage] and then has other characters after it, that
 * [AirbyteMessage] will still be parsed. If there are multiple [AirbyteMessage] records on the same
 * line, only the first will be parsed.
 *
 * Handles parsing and validation from a specific version of the Airbyte Protocol as well as
 * upgrading messages to the current version.
 */
class VersionedAirbyteStreamFactory<T>(
  private val serDeProvider: AirbyteMessageSerDeProvider =
    AirbyteMessageSerDeProvider(listOf(AirbyteMessageV0Deserializer()), listOf(AirbyteMessageV0Serializer())),
  private var migratorFactory: AirbyteProtocolVersionedMigratorFactory,
  private var protocolVersion: Version,
  private val connectionId: Optional<UUID> = Optional.empty(),
  private val configuredAirbyteCatalog: Optional<ConfiguredAirbyteCatalog> = Optional.empty(),
  private val containerLogMdcBuilder: MdcScope.Builder = DEFAULT_MDC_SCOPE,
  private val invalidLineFailureConfiguration: InvalidLineFailureConfiguration,
  private val gsonPksExtractor: GsonPksExtractor,
  private val metricClient: MetricClient,
  private val logger: KLogger = KotlinLogging.logger { },
) : AirbyteStreamFactory {
  @JvmRecord
  data class InvalidLineFailureConfiguration(
    val printLongRecordPks: Boolean,
  )

  private lateinit var deserializer: AirbyteMessageDeserializer<AirbyteMessage>
  private var shouldDetectVersion = false
  private lateinit var migrator: AirbyteMessageVersionedMigrator<AirbyteMessage>

  init {
    initializeForProtocolVersion(protocolVersion)
  }

  /**
   * Create the [AirbyteMessage] stream.
   *
   * If detectVersion is set to true, it will decide which protocol version to use from the content of
   * the stream rather than the one passed from the constructor.
   */
  override fun create(
    bufferedReader: BufferedReader,
    origin: MessageOrigin,
  ): Stream<AirbyteMessage> {
    detectAndInitialiseMigrators(bufferedReader)
    val needMigration = protocolVersion.getMajorVersion() != migratorFactory.mostRecentVersion.getMajorVersion()
    val protocolMessage =
      if (needMigration) {
        ", messages will be upgraded to protocol version ${migratorFactory.mostRecentVersion.serialize()}"
      } else {
        ""
      }
    logger.info { "Reading messages from protocol version ${protocolVersion.serialize()}$protocolMessage" }
    return addLineReadLogic(bufferedReader, origin)
  }

  private fun detectAndInitialiseMigrators(bufferedReader: BufferedReader) {
    if (shouldDetectVersion) {
      val versionMaybe: Version?
      try {
        versionMaybe = detectVersion(bufferedReader)
      } catch (e: IOException) {
        throw RuntimeException(e)
      }
      versionMaybe?.let {
        logger.info { "Detected Protocol Version ${versionMaybe.serialize()}" }
        initializeForProtocolVersion(versionMaybe)
      } ?: {
        // No version found, use the default as a fallback
        logger.info { "Unable to detect Protocol Version, assuming protocol version ${fallbackVersion.serialize()}" }
        initializeForProtocolVersion(fallbackVersion)
      }
    }
  }

  private fun addLineReadLogic(
    bufferedReader: BufferedReader,
    origin: MessageOrigin,
  ): Stream<AirbyteMessage> =
    bufferedReader
      .lines()
      .flatMap<AirbyteMessage> { line: String -> this.toAirbyteMessage(line, origin) }
      .filter { message: AirbyteMessage -> runBlocking { filterLog(message) } }

  /**
   * Attempt to detect the version by scanning the stream
   *
   * Using the BufferedReader reset/mark feature to get a look-ahead. We will attempt to find the
   * first SPEC message and decide on a protocol version from this message.
   *
   * @param bufferedReader the stream to read
   * @return The Version if found
   * @throws IOException exception while writing
   */
  @Throws(IOException::class)
  private fun detectVersion(bufferedReader: BufferedReader): Version? {
    // Buffer size needs to be big enough to containing everything we need for the detection. Otherwise,
    // the reset will fail.
    bufferedReader.mark(BUFFER_READ_AHEAD_LIMIT)
    try {
      // Cap detection to the first 10 messages. When doing the protocol detection, we expect the SPEC
      // message to show up early in the stream. Ideally, it should be the first message however, we do not
      // enforce this constraint currently, so connectors may send LOG messages before.
      for (@Suppress("UNUSED_PARAMETER")i in 0..<MESSAGES_LOOK_AHEAD_FOR_DETECTION) {
        val line = bufferedReader.readLine()
        val jsonOpt = Jsons.tryDeserialize(line)
        if (jsonOpt.isPresent) {
          val json = jsonOpt.get()
          if (isSpecMessage(json)) {
            val protocolVersionNode = json.at("/spec/protocol_version")
            bufferedReader.reset()
            return if (!protocolVersionNode.isMissingNode) {
              Version(protocolVersionNode.asText())
            } else {
              null
            }
          }
        }
      }
      bufferedReader.reset()
      return null
    } catch (e: IOException) {
      logger.warn {
        "Protocol version detection failed, it is likely than the connector sent more than ${BUFFER_READ_AHEAD_LIMIT}B without an complete SPEC message.  A SPEC message that is too long could be the root cause here."
      }
      throw e
    }
  }

  private fun isSpecMessage(json: JsonNode): Boolean =
    json.has(TYPE_FIELD_NAME) && "spec".equals(json.get(TYPE_FIELD_NAME).asText(), ignoreCase = true)

  fun setDetectVersion(detectVersion: Boolean): Boolean = detectVersion.also { this.shouldDetectVersion = it }

  fun withDetectVersion(detectVersion: Boolean): VersionedAirbyteStreamFactory<T> {
    setDetectVersion(detectVersion)
    return this
  }

  @Suppress("UNCHECKED_CAST")
  internal fun initializeForProtocolVersion(protocolVersion: Version) {
    this.deserializer = serDeProvider.getDeserializer(protocolVersion) as AirbyteMessageDeserializer<AirbyteMessage>
    this.migrator = migratorFactory.getAirbyteMessageMigrator(protocolVersion)
    this.protocolVersion = protocolVersion
  }

  private suspend fun filterLog(message: AirbyteMessage): Boolean {
    val isLog = message.type == AirbyteMessage.Type.LOG
    if (isLog) {
      containerLogMdcBuilder.build().use { ignored ->
        withContext(MDCContext(MDC.getCopyOfContextMap())) {
          internalLog(message.log)
        }
      }
    }
    return !isLog
  }

  internal fun internalLog(logMessage: AirbyteLogMessage) {
    val combinedMessage =
      logMessage.message + (
        if (logMessage.stackTrace != null) {
          (
            (
              System.lineSeparator() +
                "Stack Trace: " + logMessage.stackTrace
            )
          )
        } else {
          ""
        }
      )

    when (logMessage.level) {
      AirbyteLogMessage.Level.FATAL, AirbyteLogMessage.Level.ERROR -> logger.error { combinedMessage }
      AirbyteLogMessage.Level.WARN -> logger.warn { combinedMessage }
      AirbyteLogMessage.Level.DEBUG -> logger.debug { combinedMessage }
      AirbyteLogMessage.Level.TRACE -> logger.trace { combinedMessage }
      else -> logger.info { combinedMessage }
    }
  }

  /**
   * For every incoming message,
   *
   *
   * 1. deserialize the incoming JSON string to [AirbyteMessage].
   *
   *
   * 2. validate the message.
   *
   *
   * 3. upgrade the message to the platform version, if needed.
   */
  internal fun toAirbyteMessage(
    line: String,
    origin: MessageOrigin,
  ): Stream<AirbyteMessage?> {
    logLargeRecordWarning(line)

    var m: Optional<AirbyteMessage> = deserializer.deserializeExact(line)
    if (m.isPresent) {
      m = BasicAirbyteMessageValidator.validate(m.get(), configuredAirbyteCatalog, origin)

      if (m.isEmpty) {
        logger.debug { "Validation failed: ${Jsons.serialize(line)}" }
        return m.stream()
      }

      return upgradeMessage(m.get())
    }

    logMalformedLogMessage(line)
    return m.stream()
  }

  private fun logLargeRecordWarning(line: String) {
    if (line.length < MAXIMUM_CHARACTERS_ALLOWED) {
      return
    }
    try {
      containerLogMdcBuilder.build().use { ignored ->
        connectionId.ifPresentOrElse(
          Consumer { c: UUID? ->
            metricClient.count(
              metric = OssMetricsRegistry.LINE_SKIPPED_TOO_LONG,
              attributes = arrayOf(MetricAttribute(MetricTags.CONNECTION_ID, c.toString())),
            )
          },
        ) { metricClient.count(OssMetricsRegistry.LINE_SKIPPED_TOO_LONG) }
        metricClient.distribution(OssMetricsRegistry.TOO_LONG_LINES_DISTRIBUTION, line.length.toDouble())
        if (invalidLineFailureConfiguration.printLongRecordPks) {
          logger.warn { "[LARGE RECORD] Risk of Destinations not being able to properly handle: ${line.length}" }
          configuredAirbyteCatalog.ifPresent(
            Consumer { airbyteCatalog: ConfiguredAirbyteCatalog? ->
              logger
                .warn { "[LARGE RECORD] The primary keys of the long record are: ${gsonPksExtractor.extractPks(airbyteCatalog!!, line)}" }
            },
          )
        }
      }
    } catch (e: Exception) {
      throw e
    }
  }

  /**
   * If a line cannot be deserialized into an AirbyteMessage, either:
   *
   *
   * 1) We ran into serialization errors, e.g., too big, garbled etc. The most common error being too
   * big.
   *
   *
   * 2) It is a log message that should be an Airbyte Log Message. Currently, the protocol allows for
   * connectors to log to standard out. This is not ideal as it makes it difficult to distinguish
   * between proper and garbled messages. However, since all Java connectors (both source and
   * destination) currently do this, we cannot change this behavior today, though in the long term we
   * want to amend the Protocol and strictly enforce this.
   *
   *
   *
   */
  private fun logMalformedLogMessage(line: String) {
    try {
      containerLogMdcBuilder.build().use { ignored ->
        if (line.lowercase(Locale.getDefault()).replace("\\s".toRegex(), "").contains("{\"type\":\"record\",\"record\":")) {
          // Connectors can sometimes log error messages from failing to parse an AirbyteRecordMessage.
          // Filter on record into debug to try and prevent such cases. Though this catches non-record
          // messages, this is ok as we rather be safe than, sorry.
          logger.warn { "Could not parse the string received from source, it seems to be a record message" }
          metricClient.count(
            metric = OssMetricsRegistry.LINE_SKIPPED_WITH_RECORD,
            attributes = malformedLogAttributes(line, connectionId),
          )
          logger.debug { malformedAirbyteRecordLogMessage(getConnectionId(), line) }
        } else {
          metricClient.count(
            metric = OssMetricsRegistry.NON_AIRBYTE_MESSAGE_LOG_LINE,
            attributes = malformedLogAttributes(line, connectionId),
          )
          logger.info { malformedNonAirbyteRecordLogMessage(getConnectionId(), line) }
        }
      }
    } catch (e: Exception) {
      throw e
    }
  }

  internal fun upgradeMessage(msg: AirbyteMessage): Stream<AirbyteMessage?> {
    try {
      val message: AirbyteMessage = migrator.upgrade(msg, configuredAirbyteCatalog)
      return Stream.of(message)
    } catch (e: RuntimeException) {
      logger.warn(e) { "Failed to upgrade a message from version $protocolVersion: ${Jsons.serialize<AirbyteMessage?>(msg)}" }
      return Stream.empty()
    }
  }

  private fun malformedLogAttributes(
    line: String?,
    connectionId: Optional<UUID>,
  ): Array<MetricAttribute?> {
    val attributes: MutableList<MetricAttribute?> = mutableListOf()
    attributes.add(MetricAttribute(MetricTags.MALFORMED_LOG_LINE_LENGTH, (if (StringUtils.isNotEmpty(line)) line!!.length else 0).toString()))
    connectionId.ifPresent(Consumer { c: UUID? -> attributes.add(MetricAttribute(MetricTags.CONNECTION_ID, c.toString())) })
    return attributes.toTypedArray<MetricAttribute?>()
  }

  private fun getConnectionId(): String = if (connectionId.isPresent) connectionId.get().toString() else CONNECTION_ID_NOT_PRESENT
}
