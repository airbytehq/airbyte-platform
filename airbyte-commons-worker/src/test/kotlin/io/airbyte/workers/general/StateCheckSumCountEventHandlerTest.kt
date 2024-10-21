package io.airbyte.workers.general

import com.google.common.hash.Hashing
import io.airbyte.analytics.Deployment
import io.airbyte.analytics.DeploymentFetcher
import io.airbyte.analytics.TrackingIdentity
import io.airbyte.analytics.TrackingIdentityFetcher
import io.airbyte.api.client.model.generated.DeploymentMetadataRead
import io.airbyte.commons.json.Jsons
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.protocol.models.AirbyteStateMessage
import io.airbyte.protocol.models.AirbyteStateStats
import io.airbyte.protocol.models.AirbyteStreamState
import io.airbyte.protocol.models.StreamDescriptor
import io.airbyte.workers.exception.InvalidChecksumException
import io.airbyte.workers.general.StateCheckSumCountEventHandler.Companion.DUMMY_STATE_MESSAGE
import io.airbyte.workers.internal.bookkeeping.AirbyteMessageOrigin
import io.airbyte.workers.internal.bookkeeping.getNameNamespacePair
import io.airbyte.workers.internal.bookkeeping.getStateHashCode
import io.airbyte.workers.internal.bookkeeping.getStateIdForStatsTracking
import io.airbyte.workers.models.StateCheckSumCountEvent
import io.airbyte.workers.models.StateWithId
import io.mockk.Runs
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Instant
import java.util.Optional
import java.util.UUID
import java.util.function.Supplier

class StateCheckSumCountEventHandlerTest {
  private val pubSubWriter = mockk<StateCheckSumEventPubSubWriter>(relaxed = true)
  private val featureFlagClient = mockk<FeatureFlagClient>(relaxed = true)
  private val deploymentFetcher = mockk<DeploymentFetcher>(relaxed = true)
  private val trackingIdentityFetcher = mockk<TrackingIdentityFetcher>(relaxed = true)
  private val stateCheckSumErrorReporter = mockk<StateCheckSumErrorReporter>(relaxed = true)
  private val connectionId = UUID.randomUUID()
  private val workspaceId = UUID.randomUUID()
  private val deployment = Deployment(DeploymentMetadataRead(UUID.randomUUID(), "test-mode", "test-version"))
  private val trackingIdentity = TrackingIdentity(UUID.randomUUID(), "test-email", null, null, null)
  private val jobId = 123L
  private val attemptNumber = 1
  private val epochMilliSupplier: Supplier<Long> = Supplier { 1718277705335 }
  private val idSupplier: Supplier<UUID> = Supplier { UUID.fromString("8d9ccf10-0ddd-4533-a4c7-9e502abd4723") }

  private lateinit var handler: StateCheckSumCountEventHandler

  @BeforeEach
  fun setUp() {
    every { trackingIdentityFetcher.apply(any()) } returns trackingIdentity
    every { deploymentFetcher.get() } returns deployment
    every { featureFlagClient.boolVariation(any(), any()) } returns true
    every { stateCheckSumErrorReporter.reportError(any(), any(), any(), any(), any(), any(), any(), any(), any()) } just Runs
    handler =
      StateCheckSumCountEventHandler(
        pubSubWriter = Optional.of(pubSubWriter),
        featureFlagClient = featureFlagClient,
        deploymentFetcher = deploymentFetcher,
        trackingIdentityFetcher = trackingIdentityFetcher,
        stateCheckSumReporter = stateCheckSumErrorReporter,
        connectionId = connectionId,
        workspaceId = workspaceId,
        jobId = jobId,
        attemptNumber = attemptNumber,
        epochMilliSupplier = epochMilliSupplier,
        idSupplier = idSupplier,
      )
  }

  @AfterEach
  fun tearDown() {
    clearAllMocks()
  }

  @Test
  fun `no state message seen should not emit any events`() {
    handler.close(true)
    verify(exactly = 1) { pubSubWriter.close() }
    verify(exactly = 0) { pubSubWriter.publishEvent(any<List<StateCheckSumCountEvent>>()) }
  }

  @Test
  fun `default epochMilliSupplier test`() {
    val handler =
      StateCheckSumCountEventHandler(
        pubSubWriter = Optional.of(pubSubWriter),
        featureFlagClient = featureFlagClient,
        deploymentFetcher = deploymentFetcher,
        trackingIdentityFetcher = trackingIdentityFetcher,
        stateCheckSumReporter = stateCheckSumErrorReporter,
        connectionId = connectionId,
        workspaceId = workspaceId,
        jobId = jobId,
        attemptNumber = attemptNumber,
      )
    val timeInMicroSecond = handler.getCurrentTimeInMicroSecond()
    val instant = Instant.ofEpochMilli(timeInMicroSecond / 1000)
    Thread.sleep(1)
    val timeInMicroSecond2 = handler.getCurrentTimeInMicroSecond()
    val instant2 = Instant.ofEpochMilli(timeInMicroSecond2 / 1000)
    assertTrue(timeInMicroSecond2 > timeInMicroSecond)
    assertTrue(instant2.isAfter(instant))
  }

  @Nested
  internal inner class SourceDestinationCombinedCheckSumTests {
    @Test
    internal fun `should emit 3 events when no error is reported`() {
      val sourceStateMessage =
        airbyteStateMessageWithOutAnyCounts()
          .withSourceStats(AirbyteStateStats().withRecordCount(1.0))
      val destinationStateMessage =
        airbyteStateMessageWithOutAnyCounts()
          .withDestinationStats(AirbyteStateStats().withRecordCount(1.0))
          .withSourceStats(AirbyteStateStats().withRecordCount(1.0))

      handler.validateStateChecksum(
        stateMessage = sourceStateMessage,
        platformRecordCount = 1.0,
        origin = AirbyteMessageOrigin.SOURCE,
        failOnInvalidChecksum = true,
        checksumValidationEnabled = true,
      )
      handler.validateStateChecksum(
        stateMessage = destinationStateMessage,
        platformRecordCount = 1.0,
        origin = AirbyteMessageOrigin.DESTINATION,
        failOnInvalidChecksum = true,
        checksumValidationEnabled = true,
      )
      handler.close(true)

      verify(exactly = 1) { pubSubWriter.close() }
      val expectedMessages =
        listOf(
          checkSumEventForStateMessage(1.0, AirbyteMessageOrigin.SOURCE, DUMMY_STATE_MESSAGE),
          checkSumEventForStateMessage(1.0, AirbyteMessageOrigin.INTERNAL, DUMMY_STATE_MESSAGE),
          checkSumEventForStateMessage(1.0, AirbyteMessageOrigin.DESTINATION, DUMMY_STATE_MESSAGE),
        )
      verify(exactly = 1) { pubSubWriter.publishEvent(expectedMessages) }
    }

    @Test
    internal fun `should emit 0 events when not completed successfully`() {
      val sourceStateMessage =
        airbyteStateMessageWithOutAnyCounts()
          .withSourceStats(AirbyteStateStats().withRecordCount(1.0))
      val destinationStateMessage =
        airbyteStateMessageWithOutAnyCounts()
          .withDestinationStats(AirbyteStateStats().withRecordCount(1.0))
          .withSourceStats(AirbyteStateStats().withRecordCount(1.0))

      handler.validateStateChecksum(
        stateMessage = sourceStateMessage,
        platformRecordCount = 1.0,
        origin = AirbyteMessageOrigin.SOURCE,
        failOnInvalidChecksum = true,
        checksumValidationEnabled = true,
      )
      handler.validateStateChecksum(
        stateMessage = destinationStateMessage,
        platformRecordCount = 1.0,
        origin = AirbyteMessageOrigin.DESTINATION,
        failOnInvalidChecksum = true,
        checksumValidationEnabled = true,
      )
      handler.close(false)

      verify(exactly = 1) { pubSubWriter.close() }
      verify(exactly = 0) { pubSubWriter.publishEvent(any()) }
    }
  }

  @Nested
  internal inner class SourceStateCheckSumTests {
    @Test
    internal fun `source count is present and equals platform count`() {
      val stateMessage =
        airbyteStateMessageWithOutAnyCounts()
          .withSourceStats(AirbyteStateStats().withRecordCount(1.0))

      handler.validateStateChecksum(
        stateMessage = stateMessage,
        platformRecordCount = 1.0,
        origin = AirbyteMessageOrigin.SOURCE,
        failOnInvalidChecksum = true,
        checksumValidationEnabled = true,
      )
      handler.close(true)

      verify(exactly = 1) { pubSubWriter.close() }
      verify(exactly = 0) { pubSubWriter.publishEvent(any<List<StateCheckSumCountEvent>>()) }
    }

    @Test
    internal fun `source count present but doesn't equal platform count`() {
      val stateMessage =
        airbyteStateMessageWithOutAnyCounts()
          .withSourceStats(AirbyteStateStats().withRecordCount(2.0))

      assertThrows<InvalidChecksumException> {
        handler.validateStateChecksum(
          stateMessage = stateMessage,
          platformRecordCount = 1.0,
          origin = AirbyteMessageOrigin.SOURCE,
          failOnInvalidChecksum = true,
          checksumValidationEnabled = true,
        )
      }

      handler.close(true)

      verify(exactly = 1) { pubSubWriter.close() }
      verify(exactly = 0) { pubSubWriter.publishEvent(any<List<StateCheckSumCountEvent>>()) }
    }

    @Test
    internal fun `source count is missing`() {
      val stateMessage = airbyteStateMessageWithOutAnyCounts()

      handler.validateStateChecksum(
        stateMessage = stateMessage,
        platformRecordCount = 1.0,
        origin = AirbyteMessageOrigin.SOURCE,
        failOnInvalidChecksum = true,
        checksumValidationEnabled = true,
      )
      handler.close(true)

      verify(exactly = 1) { pubSubWriter.close() }
      verify(exactly = 0) { pubSubWriter.publishEvent(any<List<StateCheckSumCountEvent>>()) }
    }
  }

  @Nested
  internal inner class DestinationStateCheckSumTests {
    @Test
    internal fun `destination count is present and equals platform count, source count is missing`() {
      val stateMessage =
        airbyteStateMessageWithOutAnyCounts()
          .withDestinationStats(AirbyteStateStats().withRecordCount(1.0))

      handler.validateStateChecksum(
        stateMessage = stateMessage,
        platformRecordCount = 1.0,
        origin = AirbyteMessageOrigin.DESTINATION,
        failOnInvalidChecksum = true,
        checksumValidationEnabled = true,
      )
      handler.close(true)

      verify(exactly = 1) { pubSubWriter.close() }
      val expectedMessages =
        listOf(
          checkSumEventForStateMessage(1.0, AirbyteMessageOrigin.INTERNAL, stateMessage),
          checkSumEventForStateMessage(1.0, AirbyteMessageOrigin.DESTINATION, stateMessage),
        )
      verify(exactly = 1) { pubSubWriter.publishEvent(expectedMessages) }
    }

    @Test
    internal fun `destination count is present but doesn't equal platform count, source count is missing`() {
      val stateMessage =
        airbyteStateMessageWithOutAnyCounts()
          .withDestinationStats(AirbyteStateStats().withRecordCount(2.0))

      assertThrows<InvalidChecksumException> {
        handler.validateStateChecksum(
          stateMessage = stateMessage,
          platformRecordCount = 1.0,
          origin = AirbyteMessageOrigin.DESTINATION,
          failOnInvalidChecksum = true,
          checksumValidationEnabled = true,
        )
      }

      handler.validateStateChecksum(
        stateMessage = stateMessage,
        platformRecordCount = 1.0,
        origin = AirbyteMessageOrigin.DESTINATION,
        failOnInvalidChecksum = false,
        checksumValidationEnabled = true,
      )
      handler.close(true)

      verify(exactly = 1) { pubSubWriter.close() }
      val expectedMessages =
        listOf(
          checkSumEventForStateMessage(1.0, AirbyteMessageOrigin.INTERNAL, stateMessage),
          checkSumEventForStateMessage(2.0, AirbyteMessageOrigin.DESTINATION, stateMessage),
        )
      verify(exactly = 1) { pubSubWriter.publishEvent(expectedMessages) }
    }

    @Test
    internal fun `destination count is present, source count is present and both equal platform count`() {
      val stateMessage =
        airbyteStateMessageWithOutAnyCounts()
          .withDestinationStats(AirbyteStateStats().withRecordCount(1.0))
          .withSourceStats(AirbyteStateStats().withRecordCount(1.0))

      handler.validateStateChecksum(
        stateMessage = stateMessage,
        platformRecordCount = 1.0,
        origin = AirbyteMessageOrigin.DESTINATION,
        failOnInvalidChecksum = true,
        checksumValidationEnabled = true,
      )
      handler.close(true)

      verify(exactly = 1) { pubSubWriter.close() }
      // No event is emitted even after close because we have not seen a source state message
      verify(exactly = 0) { pubSubWriter.publishEvent(any<List<StateCheckSumCountEvent>>()) }
    }

    @Test
    internal fun `destination count is present and equals platform count but doesn't equal source count`() {
      val stateMessage =
        airbyteStateMessageWithOutAnyCounts()
          .withDestinationStats(AirbyteStateStats().withRecordCount(1.0))
          .withSourceStats(AirbyteStateStats().withRecordCount(2.0))

      assertThrows<InvalidChecksumException> {
        handler.validateStateChecksum(
          stateMessage = stateMessage,
          platformRecordCount = 1.0,
          origin = AirbyteMessageOrigin.DESTINATION,
          failOnInvalidChecksum = true,
          checksumValidationEnabled = true,
        )
      }

      handler.close(true)

      verify(exactly = 1) { pubSubWriter.close() }
      val expectedMessages =
        listOf(
          checkSumEventForStateMessage(2.0, AirbyteMessageOrigin.SOURCE, stateMessage),
          checkSumEventForStateMessage(1.0, AirbyteMessageOrigin.INTERNAL, stateMessage),
          checkSumEventForStateMessage(1.0, AirbyteMessageOrigin.DESTINATION, stateMessage),
        )
      verify(exactly = 1) { pubSubWriter.publishEvent(expectedMessages) }
    }

    @Test
    internal fun `destination count is present and equals source count but doesn't equal platform count`() {
      val stateMessage =
        airbyteStateMessageWithOutAnyCounts()
          .withDestinationStats(AirbyteStateStats().withRecordCount(2.0))
          .withSourceStats(AirbyteStateStats().withRecordCount(2.0))

      assertThrows<InvalidChecksumException> {
        handler.validateStateChecksum(
          stateMessage = stateMessage,
          platformRecordCount = 1.0,
          origin = AirbyteMessageOrigin.DESTINATION,
          failOnInvalidChecksum = true,
          checksumValidationEnabled = true,
        )
      }

      handler.close(true)

      verify(exactly = 1) { pubSubWriter.close() }
      val expectedMessages =
        listOf(
          checkSumEventForStateMessage(2.0, AirbyteMessageOrigin.SOURCE, stateMessage),
          checkSumEventForStateMessage(1.0, AirbyteMessageOrigin.INTERNAL, stateMessage),
          checkSumEventForStateMessage(2.0, AirbyteMessageOrigin.DESTINATION, stateMessage),
        )
      verify(exactly = 1) { pubSubWriter.publishEvent(expectedMessages) }
    }

    @Test
    internal fun `destination count is present but doesnt equal source count and platform count`() {
      val stateMessage =
        airbyteStateMessageWithOutAnyCounts()
          .withDestinationStats(AirbyteStateStats().withRecordCount(5.0))
          .withSourceStats(AirbyteStateStats().withRecordCount(1.0))

      assertThrows<InvalidChecksumException> {
        handler.validateStateChecksum(
          stateMessage = stateMessage,
          platformRecordCount = 1.0,
          origin = AirbyteMessageOrigin.DESTINATION,
          failOnInvalidChecksum = true,
          checksumValidationEnabled = true,
        )
      }

      handler.close(true)

      verify(exactly = 1) { pubSubWriter.close() }
      val expectedMessages =
        listOf(
          checkSumEventForStateMessage(1.0, AirbyteMessageOrigin.SOURCE, stateMessage),
          checkSumEventForStateMessage(1.0, AirbyteMessageOrigin.INTERNAL, stateMessage),
          checkSumEventForStateMessage(5.0, AirbyteMessageOrigin.DESTINATION, stateMessage),
        )
      verify(exactly = 1) { pubSubWriter.publishEvent(expectedMessages) }
    }

    @Test
    internal fun `all 3 counts present but none match each other`() {
      val stateMessage =
        airbyteStateMessageWithOutAnyCounts()
          .withDestinationStats(AirbyteStateStats().withRecordCount(5.0))
          .withSourceStats(AirbyteStateStats().withRecordCount(3.0))

      assertThrows<InvalidChecksumException> {
        handler.validateStateChecksum(
          stateMessage = stateMessage,
          platformRecordCount = 1.0,
          origin = AirbyteMessageOrigin.DESTINATION,
          failOnInvalidChecksum = true,
          checksumValidationEnabled = true,
        )
      }

      handler.close(true)

      verify(exactly = 1) { pubSubWriter.close() }
      val expectedMessages =
        listOf(
          checkSumEventForStateMessage(3.0, AirbyteMessageOrigin.SOURCE, stateMessage),
          checkSumEventForStateMessage(1.0, AirbyteMessageOrigin.INTERNAL, stateMessage),
          checkSumEventForStateMessage(5.0, AirbyteMessageOrigin.DESTINATION, stateMessage),
        )
      verify(exactly = 1) { pubSubWriter.publishEvent(expectedMessages) }
    }

    @Test
    internal fun `destination count and source count both are missing`() {
      val stateMessage = airbyteStateMessageWithOutAnyCounts()

      handler.validateStateChecksum(
        stateMessage = stateMessage,
        platformRecordCount = 1.0,
        origin = AirbyteMessageOrigin.DESTINATION,
        failOnInvalidChecksum = true,
        checksumValidationEnabled = true,
      )
      handler.close(true)

      verify(exactly = 1) { pubSubWriter.close() }
      val expectedMessages =
        listOf(
          checkSumEventForStateMessage(1.0, AirbyteMessageOrigin.INTERNAL, stateMessage),
        )
      verify(exactly = 1) { pubSubWriter.publishEvent(expectedMessages) }
    }

    @Test
    internal fun `destination count is missing, source count is present and equals platform count`() {
      val stateMessage =
        airbyteStateMessageWithOutAnyCounts()
          .withSourceStats(AirbyteStateStats().withRecordCount(1.0))

      handler.validateStateChecksum(
        stateMessage = stateMessage,
        platformRecordCount = 1.0,
        origin = AirbyteMessageOrigin.DESTINATION,
        failOnInvalidChecksum = true,
        checksumValidationEnabled = true,
      )
      handler.close(true)

      verify(exactly = 1) { pubSubWriter.close() }
      val expectedMessages =
        listOf(
          checkSumEventForStateMessage(1.0, AirbyteMessageOrigin.SOURCE, stateMessage),
          checkSumEventForStateMessage(1.0, AirbyteMessageOrigin.INTERNAL, stateMessage),
        )
      verify(exactly = 1) { pubSubWriter.publishEvent(expectedMessages) }
    }

    @Test
    internal fun `destination count is missing, source count is present but doesn't equal platform count`() {
      val stateMessage =
        airbyteStateMessageWithOutAnyCounts()
          .withSourceStats(AirbyteStateStats().withRecordCount(2.0))

      handler.validateStateChecksum(
        stateMessage = stateMessage,
        platformRecordCount = 1.0,
        origin = AirbyteMessageOrigin.DESTINATION,
        failOnInvalidChecksum = true,
        checksumValidationEnabled = true,
      )
      handler.close(true)

      verify(exactly = 1) { pubSubWriter.close() }
      val expectedMessages =
        listOf(
          checkSumEventForStateMessage(2.0, AirbyteMessageOrigin.SOURCE, stateMessage),
          checkSumEventForStateMessage(1.0, AirbyteMessageOrigin.INTERNAL, stateMessage),
        )
      verify(exactly = 1) { pubSubWriter.publishEvent(expectedMessages) }
    }
  }

  private fun airbyteStateMessageWithOutAnyCounts(): AirbyteStateMessage {
    val stateMessage =
      StateWithId.attachIdToStateMessageFromSource(
        AirbyteStateMessage()
          .withType(AirbyteStateMessage.AirbyteStateType.STREAM)
          .withStream(
            AirbyteStreamState()
              .withStreamState(Jsons.jsonNode(mapOf("cursor" to "value")))
              .withStreamDescriptor(StreamDescriptor().withNamespace("dummy-namespace").withName("dummy-name")),
          ),
      )
    return stateMessage
  }

  private fun checkSumEventForStateMessage(
    recordCount: Double,
    stateOrigin: AirbyteMessageOrigin,
    stateMessage: AirbyteStateMessage,
  ): StateCheckSumCountEvent {
    val nameNamespacePair = getNameNamespacePair(stateMessage)
    return StateCheckSumCountEvent(
      deployment.getDeploymentVersion(),
      attemptNumber.toLong(),
      connectionId.toString(),
      deployment.getDeploymentId().toString(),
      deployment.getDeploymentMode(),
      trackingIdentity.email,
      idSupplier.get().toString(),
      jobId.toString(),
      recordCount.toLong(),
      stateMessage.getStateHashCode(Hashing.murmur3_32_fixed()).toString(),
      stateMessage.getStateIdForStatsTracking().toString(),
      stateOrigin.name,
      stateMessage.type.toString(),
      nameNamespacePair.name,
      nameNamespacePair.namespace,
      epochMilliSupplier.get() * 1000,
      true,
    )
  }
}
