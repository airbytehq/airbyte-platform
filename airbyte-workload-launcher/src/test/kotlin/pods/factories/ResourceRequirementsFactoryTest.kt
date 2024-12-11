package pods.factories

import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.config.SyncResourceRequirements
import io.airbyte.config.helpers.ResourceRequirementsUtils
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.pod.ResourceConversionUtils
import io.airbyte.workload.launcher.pods.factories.ResourceRequirementsFactory
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import pods.factories.ResourceRequirementsFactoryTest.Fixtures.defaultCheckConnectorReqs
import pods.factories.ResourceRequirementsFactoryTest.Fixtures.defaultDiscoverConnectorReqs
import pods.factories.ResourceRequirementsFactoryTest.Fixtures.fileTransferReqs
import pods.factories.ResourceRequirementsFactoryTest.Fixtures.sidecarReqs
import pods.factories.ResourceRequirementsFactoryTest.Fixtures.specConnectorReqs
import java.util.stream.Stream
import io.airbyte.config.ResourceRequirements as AirbyteResourceRequirements

class ResourceRequirementsFactoryTest {
  private lateinit var factory: ResourceRequirementsFactory

  @BeforeEach
  fun setup() {
    factory =
      ResourceRequirementsFactory(
        defaultCheckConnectorReqs,
        defaultDiscoverConnectorReqs,
        specConnectorReqs,
        sidecarReqs,
        fileTransferReqs,
      )
  }

  @ParameterizedTest
  @MethodSource("requirementsMatrix")
  fun `builds orchestrator reqs from input`(reqs: AirbyteResourceRequirements?) {
    val input =
      ReplicationInput()
        .withSyncResourceRequirements(
          SyncResourceRequirements()
            .withOrchestrator(reqs),
        )

    val result = factory.orchestrator(input)

    Assertions.assertEquals(reqs, result)
  }

  @ParameterizedTest
  @MethodSource("requirementsMatrix")
  fun `builds source reqs from input (non-file transfer)`(reqs: AirbyteResourceRequirements?) {
    val input =
      ReplicationInput()
        .withSyncResourceRequirements(
          SyncResourceRequirements()
            .withSource(reqs),
        ).withUseFileTransfer(false)

    val result = factory.replSource(input)

    Assertions.assertEquals(reqs, result)
  }

  @ParameterizedTest
  @MethodSource("requirementsMatrix")
  fun `builds source reqs from input (file transfer)`(reqs: AirbyteResourceRequirements?) {
    val input =
      ReplicationInput()
        .withSyncResourceRequirements(
          SyncResourceRequirements()
            .withSource(reqs),
        ).withUseFileTransfer(true)

    val result = factory.replSource(input)

    Assertions.assertEquals(reqs?.cpuLimit, result?.cpuLimit)
    Assertions.assertEquals(reqs?.cpuRequest, result?.cpuRequest)
    Assertions.assertEquals(reqs?.memoryLimit, result?.memoryLimit)
    Assertions.assertEquals(reqs?.memoryRequest, result?.memoryRequest)
    Assertions.assertEquals(fileTransferReqs.ephemeralStorageLimit, result?.ephemeralStorageLimit)
    Assertions.assertEquals(fileTransferReqs.ephemeralStorageRequest, result?.ephemeralStorageRequest)
  }

  @ParameterizedTest
  @MethodSource("requirementsMatrix")
  fun `builds dest reqs from input`(reqs: AirbyteResourceRequirements?) {
    val input =
      ReplicationInput()
        .withSyncResourceRequirements(
          SyncResourceRequirements()
            .withDestination(reqs),
        )

    val result = factory.replDestination(input)

    Assertions.assertEquals(reqs, result)
  }

  @Test
  fun `builds sidecar reqs from static config`() {
    val result = factory.sidecar()

    Assertions.assertEquals(sidecarReqs, result)
  }

  @ParameterizedTest
  @MethodSource("nonNullRequirementsMatrix")
  fun `builds check connector reqs from input if provided`(reqs: AirbyteResourceRequirements) {
    val input =
      CheckConnectionInput(
        mockk(),
        mockk(),
        StandardCheckConnectionInput()
          .withResourceRequirements(reqs),
      )

    val result = factory.checkConnector(input)

    val expected = ResourceRequirementsUtils.mergeResourceRequirements(reqs, defaultCheckConnectorReqs)

    Assertions.assertEquals(expected, result)
  }

  @Test
  fun `builds check connector reqs from defaults if not provided`() {
    val input =
      CheckConnectionInput(
        mockk(),
        mockk(),
        StandardCheckConnectionInput(),
      )

    val result = factory.checkConnector(input)

    Assertions.assertEquals(defaultCheckConnectorReqs, result)
  }

  @ParameterizedTest
  @MethodSource("nonNullRequirementsMatrix")
  fun `builds discover connector reqs from input if provided`(reqs: AirbyteResourceRequirements) {
    val input =
      DiscoverCatalogInput(
        mockk(),
        mockk(),
        StandardDiscoverCatalogInput()
          .withResourceRequirements(reqs),
      )

    val result = factory.discoverConnector(input)

    val expected = ResourceRequirementsUtils.mergeResourceRequirements(reqs, defaultDiscoverConnectorReqs)

    Assertions.assertEquals(expected, result)
  }

  @Test
  fun `builds discover connector reqs from defaults if not provided`() {
    val input =
      DiscoverCatalogInput(
        mockk(),
        mockk(),
        StandardDiscoverCatalogInput(),
      )

    val result = factory.discoverConnector(input)

    Assertions.assertEquals(defaultDiscoverConnectorReqs, result)
  }

  @Test
  fun `builds spec connector reqs from static config`() {
    val result = factory.specConnector()

    Assertions.assertEquals(specConnectorReqs, result)
  }

  @ParameterizedTest
  @MethodSource("requirementsMatrix")
  fun `builds repl init reqs from orch reqs on input`(reqs: AirbyteResourceRequirements?) {
    val input =
      ReplicationInput()
        .withSyncResourceRequirements(
          SyncResourceRequirements()
            .withOrchestrator(reqs),
        )

    val result = factory.replInit(input)

    Assertions.assertEquals(reqs, result)
  }

  @ParameterizedTest
  @MethodSource("nonNullRequirementsMatrix")
  fun `builds check init reqs from sum of connector and sidecar config`(reqs: AirbyteResourceRequirements) {
    val input =
      CheckConnectionInput(
        jobRunConfig = mockk(),
        launcherConfig = mockk(),
        checkConnectionInput = mockk(),
      )
    val spy = spyk(factory)
    every { spy.checkConnector(input) } returns reqs

    val result = spy.checkInit(input)

    val expected = ResourceConversionUtils.sumResourceRequirements(reqs, sidecarReqs)

    Assertions.assertEquals(expected, result)
  }

  @ParameterizedTest
  @MethodSource("nonNullRequirementsMatrix")
  fun `builds discover init reqs from sum of connector and sidecar config`(reqs: AirbyteResourceRequirements) {
    val input =
      DiscoverCatalogInput(
        jobRunConfig = mockk(),
        launcherConfig = mockk(),
        discoverCatalogInput = mockk(),
      )
    val spy = spyk(factory)
    every { spy.discoverConnector(input) } returns reqs

    val result = spy.discoverInit(input)

    val expected = ResourceConversionUtils.sumResourceRequirements(reqs, sidecarReqs)

    Assertions.assertEquals(expected, result)
  }

  @Test
  fun `builds spec init reqs from static connector config`() {
    val result = factory.specInit()

    Assertions.assertEquals(specConnectorReqs, result)
  }

  object Fixtures {
    val defaultCheckConnectorReqs =
      AirbyteResourceRequirements()
        .withCpuLimit("2")
        .withCpuRequest("1")
        .withMemoryLimit("600Mi")
        .withMemoryRequest("400Mi")
    val defaultDiscoverConnectorReqs =
      AirbyteResourceRequirements()
        .withCpuLimit("1")
        .withCpuRequest("0.5")
        .withMemoryLimit("300Mi")
        .withMemoryRequest("200Mi")
    val specConnectorReqs =
      AirbyteResourceRequirements()
        .withCpuLimit("0.5")
        .withCpuRequest("0.2")
        .withMemoryLimit("500Mi")
        .withMemoryRequest("100Mi")
    val sidecarReqs =
      AirbyteResourceRequirements()
        .withCpuLimit("3")
        .withCpuRequest("3")
        .withMemoryLimit("1000Mi")
        .withMemoryRequest("800Mi")
    val fileTransferReqs =
      AirbyteResourceRequirements()
        .withEphemeralStorageLimit("6Gi")
        .withEphemeralStorageRequest("100Mi")
  }

  companion object {
    @JvmStatic
    private fun requirementsMatrix(): Stream<Arguments> =
      Stream.of(
        Arguments.of(null),
        Arguments.of(
          AirbyteResourceRequirements()
            .withCpuLimit("1")
            .withCpuRequest("0.5")
            .withMemoryLimit("1000Mi")
            .withMemoryRequest("800Mi"),
        ),
        Arguments.of(
          AirbyteResourceRequirements()
            .withCpuLimit("2")
            .withCpuRequest("1")
            .withMemoryLimit("2000Mi")
            .withMemoryRequest("2000Mi"),
        ),
        Arguments.of(
          AirbyteResourceRequirements()
            .withCpuLimit("3")
            .withCpuRequest("3")
            .withMemoryLimit("1000Mi")
            .withMemoryRequest("500Mi")
            .withEphemeralStorageLimit("2Gi")
            .withEphemeralStorageRequest("1Gi"),
        ),
        Arguments.of(
          AirbyteResourceRequirements()
            .withCpuLimit("1")
            .withCpuRequest("0.1")
            .withMemoryLimit("1G")
            .withMemoryRequest("1G")
            .withEphemeralStorageLimit("5Gi")
            .withEphemeralStorageRequest("5Gi"),
        ),
      )

    @JvmStatic
    private fun nonNullRequirementsMatrix() = requirementsMatrix().skip(1)
  }
}
