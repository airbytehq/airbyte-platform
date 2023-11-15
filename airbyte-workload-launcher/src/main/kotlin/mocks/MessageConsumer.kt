package io.airbyte.workload.launcher.mocks

import io.airbyte.commons.json.Jsons
import io.airbyte.commons.resources.MoreResources
import io.airbyte.workload.launcher.pipeline.LauncherInput
import io.airbyte.workload.launcher.serde.PayloadDeserializer
import jakarta.inject.Singleton
import java.lang.Thread.sleep
import java.util.concurrent.atomic.AtomicInteger

@Singleton
class MessageConsumer(private val deserializer: PayloadDeserializer) {
  private val squirtle = MoreResources.readResource("squirtle-e2e-replication-input.json")
  private val charmander = MoreResources.readResource("charmander-e2e-replication-input.json")
  private val bulbasaur = MoreResources.readResource("bulbasaur-e2e-replication-input.json")
  private val pikachu = MoreResources.readResource("pikachu-e2e-replication-input.json")

  private var mockJobId = AtomicInteger(1)

  private val inputs =
    mutableListOf(
      squirtle,
      charmander,
      bulbasaur,
      pikachu,
    )

  fun read(): LauncherInput {
    // simulate a slow publish-interval for now
    sleep(30000)
    // cycle messages (is there a Kotlin-y way to do this?)
    val input = inputs.removeFirst()
    inputs.add(input)

    // mock incrementing ids
    val id = mockJobId.getAndIncrement().toString()
    val mocked = mockJobIdFor(input, id)

    return LauncherInput("workload-$id", mocked, mapOf(), "/")
  }

  private fun mockJobIdFor(
    input: String,
    id: String,
  ): String {
    // just deserializing/re-serializing to make it easier to set
    val parsed = deserializer.toReplicationInput(input)

    parsed.jobRunConfig.jobId = id
    parsed.sourceLauncherConfig.jobId = id
    parsed.destinationLauncherConfig.jobId = id

    return Jsons.serialize(parsed)
  }

  fun size(): Long {
    return inputs.size.toLong()
  }
}
