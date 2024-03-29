package io.airbyte.workload.launcher.pods.factories

import io.airbyte.commons.envvar.EnvVar
import io.airbyte.workers.process.KubePodProcess
import io.fabric8.kubernetes.api.model.CapabilitiesBuilder
import io.fabric8.kubernetes.api.model.Container
import io.fabric8.kubernetes.api.model.ContainerBuilder
import io.fabric8.kubernetes.api.model.ResourceRequirements
import io.fabric8.kubernetes.api.model.SeccompProfileBuilder
import io.fabric8.kubernetes.api.model.SecurityContext
import io.fabric8.kubernetes.api.model.SecurityContextBuilder
import io.fabric8.kubernetes.api.model.VolumeMount
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton

@Singleton
class InitContainerFactory(
  @Value("\${airbyte.worker.job.kube.images.busybox}") private val imageName: String,
) {
  fun create(
    resourceReqs: ResourceRequirements,
    volumeMounts: List<VolumeMount>,
  ): Container {
    return ContainerBuilder()
      .withName(KubePodProcess.INIT_CONTAINER_NAME)
      .withImage(imageName)
      .withWorkingDir(KubePodProcess.CONFIG_DIR)
      .withCommand(
        listOf(
          "sh",
          "-c",
          String.format(
            """
            i=0
            until [ ${'$'}i -gt 60 ]
            do
              echo "${'$'}i - waiting for config file transfer to complete..."
              # check if the upload-complete file exists, if so exit without error
              if [ -f "%s/%s" ]; then
                # Wait 50ms for the incoming kubectl cp call to cleanly exit
                sleep .05
                exit 0
              fi
              i=${'$'}((i+1))
              sleep 1
            done
            echo "config files did not transfer in time"
            # no upload-complete file was created in time, exit with error
            exit 1
            """.trimIndent(),
            KubePodProcess.CONFIG_DIR,
            KubePodProcess.SUCCESS_FILE_NAME,
          ),
        ),
      )
      .withResources(resourceReqs)
      .withVolumeMounts(volumeMounts)
      .withSecurityContext(containerSecurityContext())
      .build()
  }
}

private fun containerSecurityContext(): SecurityContext? =
  when (EnvVar.ROOTLESS_WORKLOAD.fetch(default = "false").toBoolean()) {
    true ->
      SecurityContextBuilder()
        .withAllowPrivilegeEscalation(false)
        .withRunAsUser(1000L)
        .withRunAsGroup(1000L)
        .withReadOnlyRootFilesystem(false)
        .withRunAsNonRoot(true)
        .withCapabilities(CapabilitiesBuilder().addAllToDrop(listOf("ALL")).build())
        .withSeccompProfile(SeccompProfileBuilder().withType("RuntimeDefault").build())
        .build()
    false -> null
  }
