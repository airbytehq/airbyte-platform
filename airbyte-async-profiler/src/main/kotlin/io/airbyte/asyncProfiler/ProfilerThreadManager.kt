/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.asyncProfiler

import io.airbyte.commons.storage.StorageClient
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.io.File
import java.nio.file.Files

/**
 * Manages running the ProfilerService on multiple processes in parallel.
 */
@Singleton
class ProfilerThreadManager(
  private val profilerService: ProfilerService,
  @Value("\${airbyte.connection-id}") private val connectionId: String,
  @Value("\${airbyte.job-id}") private val jobId: Long,
  @Value("\${airbyte.attempt-id}") private val attemptId: String,
  @Named("profilerOutputStore") private val storageClient: StorageClient,
) {
  private val logger = KotlinLogging.logger {}

  data class ProfilingJob(
    val mainClassKeyword: String,
    val eventType: String,
  )

  data class ProfilingResult(
    val resultStatus: ProfilerResult,
    val message: String,
    val outputFilePath: String?,
  )

  enum class ProfilerResult {
    SUCCESS,
    FAILED,
  }

  /**
   * Starts a thread for each [ProfilingJob], waits for them to complete,
   * and uploads the results.
   */
  fun startProfilingJobs(jobs: List<ProfilingJob>) {
    val threads = mutableListOf<Thread>()
    val results = mutableMapOf<String, ProfilingResult>()

    // Create a thread per job
    jobs.forEach { job ->
      val thread =
        Thread {
          val keyword = job.mainClassKeyword
          try {
            logger.info { "Starting profiler for process keyword='$keyword'..." }
            val profilingResult =
              profilerService.runProfiler(
                mainClassKeyword = job.mainClassKeyword,
                eventType = job.eventType,
              )
            logger.info { "Profiler finished for '$keyword' with result: $profilingResult" }
            if (profilingResult.resultStatus == ProfilerResult.SUCCESS && profilingResult.outputFilePath != null) {
              val outputFile = File(profilingResult.outputFilePath)
              uploadFile(outputFile, profilingResult)
            }
            synchronized(results) {
              results[keyword] = profilingResult
            }
          } catch (ex: Exception) {
            logger.error(ex) { "Error while profiling process with keyword='$keyword'." }
          }
        }.apply {
          name = "ProfilerThread-${job.mainClassKeyword}"
        }
      threads.add(thread)
    }

    threads.forEach { it.start() }

    // Wait for them to finish
    threads.forEach { it.join() }

    logger.info { "All profiling threads have completed." }

    results.forEach { (keyword, result) ->
      logger.info { "Result for process keyword='$keyword': $result" }
    }
  }

  private fun uploadFile(
    outputFile: File,
    profilingResult: ProfilingResult,
  ) {
    if (outputFile.exists()) {
      val remotePath = "$connectionId/$jobId/$attemptId/${outputFile.name}"
      storageClient.write(remotePath, Files.readString(outputFile.toPath()))
    } else {
      logger.warn { "File ${profilingResult.outputFilePath} not found; skipping upload." }
    }
  }
}
