/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.utils

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.json.gson.GsonFactory
import com.google.api.services.sqladmin.SQLAdmin
import com.google.api.services.sqladmin.model.Database
import com.google.api.services.sqladmin.model.Operation
import com.google.auth.http.HttpCredentialsAdapter
import com.google.auth.oauth2.GoogleCredentials
import com.google.common.annotations.VisibleForTesting
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.http.HttpStatus
import java.io.IOException
import java.util.concurrent.Callable

/**
 * Creates and deletes GCP CloudSQL databases.
 */
class CloudSqlDatabaseProvisioner {
  private val sqlAdmin: SQLAdmin
  private val maxPollAttempts: Int
  private val maxApiCallAttempts: Int

  @VisibleForTesting
  internal constructor(sqlAdmin: SQLAdmin, maxPollAttempts: Int, maxApiCallAttempts: Int) {
    this.sqlAdmin = sqlAdmin
    this.maxPollAttempts = maxPollAttempts
    this.maxApiCallAttempts = maxApiCallAttempts
  }

  constructor() {
    this.sqlAdmin =
      SQLAdmin
        .Builder(
          GoogleNetHttpTransport.newTrustedTransport(),
          GsonFactory.getDefaultInstance(),
          HttpCredentialsAdapter(GoogleCredentials.getApplicationDefault()),
        ).setApplicationName(
          APPLICATION_NAME,
        ).build()
    this.maxPollAttempts = DEFAULT_MAX_POLL_ATTEMPTS
    this.maxApiCallAttempts = DEFAULT_MAX_API_CALL_ATTEMPTS
  }

  @Synchronized
  @Throws(IOException::class, InterruptedException::class)
  fun createDatabase(
    projectId: String?,
    instanceId: String?,
    databaseName: String,
  ): String {
    val database = Database().setName(databaseName)
    val operation =
      runWithRetry {
        sqlAdmin.databases().insert(projectId, instanceId, database).execute()
      }
    pollOperation(projectId, operation.name)

    return databaseName
  }

  @Synchronized
  @Throws(IOException::class, InterruptedException::class)
  fun deleteDatabase(
    projectId: String?,
    instanceId: String?,
    databaseName: String?,
  ) {
    val operation =
      runWithRetry {
        sqlAdmin.databases().delete(projectId, instanceId, databaseName).execute()
      }
    pollOperation(projectId, operation.name)
  }

  /**
   * Database operations are asynchronous. This method polls the operation until it is done.
   */
  @VisibleForTesting
  @Throws(IOException::class, InterruptedException::class)
  fun pollOperation(
    projectId: String?,
    operationName: String,
  ) {
    var pollAttempts = 0
    while (pollAttempts < maxPollAttempts) {
      val operation = sqlAdmin.operations()[projectId, operationName].execute()
      if (SQL_OPERATION_DONE_STATUS == operation.status) {
        return
      }
      Thread.sleep(1000)
      pollAttempts += 1
    }

    throw RuntimeException("Operation  $operationName did not complete successfully")
  }

  /**
   * If there's another operation already in progress in one same cloudsql instance then the api will
   * return a 409 error. This method will retry api calls that return a 409 error.
   */
  @VisibleForTesting
  @Throws(InterruptedException::class)
  fun runWithRetry(callable: Callable<Operation>): Operation {
    var attempts = 0
    while (attempts < maxApiCallAttempts) {
      try {
        return callable.call()
      } catch (e: GoogleJsonResponseException) {
        if (e.statusCode == HttpStatus.SC_CONFLICT) {
          attempts++
          log.info { "Attempt $attempts failed with 409 error" }
          log.info("Exception thrown by API: " + e.message)
          Thread.sleep(1000)
        }
      } catch (e: Exception) {
        throw RuntimeException(e)
      }
    }
    throw RuntimeException("Max retries exceeded. Could not complete operation.")
  }

  companion object {
    private val log = KotlinLogging.logger {}

    private const val SQL_OPERATION_DONE_STATUS = "DONE"
    private const val DEFAULT_MAX_POLL_ATTEMPTS = 10
    private const val DEFAULT_MAX_API_CALL_ATTEMPTS = 10
    private const val APPLICATION_NAME = "cloud-sql-database-provisioner"
  }
}
