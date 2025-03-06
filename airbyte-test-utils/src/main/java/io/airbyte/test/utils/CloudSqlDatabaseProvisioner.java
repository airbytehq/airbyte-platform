/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.test.utils;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.sqladmin.SQLAdmin;
import com.google.api.services.sqladmin.model.Database;
import com.google.api.services.sqladmin.model.Operation;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.concurrent.Callable;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates and deletes GCP CloudSQL databases.
 */
@SuppressWarnings("PMD.LooseCoupling")
public class CloudSqlDatabaseProvisioner {

  private static final Logger LOGGER = LoggerFactory.getLogger(CloudSqlDatabaseProvisioner.class);

  private static final String SQL_OPERATION_DONE_STATUS = "DONE";
  private static final int DEFAULT_MAX_POLL_ATTEMPTS = 10;
  private static final int DEFAULT_MAX_API_CALL_ATTEMPTS = 10;
  private static final String APPLICATION_NAME = "cloud-sql-database-provisioner";

  private final SQLAdmin sqlAdmin;
  private final int maxPollAttempts;
  private final int maxApiCallAttempts;

  @VisibleForTesting
  CloudSqlDatabaseProvisioner(SQLAdmin sqlAdmin, int maxPollAttempts, int maxApiCallAttempts) {
    this.sqlAdmin = sqlAdmin;
    this.maxPollAttempts = maxPollAttempts;
    this.maxApiCallAttempts = maxApiCallAttempts;
  }

  public CloudSqlDatabaseProvisioner() throws GeneralSecurityException, IOException {
    this.sqlAdmin = new SQLAdmin.Builder(
        GoogleNetHttpTransport.newTrustedTransport(),
        GsonFactory.getDefaultInstance(),
        new HttpCredentialsAdapter(GoogleCredentials.getApplicationDefault())).setApplicationName(APPLICATION_NAME).build();
    this.maxPollAttempts = DEFAULT_MAX_POLL_ATTEMPTS;
    this.maxApiCallAttempts = DEFAULT_MAX_API_CALL_ATTEMPTS;
  }

  public synchronized String createDatabase(String projectId, String instanceId, String databaseName) throws IOException, InterruptedException {
    Database database = new Database().setName(databaseName);
    Operation operation = runWithRetry(() -> sqlAdmin.databases().insert(projectId, instanceId, database).execute());
    pollOperation(projectId, operation.getName());

    return databaseName;
  }

  public synchronized void deleteDatabase(String projectId, String instanceId, String databaseName) throws IOException, InterruptedException {
    Operation operation = runWithRetry(() -> sqlAdmin.databases().delete(projectId, instanceId, databaseName).execute());
    pollOperation(projectId, operation.getName());
  }

  /**
   * Database operations are asynchronous. This method polls the operation until it is done.
   */
  @VisibleForTesting
  void pollOperation(String projectId, String operationName) throws IOException, InterruptedException {
    int pollAttempts = 0;
    while (pollAttempts < maxPollAttempts) {
      Operation operation = sqlAdmin.operations().get(projectId, operationName).execute();
      if (SQL_OPERATION_DONE_STATUS.equals(operation.getStatus())) {
        return;
      }
      Thread.sleep(1000);
      pollAttempts += 1;
    }

    throw new RuntimeException("Operation  " + operationName + " did not complete successfully");
  }

  /**
   * If there's another operation already in progress in one same cloudsql instance then the api will
   * return a 409 error. This method will retry api calls that return a 409 error.
   */
  @VisibleForTesting
  Operation runWithRetry(Callable<Operation> callable) throws InterruptedException {
    int attempts = 0;
    while (attempts < maxApiCallAttempts) {
      try {
        return callable.call();
      } catch (GoogleJsonResponseException e) {
        if (e.getStatusCode() == HttpStatus.SC_CONFLICT) {
          attempts++;
          LOGGER.info("Attempt " + attempts + " failed with 409 error");
          LOGGER.info("Exception thrown by API: " + e.getMessage());
          Thread.sleep(1000);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    throw new RuntimeException("Max retries exceeded. Could not complete operation.");
  }

}
