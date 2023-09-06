/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config;

/**
 * POJO for the EarlySyncJob domain model. We run the EARLY_SYNC_JOB_QUERY query to identify early
 * syncs, by looking for sync jobs that are successful and have their associated connector created
 * in the last 7 days. Query returns the id and is_free columns from the jobs table, which is then
 * mapped to this POJO.
 */
public class EarlySyncJob {

  private final Long id;
  private final Boolean isFree;

  public EarlySyncJob(final Long id, final Boolean isFree) {
    this.id = id;
    this.isFree = isFree;
  }

  public Long getId() {
    return id;
  }

  public Boolean isFree() {
    return isFree;
  }

}
