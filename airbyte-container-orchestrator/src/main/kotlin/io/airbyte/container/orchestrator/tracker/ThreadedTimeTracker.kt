/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.tracker

import jakarta.inject.Singleton

/**
 * This class exists to track timing information for the sync. It needs to be thread-safe as
 * multiple threads (source, destination, and worker) will be accessing it.
 */
@Singleton
class ThreadedTimeTracker {
  /** lock used for synchronization */
  private val lock = Any()

  /**
   * The properties in this class are odd. They have private synchronized setters to ensure they are only set via
   * the track* methods, and they have public synchronized getters.
   */

  private var _replicationStartTime: Long = 0
  var replicationStartTime: Long
    get() = synchronized(lock) { _replicationStartTime }
    private set(value) = synchronized(lock) { _replicationStartTime = value }

  private var _replicationEndTime: Long = 0
  var replicationEndTime: Long
    get() = synchronized(lock) { _replicationEndTime }
    private set(value) = synchronized(lock) { _replicationEndTime = value }

  private var _sourceReadStartTime: Long = 0
  var sourceReadStartTime: Long
    get() = synchronized(lock) { _sourceReadStartTime }
    private set(value) = synchronized(lock) { _sourceReadStartTime = value }

  private var _sourceReadEndTime: Long = 0
  var sourceReadEndTime: Long
    get() = synchronized(lock) { _sourceReadEndTime }
    private set(value) = synchronized(lock) { _sourceReadEndTime = value }

  private var _destinationWriteStartTime: Long = 0
  var destinationWriteStartTime: Long
    get() = synchronized(lock) { _destinationWriteStartTime }
    private set(value) = synchronized(lock) { _destinationWriteStartTime = value }

  private var _destinationWriteEndTime: Long = 0
  var destinationWriteEndTime: Long
    get() = synchronized(lock) { _destinationWriteEndTime }
    private set(value) = synchronized(lock) { _destinationWriteEndTime = value }

  fun trackReplicationStartTime() {
    this.replicationStartTime = System.currentTimeMillis()
  }

  fun trackReplicationEndTime() {
    this.replicationEndTime = System.currentTimeMillis()
  }

  fun trackSourceReadStartTime() {
    this.sourceReadStartTime = System.currentTimeMillis()
  }

  fun trackSourceReadEndTime() {
    this.sourceReadEndTime = System.currentTimeMillis()
  }

  fun trackDestinationWriteStartTime() {
    this.destinationWriteStartTime = System.currentTimeMillis()
  }

  fun trackDestinationWriteEndTime() {
    this.destinationWriteEndTime = System.currentTimeMillis()
  }
}
