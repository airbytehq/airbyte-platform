/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.logging.logback

/**
 * The qualified class name of the calling code for logging purposes.  This
 * key should be added to the MDC when the point of logging is not the point
 * in the code that should be recorded by the logger layout.
 */
const val CALLER_QUALIFIED_CLASS_NAME_PATTERN = "CALLER_FQCN"

/**
 * The line number of the calling code for logging purposes.  This
 * key should be added to the MDC when the point of logging is not the point
 * in the code that should be recorded by the logger layout.
 */
const val CALLER_LINE_NUMBER_PATTERN = "CALLER_LINE_NUMBER"

/**
 * The method name of the calling code for logging purposes.  This
 * key should be added to the MDC when the point of logging is not the point
 * in the code that should be recorded by the logger layout.
 */
const val CALLER_METHOD_NAME_PATTERN = "CALLER_METHOD_NAME"

/**
 * The thread name of the calling code for logging purposes.  This
 * key should be added to the MDC when the point of logging is not the point
 * in the code that should be recorded by the logger layout.
 */
const val CALLER_THREAD_NAME_PATTERN = "CALLER_THREAD_NAME"

const val CLOUD_OPERATIONS_JOB_LOGGER_NAME = "airbyte-cloud-operations-job-logger"
const val PLATFORM_LOGGER_NAME = "airbyte-platform-logger"
const val AUDIT_LOGGER_NAME = "airbyte-audit-logger"
