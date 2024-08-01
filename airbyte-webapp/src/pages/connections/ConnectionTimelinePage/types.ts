import * as yup from "yup";

import { ConnectionEventType, FailureOrigin, FailureType, JobConfigType } from "core/api/types/AirbyteClient";

/**
 * add a new event type to the connection timeline:
 *
 * 1. Add a new schema object for the event summary (unless it matches one that already exists)
 * 2. Add a new schema for the event, OR add the event_type to an existing one
 * 3. use <schemaYouMade>.isValidSync(event) to check if the event is of that type in the return statement of ConnectionTimelinePage.  You can use InferType<typeof <schemaYouMade>> to reference the type in code.
 */

/**
 * SCHEMA OBJECTS
 */

// property-specific schemas
const streamDescriptorSchema = yup.object({
  name: yup.string().required(),
  namespace: yup.string().optional(),
});

const jobRunningStreamSchema = yup.object({
  streamName: yup.string().required(),
  streamNamespace: yup.string().optional(),
  configType: yup.mixed<JobConfigType>().oneOf(["sync", "refresh", "clear", "reset_connection"]).required(),
});

export const jobFailureReasonSchema = yup.object({
  failureType: yup.mixed<FailureType>().optional(),
  failureOrigin: yup.mixed<FailureOrigin>().optional(),
  externalMessage: yup.string().optional(),
  internalMessage: yup.string().optional(),
  retryable: yup.boolean().optional(),
  timestamp: yup.number().required(),
  stacktrace: yup.string().optional(),
});

export const userInEventSchema = yup.object({
  email: yup.string().optional(),
  id: yup.string().optional(),
  name: yup.string().optional(),
});

// artificial job events
export const jobRunningSummarySchema = yup.object({
  jobId: yup.number().required(),
  streams: yup.array().of(jobRunningStreamSchema).required(),
  configType: yup.mixed<JobConfigType>().oneOf(["clear", "reset_connection", "sync", "refresh"]).required(),
});

// jobs
export const syncEventSummarySchema = yup.object({
  startTimeEpochSeconds: yup.number().required(),
  endTimeEpochSeconds: yup.number().required(),
  attemptsCount: yup.number().optional(),
  bytesLoaded: yup.number().required(),
  recordsLoaded: yup.number().optional(),
  jobId: yup.number().required(),
});

export const syncFailureEventSummarySchema = yup.object({
  startTimeEpochSeconds: yup.number().required(),
  endTimeEpochSeconds: yup.number().required(),
  attemptsCount: yup.number().optional(),
  bytesLoaded: yup.number().optional(),
  recordsLoaded: yup.number().optional(),
  jobId: yup.number().required(),
  failureReason: jobFailureReasonSchema.required(),
});

export const refreshEventSummarySchema = yup.object({
  startTimeEpochSeconds: yup.number().required(),
  endTimeEpochSeconds: yup.number().required(),
  attemptsCount: yup.number().optional(),
  streams: yup.array().of(streamDescriptorSchema).required(),
  jobId: yup.number().required(),
});

export const clearEventSummarySchema = yup.object({
  startTimeEpochSeconds: yup.number().required(),
  endTimeEpochSeconds: yup.number().required(),
  attemptsCount: yup.number().optional(),
  streams: yup.array().of(streamDescriptorSchema).required(),
  jobId: yup.number().required(),
});

export const jobStartedSummarySchema = yup.object({
  streams: yup.array().of(streamDescriptorSchema).optional(),
  startTimeEpochSeconds: yup.number().required(),
  jobId: yup.number().required(),
});

export const generalEventSchema = yup.object({
  id: yup.string().required(),
  connectionId: yup.string().required(),
  user: userInEventSchema.optional(),
  createdAt: yup.number().required(),
  eventType: yup
    .mixed<ConnectionEventType | "RUNNING_JOB">()
    .oneOf([...Object.values(ConnectionEventType), "RUNNING_JOB"])
    .required(),
  summary: yup.mixed().required(),
});

export const syncEventSchema = generalEventSchema.shape({
  eventType: yup
    .mixed<ConnectionEventType>()
    .oneOf([ConnectionEventType.SYNC_SUCCEEDED, ConnectionEventType.SYNC_CANCELLED])
    .required(),
  summary: syncEventSummarySchema.required(),
});

export const syncFailEventSchema = generalEventSchema.shape({
  eventType: yup
    .mixed<ConnectionEventType>()
    .oneOf([ConnectionEventType.SYNC_FAILED, ConnectionEventType.SYNC_INCOMPLETE])
    .required(),
  summary: syncFailureEventSummarySchema.required(),
});

export const refreshEventSchema = generalEventSchema.shape({
  eventType: yup
    .mixed<ConnectionEventType>()
    .oneOf([
      ConnectionEventType.REFRESH_SUCCEEDED,
      ConnectionEventType.REFRESH_FAILED,
      ConnectionEventType.REFRESH_INCOMPLETE,
      ConnectionEventType.REFRESH_CANCELLED,
    ])
    .required(),
  summary: refreshEventSummarySchema.required(),
});

export const clearEventSchema = generalEventSchema.shape({
  eventType: yup
    .mixed<ConnectionEventType>()
    .oneOf([
      ConnectionEventType.CLEAR_SUCCEEDED,
      ConnectionEventType.CLEAR_FAILED,
      ConnectionEventType.CLEAR_INCOMPLETE,
      ConnectionEventType.CLEAR_CANCELLED,
    ])
    .required(),
  summary: clearEventSummarySchema.required(),
});

export const jobStartedEventSchema = generalEventSchema.shape({
  eventType: yup
    .mixed<ConnectionEventType>()
    .oneOf([ConnectionEventType.CLEAR_STARTED, ConnectionEventType.REFRESH_STARTED, ConnectionEventType.SYNC_STARTED])
    .required(),
  summary: jobStartedSummarySchema.required(),
});

export const jobRunningSchema = generalEventSchema.shape({
  eventType: yup.string().oneOf(["RUNNING_JOB"]).required(),
  summary: jobRunningSummarySchema.required(),
});
