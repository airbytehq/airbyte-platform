import { InferType } from "yup";

import { Box } from "components/ui/Box";

import { ConnectionEvent } from "core/api/types/AirbyteClient";

import { ClearEventItem } from "./ClearEventItem";
import styles from "./EventLineItem.module.scss";
import { JobStartEventItem } from "./JobStartEventItem";
import { RefreshEventItem } from "./RefreshEventItem";
import { RunningJobItem } from "./RunningJobItem";
import { SyncEventItem } from "./SyncEventItem";
import { SyncFailEventItem } from "./SyncFailEventItem";
import {
  clearEventSchema,
  syncFailEventSchema,
  jobStartedEventSchema,
  refreshEventSchema,
  syncEventSchema,
  jobRunningSchema,
} from "../types";

export const EventLineItem: React.FC<{ event: ConnectionEvent | InferType<typeof jobRunningSchema> }> = ({ event }) => {
  // streams is only present at the top level on a ConnectionSyncProgressRead, not on the ConnectionEvent items
  if (jobRunningSchema.isValidSync(event, { recursive: true, stripUnknown: true })) {
    return (
      <Box py="lg" key={event.id}>
        <RunningJobItem jobRunningItem={event} />
      </Box>
    );
  } else if (syncEventSchema.isValidSync(event, { recursive: true, stripUnknown: true })) {
    return (
      <Box py="lg" key={event.id}>
        <SyncEventItem syncEvent={event} />
      </Box>
    );
  } else if (syncFailEventSchema.isValidSync(event, { recursive: true, stripUnknown: true })) {
    return (
      <Box py="lg" key={event.id}>
        <SyncFailEventItem syncEvent={event} />
      </Box>
    );
  } else if (refreshEventSchema.isValidSync(event, { recursive: true, stripUnknown: true })) {
    return (
      <Box py="lg" key={event.id}>
        <RefreshEventItem refreshEvent={event} key={event.id} />
      </Box>
    );
  } else if (clearEventSchema.isValidSync(event, { recursive: true, stripUnknown: true })) {
    return (
      <Box py="lg" key={event.id}>
        <ClearEventItem clearEvent={event} />
      </Box>
    );
  } else if (jobStartedEventSchema.isValidSync(event, { recursive: true, stripUnknown: true })) {
    return (
      <Box py="lg" key={event.id}>
        <JobStartEventItem jobStartEvent={event} />
      </Box>
    );
  }
  // virtuoso cannot gracefully handle a <1px item inside an li, so we need to return a 1px tall empty item
  // https://virtuoso.dev/troubleshooting/#i-get-error-zero-sized-element-this-should-not-happen
  // https://github.com/petyosi/react-virtuoso/issues/35
  return <span className={styles.emptyItem} />;
};
