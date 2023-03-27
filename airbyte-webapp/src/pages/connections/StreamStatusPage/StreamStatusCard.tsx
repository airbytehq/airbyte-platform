import { FormattedMessage } from "react-intl";

import { useConnectionSyncContext } from "components/connection/ConnectionSync/ConnectionSyncContext";
import {
  filterEmptyStreamStatuses,
  StreamStatusType,
  useSortStreams,
} from "components/connection/StreamStatus/streamStatusUtils";
import { StreamStatusIndicator, StreamStatusLoadingSpinner } from "components/connection/StreamStatusIndicator";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { useStreamsListContext } from "./StreamsListContext";
import styles from "./StreamStatusCard.module.scss";

export const StreamStatusCard: React.FC = () => {
  const { streams } = useStreamsListContext();
  const { syncStarting, jobSyncRunning, resetStarting, jobResetRunning } = useConnectionSyncContext();

  const streamsByStatus = filterEmptyStreamStatuses(useSortStreams(streams));

  return (
    <FlexContainer gap="xl">
      {streamsByStatus.map(([status, streams]) => (
        <FlexContainer alignItems="center" gap="sm" className={styles.container} key={status}>
          <StreamStatusIndicator status={status as StreamStatusType} withBox />
          <Text size="xl" as="span">
            {streams.length}
          </Text>
          <FormattedMessage id={`connection.stream.status.${status}`} />
          {(syncStarting || jobSyncRunning || resetStarting || jobResetRunning) && (
            <FlexContainer alignItems="center" gap="none">
              <StreamStatusLoadingSpinner className={styles.syncingSpinner} />
            </FlexContainer>
          )}
        </FlexContainer>
      ))}
    </FlexContainer>
  );
};
