import { FormattedMessage } from "react-intl";

import { AirbyteStreamWithStatusAndConfiguration } from "components/connection/StreamStatus/getStreamsWithStatus";
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

const SyncingStreams: React.FC<{ streams: AirbyteStreamWithStatusAndConfiguration[] }> = ({ streams }) => {
  const streamsSyncing = streams
    .map((stream) => stream.config?.isSyncing || stream.config?.isResetting)
    .filter(Boolean);
  if (!streamsSyncing.length) {
    return null;
  }

  return (
    <FlexContainer alignItems="center" gap="sm">
      <StreamStatusLoadingSpinner className={styles.syncingSpinner} />
      <div>{streamsSyncing.length}</div>
    </FlexContainer>
  );
};

export const StreamStatusCard: React.FC = () => {
  const { streams } = useStreamsListContext();

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
          <SyncingStreams streams={streams} />
        </FlexContainer>
      ))}
    </FlexContainer>
  );
};
