import classNames from "classnames";
import React, { useState } from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";
import { Virtuoso } from "react-virtuoso";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Text } from "components/ui/Text";

import { ConnectionStream } from "core/api/types/AirbyteClient";

import { ConnectionRefreshFormValues } from "./ConnectionRefreshModal";
import styles from "./StreamsRefreshListBlock.module.scss";

interface StreamsRefreshListBlockProps {
  streamsSupportingMergeRefresh: ConnectionStream[];
  streamsSupportingTruncateRefresh: ConnectionStream[];
  totalStreams: number;
}

export const StreamsRefreshListBlock: React.FC<StreamsRefreshListBlockProps> = ({
  streamsSupportingMergeRefresh,
  streamsSupportingTruncateRefresh,
  totalStreams,
}) => {
  const [isExpanded, setIsExpanded] = useState(false);
  const { watch } = useFormContext<ConnectionRefreshFormValues>();
  const refreshType = watch("refreshType");

  const streamsToList = refreshType === "merge" ? streamsSupportingMergeRefresh : streamsSupportingTruncateRefresh;
  const initialStreamsCount = 5;

  const visibleStreams = isExpanded ? streamsToList : streamsToList.slice(0, initialStreamsCount);

  const Header = () => (
    <Box pb="md">
      <Text size="sm" bold>
        <FormattedMessage
          id="connection.actions.refreshData.affectedStreamsList.title"
          values={{ affected: streamsToList.length, total: totalStreams }}
        />
      </Text>
    </Box>
  );

  return (
    <>
      {isExpanded ? (
        <div
          className={classNames(
            styles.streamsRefreshListBlock__container,
            styles["streamsRefreshListBlock__container--expanded"]
          )}
        >
          <Virtuoso
            components={{ Header }}
            data={visibleStreams}
            totalCount={visibleStreams.length}
            itemContent={(_index, stream: ConnectionStream) => (
              <Box px="sm" py="xs">
                <Text italicized size="sm">
                  {stream.streamName}
                </Text>
              </Box>
            )}
            overscan={500}
          />
        </div>
      ) : (
        <div className={styles.streamsRefreshListBlock__container}>
          <Header />
          <Box pb={streamsToList.length - visibleStreams.length > 0 ? "xl" : "xs"}>
            {visibleStreams.map((stream) => (
              <Box px="xs" py="xs" key={stream.streamName}>
                <Text italicized size="sm">
                  {stream.streamName}
                </Text>
              </Box>
            ))}
          </Box>
          {streamsToList.length - visibleStreams.length > 0 && (
            <Button
              variant="clear"
              onClick={() => setIsExpanded(true)}
              className={styles.streamsRefreshListBlock__seeMoreButton}
            >
              <Text>
                <FormattedMessage
                  id="connection.actions.refreshData.affectedStreamsList.seeMore"
                  values={{ count: streamsToList.length - visibleStreams.length }}
                />
              </Text>
            </Button>
          )}
        </div>
      )}
    </>
  );
};
