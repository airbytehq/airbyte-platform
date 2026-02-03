import { FormattedMessage } from "react-intl";

import { Text } from "components/ui/Text";

import { JobConfigType } from "core/api/types/AirbyteClient";

interface CancelJobModalBodyProps {
  isConnectionInitialSync: boolean;
  streamsSyncingForFirstTime: string[];
  configType: JobConfigType;
}
export const CancelJobModalBody: React.FC<CancelJobModalBodyProps> = ({
  isConnectionInitialSync,
  streamsSyncingForFirstTime,
  configType,
}) => {
  if (isConnectionInitialSync) {
    return <FormattedMessage id="connection.actions.cancel.confirm.body.initialSync" />;
  }

  if (streamsSyncingForFirstTime.length === 1) {
    const streamValues = streamsSyncingForFirstTime.map((stream) => {
      return (
        <Text key={stream} as="span" size="lg">
          {stream}
        </Text>
      );
    });

    return (
      <FormattedMessage
        id="connection.actions.cancel.confirm.body.streamInitialSync"
        values={{ streamValues, count: streamValues.length }}
      />
    );
  }
  const configTypeId =
    configType === JobConfigType.sync
      ? "connection.actions.sync"
      : configType === JobConfigType.refresh
      ? "connection.actions.refresh"
      : "connection.actions.clear";
  return (
    <FormattedMessage
      id="connection.actions.cancel.body.generic"
      values={{
        configType: <FormattedMessage id={configTypeId} />,
      }}
    />
  );
};
