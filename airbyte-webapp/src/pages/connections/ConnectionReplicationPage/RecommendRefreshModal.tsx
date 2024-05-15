import { useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { LabeledSwitch } from "components";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { ModalBody, ModalFooter } from "components/ui/Modal";
import { Text } from "components/ui/Text";

import { ConnectionStream } from "core/api/types/AirbyteClient";

import { StreamsRefreshListBlock } from "../ConnectionSettingsPage/StreamsRefreshListBlock";

interface RecommendedRefreshWarningProps {
  onCancel: () => void;
  onComplete: (withRefresh: boolean) => void;
  streamsToRefresh: ConnectionStream[];
  totalStreams: number;
}
export const RecommendRefreshModal: React.FC<RecommendedRefreshWarningProps> = ({
  onCancel,
  onComplete,
  streamsToRefresh,
  totalStreams,
}) => {
  const [withRefresh, setWithRefresh] = useState(true);
  const { formatMessage } = useIntl();
  return (
    <>
      <ModalBody>
        <Text>
          <FormattedMessage id="connection.refreshDataHint" />
        </Text>
        <Box py="md">
          <StreamsRefreshListBlock streamsToList={streamsToRefresh} totalStreams={totalStreams} />
        </Box>
        <Box pt="md">
          <Text color="grey400" size="sm">
            <FormattedMessage id="connection.refreshDataHint.description" />
          </Text>
        </Box>
        <p>
          <LabeledSwitch
            name="refresh"
            checked={withRefresh}
            onChange={(ev) => setWithRefresh(ev.target.checked)}
            label={formatMessage({
              id: "connection.saveWithRefresh",
            })}
            checkbox
            data-testid="refreshModal-refresh-checkbox"
          />
        </p>
      </ModalBody>
      <ModalFooter>
        <Button onClick={onCancel} variant="secondary" data-testid="refreshModal-cancel">
          <FormattedMessage id="form.cancel" />
        </Button>
        <Button onClick={() => onComplete(withRefresh)} data-testid="refreshModal-save">
          <FormattedMessage id="connection.save" />
        </Button>
      </ModalFooter>
    </>
  );
};
