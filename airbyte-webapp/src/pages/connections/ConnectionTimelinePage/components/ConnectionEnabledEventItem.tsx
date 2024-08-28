import { FormattedMessage } from "react-intl";
import { InferType } from "yup";

import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { TimelineEventUser } from "./TimelineEventUser";
import { ConnectionTimelineEventActions } from "../ConnectionTimelineEventActions";
import { ConnectionTimelineEventIcon } from "../ConnectionTimelineEventIcon";
import { ConnectionTimelineEventItem } from "../ConnectionTimelineEventItem";
import { ConnectionTimelineEventSummary } from "../ConnectionTimelineEventSummary";
import { connectionEnabledEventSchema } from "../types";
import { titleIdMap } from "../utils";

interface ConnectionEnabledEventItemProps {
  event: InferType<typeof connectionEnabledEventSchema>;
}

export const ConnectionEnabledEventItem: React.FC<ConnectionEnabledEventItemProps> = ({ event }) => {
  const titleId = titleIdMap[event.eventType];

  return (
    <ConnectionTimelineEventItem>
      <ConnectionTimelineEventIcon icon="connection" />
      <ConnectionTimelineEventSummary>
        <FlexContainer gap="xs" direction="column">
          <Text bold>
            <FormattedMessage id={titleId} />
          </Text>
          <Text as="span" size="sm" color="grey400">
            <FormattedMessage
              id="connection.timeline.connection_enabled.description"
              values={{
                user: <TimelineEventUser user={event.user} />,
              }}
            />
          </Text>
        </FlexContainer>
      </ConnectionTimelineEventSummary>
      <ConnectionTimelineEventActions createdAt={event.createdAt} />
    </ConnectionTimelineEventItem>
  );
};
