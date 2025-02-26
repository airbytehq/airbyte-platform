import React from "react";
import { FormattedMessage } from "react-intl";
import { InferType } from "yup";

import { FlexContainer } from "components/ui/Flex";
import { Link } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { useCurrentWorkspaceLink } from "area/workspace/utils";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { ConnectionRoutePaths, RoutePaths } from "pages/routePaths";

import { TimelineEventUser } from "./TimelineEventUser";
import { ConnectionTimelineEventActions } from "../ConnectionTimelineEventActions";
import { ConnectionTimelineEventIcon } from "../ConnectionTimelineEventIcon";
import { ConnectionTimelineEventItem } from "../ConnectionTimelineEventItem";
import { ConnectionTimelineEventSummary } from "../ConnectionTimelineEventSummary";
import { connectionDisabledEventSchema } from "../types";
import { titleIdMap } from "../utils";

interface ConnectionDisabledEventItemProps {
  event: InferType<typeof connectionDisabledEventSchema>;
}

export const ConnectionDisabledEventItem: React.FC<ConnectionDisabledEventItemProps> = ({ event }) => {
  const titleId = titleIdMap[event.eventType];

  return (
    <ConnectionTimelineEventItem>
      <ConnectionTimelineEventIcon icon="connection" />
      <ConnectionTimelineEventSummary>
        <FlexContainer gap="xs" direction="column">
          <Text bold>
            <FormattedMessage id={titleId} />
          </Text>
          {event.user ? (
            <Text as="span" size="sm" color="grey400">
              <FormattedMessage
                id="connection.timeline.connection_disabled.description"
                values={{
                  user: <TimelineEventUser user={event.user} />,
                }}
              />
            </Text>
          ) : event.summary.disabledReason ? (
            <Text as="span" size="sm" color="grey400">
              <FormattedMessage
                id={`connectionAutoDisabledReason.${event.summary.disabledReason}`}
                values={{ schemaTabLink: SchemaTabLink }}
              />
            </Text>
          ) : null}
        </FlexContainer>
      </ConnectionTimelineEventSummary>
      <ConnectionTimelineEventActions createdAt={event.createdAt} />
    </ConnectionTimelineEventItem>
  );
};

const SchemaTabLink = (children: React.ReactNode) => {
  const createLink = useCurrentWorkspaceLink();
  const {
    connection: { connectionId },
  } = useConnectionEditService();

  return (
    <Link to={createLink(`/${RoutePaths.Connections}/${connectionId}/${ConnectionRoutePaths.Replication}`)}>
      {children}
    </Link>
  );
};
