import { FormattedMessage } from "react-intl";
import { z } from "zod";

import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { StreamMapperType } from "core/api/types/AirbyteClient";

import { TimelineEventUser } from "./TimelineEventUser";
import { ConnectionTimelineEventActions } from "../ConnectionTimelineEventActions";
import { ConnectionTimelineEventIcon } from "../ConnectionTimelineEventIcon";
import { ConnectionTimelineEventItem } from "../ConnectionTimelineEventItem";
import { ConnectionTimelineEventSummary } from "../ConnectionTimelineEventSummary";
import { mappingEventSchema } from "../types";

// TODO: import from titleIdMap in utils.tsx once it's added
// issue_link: https://github.com/airbytehq/airbyte-internal-issues/issues/10947
export const mappingTitleIdMap: Record<string, string> = {
  MAPPING_CREATE: "connection.timeline.mapping_create",
  MAPPING_UPDATE: "connection.timeline.mapping_update",
  MAPPING_DELETE: "connection.timeline.mapping_delete",
};

const mapperTypeToMessageIdMap: Record<StreamMapperType, string> = {
  [StreamMapperType.hashing]: "hashing",
  [StreamMapperType["field-renaming"]]: "field_renaming",
  [StreamMapperType["row-filtering"]]: "row_filtering",
  [StreamMapperType.encryption]: "encryption",
  [StreamMapperType["field-filtering"]]: "field_filtering",
};

interface MappingEventItemProps {
  event: z.infer<typeof mappingEventSchema>;
}

export const MappingEventItem: React.FC<MappingEventItemProps> = ({ event }) => {
  const messageId = mappingTitleIdMap[event.eventType];
  const mapperTypeId = mapperTypeToMessageIdMap[event.summary.mapperType];

  return (
    <ConnectionTimelineEventItem>
      <ConnectionTimelineEventIcon icon="mapping" />
      <ConnectionTimelineEventSummary>
        <FlexContainer gap="xs" direction="column">
          <Text bold>
            <FormattedMessage id={messageId} />
          </Text>
          <Text as="span" size="sm" color="grey400">
            <FormattedMessage
              id={`${messageId}.${mapperTypeId}.description`}
              values={{
                user: <TimelineEventUser user={event.user} />,
                stream: event.summary.streamName,
                namespace: event.summary.streamNamespace,
              }}
            />
          </Text>
        </FlexContainer>
      </ConnectionTimelineEventSummary>
      <ConnectionTimelineEventActions createdAt={event.createdAt} />
    </ConnectionTimelineEventItem>
  );
};
