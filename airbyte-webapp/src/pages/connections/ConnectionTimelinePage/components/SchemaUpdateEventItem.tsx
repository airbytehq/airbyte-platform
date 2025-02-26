import { useMemo } from "react";
import { FormattedDate, FormattedList, FormattedMessage, useIntl } from "react-intl";
import { InferType } from "yup";

import { CatalogDiffModal } from "components/connection/CatalogDiffModal";
import { getSortedDiff } from "components/connection/CatalogDiffModal/utils";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { useModalService } from "hooks/services/Modal";

import { TimelineEventUser } from "./TimelineEventUser";
import { ConnectionTimelineEventActions } from "../ConnectionTimelineEventActions";
import { ConnectionTimelineEventIcon } from "../ConnectionTimelineEventIcon";
import { ConnectionTimelineEventItem } from "../ConnectionTimelineEventItem";
import { ConnectionTimelineEventSummary } from "../ConnectionTimelineEventSummary";
import { schemaUpdateEventSchema } from "../types";
import { titleIdMap } from "../utils";

export const SchemaUpdateEventItem: React.FC<{ event: InferType<typeof schemaUpdateEventSchema> }> = ({ event }) => {
  const titleId = titleIdMap[event.eventType];
  const { formatMessage } = useIntl();
  const { openModal } = useModalService();

  const { newItems, removedItems, changedItems } = useMemo(
    () => getSortedDiff(event.summary.catalogDiff.transforms),
    [event.summary.catalogDiff.transforms]
  );

  const schemaMessage = useMemo(() => {
    const parts = [];

    if (newItems.length > 0) {
      parts.push(formatMessage({ id: "connection.timeline.schema_update.streamsAdded" }, { streams: newItems.length }));
    }

    if (removedItems.length > 0) {
      parts.push(
        formatMessage({ id: "connection.timeline.schema_update.streamsRemoved" }, { streams: removedItems.length })
      );
    }
    if (changedItems.length > 0) {
      parts.push(
        formatMessage({ id: "connection.timeline.schema_update.fieldChanges" }, { streams: changedItems.length })
      );
    }

    return (
      <>
        <FormattedList value={parts} />.
      </>
    );
  }, [changedItems.length, formatMessage, newItems.length, removedItems.length]);

  const triggerSchemaChangesModal = () =>
    openModal<void>({
      title: (
        <FlexContainer alignItems="center">
          <ConnectionTimelineEventIcon icon="schema" size="lg" />
          <FlexContainer gap="xs" direction="column">
            <Text size="lg">
              <FormattedMessage id="connection.timeline.schema_update" />
            </Text>
            <Text size="lg" color="grey400">
              <FormattedDate value={event.createdAt * 1000} timeStyle="short" dateStyle="medium" />
            </Text>
          </FlexContainer>
        </FlexContainer>
      ),
      size: "md",
      testId: "catalog-diff-modal",
      content: () => <CatalogDiffModal catalogDiff={event.summary.catalogDiff} />,
    });

  return (
    <ConnectionTimelineEventItem>
      <ConnectionTimelineEventIcon icon="schema" />
      <ConnectionTimelineEventSummary>
        <FlexContainer gap="xs" direction="column">
          <Text bold>
            <FormattedMessage id={titleId} />
          </Text>

          <Text as="span" size="sm" color="grey400">
            <FormattedMessage
              id="connection.timeline.schema_update.description"
              values={{
                user: !!event.user ? (
                  <TimelineEventUser user={event.user} />
                ) : (
                  <FormattedMessage id="general.airbyte" />
                ),
                schemaMessage,
              }}
            />{" "}
            <Button variant="link" onClick={triggerSchemaChangesModal}>
              <FormattedMessage id="connection.timeline.schema_update.viewDetails" />
            </Button>
          </Text>
        </FlexContainer>
      </ConnectionTimelineEventSummary>
      <ConnectionTimelineEventActions createdAt={event.createdAt} />
    </ConnectionTimelineEventItem>
  );
};
