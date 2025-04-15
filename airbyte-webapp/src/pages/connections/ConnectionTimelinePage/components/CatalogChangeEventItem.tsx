import { useMemo } from "react";
import { FormattedDate, FormattedList, FormattedMessage, useIntl } from "react-intl";
import { z } from "zod";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { CatalogConfigDiff, FieldDataTypeDiff, StreamFieldStatusChanged } from "core/api/types/AirbyteClient";
import { useModalService } from "hooks/services/Modal";

import styles from "./CatalogChangeEventItem.module.scss";
import { CatalogConfigDiffModal } from "./CatalogConfigDiffModal/CatalogConfigDiffModal";
import { TimelineEventUser } from "./TimelineEventUser";
import { ConnectionTimelineEventActions } from "../ConnectionTimelineEventActions";
import { ConnectionTimelineEventIcon } from "../ConnectionTimelineEventIcon";
import { ConnectionTimelineEventItem } from "../ConnectionTimelineEventItem";
import { ConnectionTimelineEventSummary } from "../ConnectionTimelineEventSummary";
import { schemaConfigUpdateEventSchema } from "../types";
import { transformCatalogDiffToCatalogConfigDiff } from "../utils";
export interface CatalogConfigDiffExtended extends CatalogConfigDiff {
  streamsAdded?: StreamFieldStatusChanged[];
  streamsRemoved?: StreamFieldStatusChanged[];
  fieldsAdded?: StreamFieldStatusChanged[];
  fieldsRemoved?: StreamFieldStatusChanged[];
  fieldsDataTypeChanged?: FieldDataTypeDiff[];
}

type FieldChangeType = Pick<
  CatalogConfigDiffExtended,
  "fieldsAdded" | "fieldsRemoved" | "fieldsDisabled" | "fieldsEnabled"
>;

const isFieldChange = (key: keyof CatalogConfigDiffExtended) =>
  key === "fieldsAdded" || key === "fieldsRemoved" || key === "fieldsDisabled" || key === "fieldsEnabled";

interface CatalogChangeEventItemProps {
  event: z.infer<typeof schemaConfigUpdateEventSchema>;
}

export const CatalogChangeEventItem: React.FC<CatalogChangeEventItemProps> = ({ event }) => {
  const { formatMessage } = useIntl();
  const { openModal } = useModalService();

  const catalogDiff = useMemo(
    () => transformCatalogDiffToCatalogConfigDiff(event.summary?.airbyteCatalogDiff?.catalogDiff),
    [event.summary?.airbyteCatalogDiff?.catalogDiff]
  );
  const catalogConfigDiff = event.summary?.airbyteCatalogDiff?.catalogConfigDiff;

  const mergedCatalogConfigDiff = useMemo(
    () => ({ ...catalogDiff, ...catalogConfigDiff }),
    [catalogConfigDiff, catalogDiff]
  );

  const schemaMessage = useMemo(() => {
    const parts = Object.entries(mergedCatalogConfigDiff)
      .filter(([_, value]) => Array.isArray(value) && value.length > 0)
      .map(([key]) =>
        formatMessage(
          { id: `connection.timeline.connection_schema_update.${key}` },
          {
            count: isFieldChange(key as keyof CatalogConfigDiffExtended)
              ? mergedCatalogConfigDiff[key as keyof FieldChangeType]?.reduce(
                  (acc, curr) => acc + (curr?.fields?.length ?? 0),
                  0
                )
              : mergedCatalogConfigDiff[key as keyof CatalogConfigDiffExtended]?.length ?? 0,
          }
        )
      );

    return <FormattedList value={parts} />;
  }, [mergedCatalogConfigDiff, formatMessage]);

  const triggerSchemaConfigChangesModal = () =>
    openModal<void>({
      title: (
        <FlexContainer alignItems="center">
          <ConnectionTimelineEventIcon icon="schema" size="lg" />
          <FlexContainer gap="xs" direction="column">
            <Text size="lg">
              <FormattedMessage id="connection.timeline.connection_schema_update" />
            </Text>
            <Text size="lg" color="grey400">
              {!!event.user ? <TimelineEventUser user={event.user} /> : <FormattedMessage id="general.airbyte" />}
              {event.createdAt && (
                <>
                  <span className={styles.dot} />
                  <FormattedDate value={event.createdAt * 1000} timeStyle="short" dateStyle="medium" />
                </>
              )}
            </Text>
          </FlexContainer>
        </FlexContainer>
      ),
      size: "md",
      testId: "schema-config-changes-modal",
      content: () => (
        <div className={styles.modalContent}>
          <CatalogConfigDiffModal catalogConfigDiff={mergedCatalogConfigDiff} />
        </div>
      ),
    });

  return (
    <ConnectionTimelineEventItem>
      <ConnectionTimelineEventIcon icon="schema" />
      <ConnectionTimelineEventSummary>
        <FlexContainer gap="xs" direction="column">
          <Text bold>
            <FormattedMessage id="connection.timeline.connection_schema_update" />
          </Text>
          <Text as="span" size="sm" color="grey400">
            <FormattedMessage
              id="connection.timeline.connection_schema_update.description"
              values={{
                user: !!event.user ? (
                  <TimelineEventUser user={event.user} />
                ) : (
                  <FormattedMessage id="general.airbyte" />
                ),
                schemaMessage,
              }}
            />{" "}
            <Button variant="link" onClick={triggerSchemaConfigChangesModal}>
              <FormattedMessage id="connection.timeline.connection_schema_update.viewDetails" />
            </Button>
          </Text>
        </FlexContainer>
      </ConnectionTimelineEventSummary>
      <ConnectionTimelineEventActions createdAt={event.createdAt} />
    </ConnectionTimelineEventItem>
  );
};
