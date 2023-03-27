import React, { useCallback, useMemo, useRef, useState } from "react";
import { useIntl } from "react-intl";

import { DestinationDefinitionRead } from "core/request/AirbyteClient";
import { useTrackPage, PageTrackingCodes } from "hooks/services/Analytics";
import {
  useDestinationDefinitionList,
  useUpdateDestinationDefinition,
} from "services/connector/DestinationDefinitionService";

import ConnectorsView from "./components/ConnectorsView";
import { useDestinationList } from "../../../../hooks/services/useDestinationHook";

const DestinationsPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_DESTINATION);

  const { formatMessage } = useIntl();
  const { destinationDefinitions } = useDestinationDefinitionList();
  const { destinations } = useDestinationList();

  const [feedbackList, setFeedbackList] = useState<Record<string, string>>({});
  const feedbackListRef = useRef(feedbackList);
  feedbackListRef.current = feedbackList;

  const { mutateAsync: updateDestinationDefinition } = useUpdateDestinationDefinition();
  const [updatingDefinitionId, setUpdatingDefinitionId] = useState<string>();

  const onUpdateVersion = useCallback(
    async ({ id, version }: { id: string; version: string }) => {
      try {
        setUpdatingDefinitionId(id);
        await updateDestinationDefinition({
          destinationDefinitionId: id,
          dockerImageTag: version,
        });
        setFeedbackList({ ...feedbackListRef.current, [id]: "success" });
      } catch (e) {
        const messageId = e.status === 422 ? "form.imageCannotFound" : "form.someError";
        setFeedbackList({
          ...feedbackListRef.current,
          [id]: formatMessage({ id: messageId }),
        });
      } finally {
        setUpdatingDefinitionId(undefined);
      }
    },
    [formatMessage, updateDestinationDefinition]
  );

  const usedDestinationDefinitions = useMemo<DestinationDefinitionRead[]>(() => {
    const destinationDefinitionMap = new Map<string, DestinationDefinitionRead>();
    destinations.forEach((destination) => {
      const destinationDefinition = destinationDefinitions.find(
        (destinationDefinition) => destinationDefinition.destinationDefinitionId === destination.destinationDefinitionId
      );

      if (destinationDefinition) {
        destinationDefinitionMap.set(destinationDefinition.destinationDefinitionId, destinationDefinition);
      }
    });

    return Array.from(destinationDefinitionMap.values());
  }, [destinations, destinationDefinitions]);

  return (
    <ConnectorsView
      type="destinations"
      onUpdateVersion={onUpdateVersion}
      usedConnectorsDefinitions={usedDestinationDefinitions}
      connectorsDefinitions={destinationDefinitions}
      updatingDefinitionId={updatingDefinitionId}
      feedbackList={feedbackList}
    />
  );
};

export default DestinationsPage;
