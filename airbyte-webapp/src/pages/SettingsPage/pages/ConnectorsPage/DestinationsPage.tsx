import React, { useCallback, useMemo, useRef, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { useDestinationDefinitionList, useUpdateDestinationDefinition, useDestinationList } from "core/api";
import { DestinationDefinitionRead } from "core/api/types/AirbyteClient";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { useNotificationService } from "hooks/services/Notification";

import ConnectorsView from "./components/ConnectorsView";

const DestinationsPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_DESTINATION);

  const { formatMessage } = useIntl();
  const { destinationDefinitions } = useDestinationDefinitionList();
  const { destinations } = useDestinationList();

  const { mutateAsync: updateDestinationDefinition } = useUpdateDestinationDefinition();
  const [updatingDefinitionId, setUpdatingDefinitionId] = useState<string>();

  const { registerNotification } = useNotificationService();

  const idToDestinationDefinition = useMemo(
    () =>
      destinationDefinitions.reduce((map, destinationDefinition) => {
        map.set(destinationDefinition.destinationDefinitionId, destinationDefinition);
        return map;
      }, new Map<string, DestinationDefinitionRead>()),
    [destinationDefinitions]
  );
  const definitionMap = useRef(idToDestinationDefinition);
  definitionMap.current = idToDestinationDefinition;

  const onUpdateVersion = useCallback(
    async ({ id, version }: { id: string; version: string }) => {
      try {
        setUpdatingDefinitionId(id);
        await updateDestinationDefinition({
          destinationDefinitionId: id,
          dockerImageTag: version,
        });
        registerNotification({
          id: `destination.update.success.${id}.${version}`,
          text: (
            <FormattedMessage
              id="admin.upgradeConnector.success"
              values={{ name: definitionMap.current.get(id)?.name, version }}
            />
          ),
          type: "success",
        });
      } catch (error) {
        registerNotification({
          id: `destination.update.error.${id}.${version}`,
          text:
            formatMessage(
              { id: "admin.upgradeConnector.error" },
              { name: definitionMap.current.get(id)?.name, version }
            ) + (error.message ? `: ${error.message}` : ""),
          type: "error",
        });
      } finally {
        setUpdatingDefinitionId(undefined);
      }
    },
    [formatMessage, registerNotification, updateDestinationDefinition]
  );

  const usedDestinationDefinitions: DestinationDefinitionRead[] = useMemo(() => {
    const usedDestinationDefinitionIds = new Set<string>(
      destinations.map((destination) => destination.destinationDefinitionId)
    );
    return destinationDefinitions
      .filter((destinationDefinition) =>
        usedDestinationDefinitionIds.has(destinationDefinition.destinationDefinitionId)
      )
      .sort((a, b) => a.name.localeCompare(b.name));
  }, [destinationDefinitions, destinations]);

  return (
    <ConnectorsView
      type="destinations"
      onUpdateVersion={onUpdateVersion}
      usedConnectorsDefinitions={usedDestinationDefinitions}
      connectorsDefinitions={destinationDefinitions}
      updatingDefinitionId={updatingDefinitionId}
    />
  );
};

export default DestinationsPage;
