import React, { useCallback, useMemo, useRef, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { useListBuilderProjects, useSourceDefinitionList, useUpdateSourceDefinition, useSourceList } from "core/api";
import { SourceDefinitionRead } from "core/api/types/AirbyteClient";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { useNotificationService } from "hooks/services/Notification";

import ConnectorsView, { ConnectorsViewProps } from "./components/ConnectorsView";

const SourcesPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_SOURCE);

  const { formatMessage } = useIntl();
  const { sources } = useSourceList();
  const { sourceDefinitions } = useSourceDefinitionList();

  const { mutateAsync: updateSourceDefinition } = useUpdateSourceDefinition();
  const [updatingDefinitionId, setUpdatingDefinitionId] = useState<string>();

  const { registerNotification } = useNotificationService();

  const idToSourceDefinition = useMemo(
    () =>
      sourceDefinitions.reduce((map, sourceDefinition) => {
        map.set(sourceDefinition.sourceDefinitionId, sourceDefinition);
        return map;
      }, new Map<string, SourceDefinitionRead>()),
    [sourceDefinitions]
  );
  const definitionMap = useRef(idToSourceDefinition);
  definitionMap.current = idToSourceDefinition;

  const onUpdateVersion = useCallback(
    async ({ id, version }: { id: string; version: string }) => {
      try {
        setUpdatingDefinitionId(id);
        await updateSourceDefinition({
          sourceDefinitionId: id,
          dockerImageTag: version,
        });
        registerNotification({
          id: `source.update.success.${id}.${version}`,
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
          id: `source.update.error.${id}.${version}`,
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
    [formatMessage, registerNotification, updateSourceDefinition]
  );

  const usedSourceDefinitions: SourceDefinitionRead[] = useMemo(() => {
    const usedSourceDefinitionIds = new Set<string>(sources.map((source) => source.sourceDefinitionId));
    return sourceDefinitions
      .filter((sourceDefinition) => usedSourceDefinitionIds.has(sourceDefinition.sourceDefinitionId))
      .sort((a, b) => a.name.localeCompare(b.name));
  }, [sourceDefinitions, sources]);

  const ConnectorsViewComponent = WithBuilderProjects;

  return (
    <ConnectorsViewComponent
      type="sources"
      updatingDefinitionId={updatingDefinitionId}
      usedConnectorsDefinitions={usedSourceDefinitions}
      connectorsDefinitions={sourceDefinitions}
      onUpdateVersion={onUpdateVersion}
    />
  );
};

export const WithBuilderProjects: React.FC<Omit<ConnectorsViewProps, "connectorBuilderProjects">> = (props) => {
  const projects = useListBuilderProjects();
  return <ConnectorsView {...props} connectorBuilderProjects={projects} />;
};

export default SourcesPage;
