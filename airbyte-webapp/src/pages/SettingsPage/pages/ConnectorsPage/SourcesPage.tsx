import React, { useCallback, useMemo, useRef, useState } from "react";
import { useIntl } from "react-intl";

import { SourceDefinitionRead } from "core/request/AirbyteClient";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { useAvailableSourceDefinitions } from "hooks/domain/connector/useAvailableSourceDefinitions";
import { useExperiment } from "hooks/services/Experiment";
import { useSourceList } from "hooks/services/useSourceHook";
import { useUpdateSourceDefinition } from "services/connector/SourceDefinitionService";
import { useListProjects } from "services/connectorBuilder/ConnectorBuilderProjectsService";

import ConnectorsView, { ConnectorsViewProps } from "./components/ConnectorsView";

const SourcesPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_SOURCE);

  const [feedbackList, setFeedbackList] = useState<Record<string, string>>({});
  const feedbackListRef = useRef(feedbackList);
  feedbackListRef.current = feedbackList;

  const { formatMessage } = useIntl();
  const { sources } = useSourceList();
  const sourceDefinitions = useAvailableSourceDefinitions();

  const showBuilderNavigationLinks = useExperiment("connectorBuilder.showNavigationLinks", false);

  const { mutateAsync: updateSourceDefinition } = useUpdateSourceDefinition();
  const [updatingDefinitionId, setUpdatingDefinitionId] = useState<string>();

  const onUpdateVersion = useCallback(
    async ({ id, version }: { id: string; version: string }) => {
      try {
        setUpdatingDefinitionId(id);
        await updateSourceDefinition({
          sourceDefinitionId: id,
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
    [formatMessage, updateSourceDefinition]
  );

  const usedSourcesDefinitions: SourceDefinitionRead[] = useMemo(() => {
    const sourceDefinitionMap = new Map<string, SourceDefinitionRead>();
    sources.forEach((source) => {
      const sourceDefinition = sourceDefinitions.find(
        (sourceDefinition) => sourceDefinition.sourceDefinitionId === source.sourceDefinitionId
      );

      if (sourceDefinition) {
        sourceDefinitionMap.set(source.sourceDefinitionId, sourceDefinition);
      }
    });

    return Array.from(sourceDefinitionMap.values()).sort((a, b) => a.name.localeCompare(b.name));
  }, [sources, sourceDefinitions]);

  const ConnectorsViewComponent = showBuilderNavigationLinks ? WithBuilderProjects : ConnectorsView;

  return (
    <ConnectorsViewComponent
      type="sources"
      updatingDefinitionId={updatingDefinitionId}
      usedConnectorsDefinitions={usedSourcesDefinitions}
      connectorsDefinitions={sourceDefinitions}
      feedbackList={feedbackList}
      onUpdateVersion={onUpdateVersion}
    />
  );
};

export const WithBuilderProjects: React.FC<Omit<ConnectorsViewProps, "connectorBuilderProjects">> = (props) => {
  const projects = useListProjects();
  return <ConnectorsView {...props} connectorBuilderProjects={projects} />;
};

export default SourcesPage;
