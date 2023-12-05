import { useMemo } from "react";

import Indicator from "components/Indicator";

import { useLatestDestinationDefinitionList } from "core/api";

interface DestinationUpdateIndicatorProps {
  id: string;
  currentVersion: string;
}

export const DestinationUpdateIndicator: React.FC<DestinationUpdateIndicatorProps> = ({ id, currentVersion }) => {
  const { destinationDefinitions } = useLatestDestinationDefinitionList();

  const isHidden = useMemo(
    () =>
      destinationDefinitions.find((definition) => definition.destinationDefinitionId === id)?.dockerImageTag ===
      currentVersion,
    [destinationDefinitions, id, currentVersion]
  );

  return <Indicator hidden={isHidden} />;
};
