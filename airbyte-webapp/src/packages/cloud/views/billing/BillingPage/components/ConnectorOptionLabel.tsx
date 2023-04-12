import { ConnectorIcon } from "components/common/ConnectorIcon";
import { ReleaseStageBadge } from "components/ReleaseStageBadge";
import { FlexContainer } from "components/ui/Flex";

import { AvailableDestination, AvailableSource } from "./CreditsUsageContext";

interface ConnectorOptionLabelProps {
  connector: AvailableSource | AvailableDestination;
}

export const ConnectorOptionLabel = ({ connector }: ConnectorOptionLabelProps) => (
  <FlexContainer alignItems="center" justifyContent="space-between">
    <ConnectorIcon icon={connector.icon} />
    {connector.name}
    <ReleaseStageBadge stage={connector.releaseStage} />
  </FlexContainer>
);
