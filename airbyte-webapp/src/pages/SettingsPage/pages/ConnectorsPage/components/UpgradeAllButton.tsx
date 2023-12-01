import React from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { Icon } from "components/ui/Icon";

import { useGetConnectorsOutOfDate, useUpdateAllConnectors } from "hooks/services/useConnector";

import { useUpdatingState } from "./ConnectorsViewContext";

interface UpdateAllButtonProps {
  connectorType: "sources" | "destinations";
}

const UpgradeAllButton: React.FC<UpdateAllButtonProps> = ({ connectorType }) => {
  const { setUpdatingAll } = useUpdatingState();
  const { hasNewSourceVersion, hasNewDestinationVersion } = useGetConnectorsOutOfDate();
  const hasNewVersion = connectorType === "sources" ? hasNewSourceVersion : hasNewDestinationVersion;

  const { mutateAsync, isLoading } = useUpdateAllConnectors(connectorType);

  const handleUpdateAllConnectors = async () => {
    // Since we want to display the loading state on each connector row that is being updated, we "share" the react-query loading state via context here
    try {
      setUpdatingAll(true);
      await mutateAsync();
    } finally {
      setUpdatingAll(false);
    }
  };

  return (
    <Button
      size="xs"
      onClick={handleUpdateAllConnectors}
      isLoading={isLoading}
      disabled={!hasNewVersion}
      icon={<Icon type="reset" />}
    >
      <FormattedMessage id="admin.upgradeAll" />
    </Button>
  );
};

export default UpgradeAllButton;
