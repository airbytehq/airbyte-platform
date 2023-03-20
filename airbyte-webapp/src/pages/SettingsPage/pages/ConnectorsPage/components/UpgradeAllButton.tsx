import { faRedoAlt } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import React from "react";
import { FormattedMessage } from "react-intl";
import styled from "styled-components";

import { Button } from "components/ui/Button";

import { useGetConnectorsOutOfDate, useUpdateAllConnectors } from "hooks/services/useConnector";

import { useUpdatingState } from "./ConnectorsViewContext";
import styles from "./UpgradeAllButton.module.scss";

const TryArrow = styled(FontAwesomeIcon)`
  margin: 0 10px -1px 0;
  font-size: 14px;
`;

const UpdateButtonContent = styled.div`
  position: relative;
  display: inline-block;
  margin-left: 5px;
`;

const ErrorBlock = styled.div`
  color: ${({ theme }) => theme.dangerColor};
  font-size: 11px;
  position: absolute;
  font-weight: normal;
  bottom: -17px;
  line-height: 11px;
  right: 0;
  left: -46px;
`;

interface UpdateAllButtonProps {
  connectorType: "sources" | "destinations";
}

const UpgradeAllButton: React.FC<UpdateAllButtonProps> = ({ connectorType }) => {
  const { setUpdatingAll } = useUpdatingState();
  const { hasNewSourceVersion, hasNewDestinationVersion } = useGetConnectorsOutOfDate();
  const hasNewVersion = connectorType === "sources" ? hasNewSourceVersion : hasNewDestinationVersion;

  const { mutateAsync, isError, isLoading, isSuccess } = useUpdateAllConnectors(connectorType);

  const handleUpdateAllConnectors = async () => {
    // Since we want to display the loading state on each connector row that is being updated, we "share" the react-query loading state via context here
    setUpdatingAll(true);
    await mutateAsync();
    setUpdatingAll(false);
  };

  return (
    <UpdateButtonContent>
      {isError && (
        <ErrorBlock>
          <FormattedMessage id="form.someError" />
        </ErrorBlock>
      )}
      <Button
        size="xs"
        className={styles.updateButton}
        onClick={handleUpdateAllConnectors}
        isLoading={isLoading}
        disabled={!hasNewVersion}
        icon={isSuccess ? undefined : <TryArrow icon={faRedoAlt} />}
      >
        {isSuccess ? <FormattedMessage id="admin.upgraded" /> : <FormattedMessage id="admin.upgradeAll" />}
      </Button>
    </UpdateButtonContent>
  );
};

export default UpgradeAllButton;
