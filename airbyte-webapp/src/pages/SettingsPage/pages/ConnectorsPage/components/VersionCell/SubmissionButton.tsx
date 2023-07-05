import React from "react";
import { useFormState } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";

import { DEV_IMAGE_TAG } from "core/domain/connector/constants";

import { VersionCellProps } from "./VersionCell";
import { useUpdatingState } from "../ConnectorsViewContext";

type SubmissionButtonProps = Omit<VersionCellProps, "onChange">;

export const SubmissionButton: React.FC<SubmissionButtonProps> = ({
  connectorDefinitionId,
  currentVersion,
  latestVersion,
}) => {
  const { isDirty, isSubmitting } = useFormState();
  const { updatingDefinitionId, updatingAll } = useUpdatingState();
  const isConnectorUpdatable = currentVersion !== latestVersion || currentVersion === DEV_IMAGE_TAG;
  const updatingCurrent = connectorDefinitionId === updatingDefinitionId;

  return (
    <Button
      type="submit"
      size="xs"
      isLoading={(updatingAll && isConnectorUpdatable) || updatingCurrent}
      disabled={(isSubmitting || !isDirty) && !isConnectorUpdatable}
    >
      <FormattedMessage id="form.change" />
    </Button>
  );
};
