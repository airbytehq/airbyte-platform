import { useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { LabeledSwitch } from "components";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { ModalBody, ModalFooter } from "components/ui/Modal";
import { Text } from "components/ui/Text";

import { ConnectionStateType } from "core/api/types/AirbyteClient";

interface ResetWarningModalProps {
  onComplete: (withReset: boolean) => void;
  onCancel: () => void;
  stateType: ConnectionStateType;
}

export const ClearDataWarningModal: React.FC<ResetWarningModalProps> = ({ onCancel, onComplete, stateType }) => {
  const { formatMessage } = useIntl();
  const [withReset, setWithReset] = useState(true);
  const requireFullReset = stateType === ConnectionStateType.legacy;

  const checkboxLabel = requireFullReset ? "connection.saveWithFullDataClear" : "connection.saveWithDataClear";

  return (
    <>
      <ModalBody>
        <Text>
          <FormattedMessage id="connection.clearDataHint" />
        </Text>
        <Box pt="md">
          <Text italicized>
            <FormattedMessage id="connection.clearDataHint.emphasized" />
          </Text>
        </Box>

        <p>
          <LabeledSwitch
            name="reset"
            checked={withReset}
            onChange={(ev) => setWithReset(ev.target.checked)}
            label={formatMessage({
              id: checkboxLabel,
            })}
            checkbox
            data-testid="resetModal-reset-checkbox"
          />
        </p>
      </ModalBody>
      <ModalFooter>
        <Button onClick={onCancel} variant="secondary" data-testid="resetModal-cancel">
          <FormattedMessage id="form.cancel" />
        </Button>
        <Button onClick={() => onComplete(withReset)} data-testid="resetModal-save">
          <FormattedMessage id="connection.save" />
        </Button>
      </ModalFooter>
    </>
  );
};
