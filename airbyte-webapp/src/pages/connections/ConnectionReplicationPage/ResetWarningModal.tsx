import { useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { LabeledSwitch } from "components";
import { Button } from "components/ui/Button";
import { ModalBody, ModalFooter } from "components/ui/Modal";
import { Text } from "components/ui/Text";

import { ConnectionStateType } from "core/api/types/AirbyteClient";

interface ResetWarningModalProps {
  onClose: (withReset: boolean) => void;
  onCancel: () => void;
  stateType: ConnectionStateType;
}

export const ResetWarningModal: React.FC<ResetWarningModalProps> = ({ onCancel, onClose, stateType }) => {
  const { formatMessage } = useIntl();
  const [withReset, setWithReset] = useState(true);
  const requireFullReset = stateType === ConnectionStateType.legacy;

  return (
    <>
      <ModalBody>
        <Text>
          <FormattedMessage id="connection.streamResetHint" />
        </Text>
        <p>
          <LabeledSwitch
            name="reset"
            checked={withReset}
            onChange={(ev) => setWithReset(ev.target.checked)}
            label={formatMessage({
              id: requireFullReset ? "connection.saveWithFullReset" : "connection.saveWithReset",
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
        <Button onClick={() => onClose(withReset)} data-testid="resetModal-save">
          <FormattedMessage id="connection.save" />
        </Button>
      </ModalFooter>
    </>
  );
};
