import { useState } from "react";
import { FormattedMessage } from "react-intl";

import { RadioButtonTiles } from "components/connection/CreateConnection/RadioButtonTiles";
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

export const ClearDataWarningModal: React.FC<ResetWarningModalProps> = ({ onCancel, onComplete }) => {
  const [shouldClear, setShouldClear] = useState("true");

  return (
    <>
      <ModalBody>
        <Text size="lg">
          <FormattedMessage id="connection.clearDataHint" />
        </Text>
        <Box pt="lg">
          <RadioButtonTiles
            light
            direction="column"
            options={[
              {
                value: "saveWithClear",
                label: <FormattedMessage id="connection.saveWithClear" />,
                description: "",
              },
              {
                value: "saveWithoutClear",
                label: <FormattedMessage id="connection.saveWithoutClear" />,
                description: (
                  <Text color="grey400" italicized>
                    <FormattedMessage id="connection.saveWithoutClear.description" />
                  </Text>
                ),
              },
            ]}
            selectedValue={shouldClear}
            onSelectRadioButton={(value) => {
              setShouldClear(value);
            }}
            name="shouldClear"
            data-testid="resetModal-reset-checkbox"
          />
        </Box>
      </ModalBody>
      <ModalFooter>
        <Button onClick={onCancel} variant="secondary" data-testid="resetModal-cancel">
          <FormattedMessage id="form.cancel" />
        </Button>
        <Button onClick={() => onComplete(shouldClear === "saveWithClear")} data-testid="resetModal-save">
          <FormattedMessage id="form.submit" />
        </Button>
      </ModalFooter>
    </>
  );
};
