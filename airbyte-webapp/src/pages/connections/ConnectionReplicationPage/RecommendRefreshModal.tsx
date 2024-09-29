import { useState } from "react";
import { FormattedMessage } from "react-intl";

import { RadioButtonTiles } from "components/connection/CreateConnection/RadioButtonTiles";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { ModalBody, ModalFooter } from "components/ui/Modal";
import { Text } from "components/ui/Text";

interface RecommendedRefreshWarningProps {
  onCancel: () => void;
  onComplete: (withRefresh: boolean) => void;
}
export const RecommendRefreshModal: React.FC<RecommendedRefreshWarningProps> = ({ onCancel, onComplete }) => {
  const [shouldRefresh, setShouldRefresh] = useState("saveWithRefresh");

  return (
    <>
      <ModalBody>
        <Text>
          <FormattedMessage id="connection.refreshDataHint" />
        </Text>
        <Box pt="lg">
          <RadioButtonTiles
            light
            direction="column"
            options={[
              {
                value: "saveWithRefresh",
                label: <FormattedMessage id="connection.saveWithRefresh" />,
                description: "",
                "data-testid": "resetModal-reset-checkbox",
              },
              {
                value: "saveWithoutRefresh",
                label: <FormattedMessage id="connection.saveWithoutRefresh" />,
                description: (
                  <Text color="grey400" italicized>
                    <FormattedMessage id="connection.saveWithoutRefresh.description" />
                  </Text>
                ),
              },
            ]}
            selectedValue={shouldRefresh}
            onSelectRadioButton={(value) => {
              setShouldRefresh(value);
            }}
            name="shouldRefresh"
          />
        </Box>
      </ModalBody>
      <ModalFooter>
        <Button onClick={onCancel} variant="secondary" data-testid="refreshModal-cancel">
          <FormattedMessage id="form.cancel" />
        </Button>
        <Button onClick={() => onComplete(shouldRefresh === "saveWithRefresh")} data-testid="refreshModal-save">
          <FormattedMessage id="connection.save" />
        </Button>
      </ModalFooter>
    </>
  );
};
