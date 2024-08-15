import isString from "lodash/isString";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useModalService } from "hooks/services/Modal";

import styles from "./ToggleUiModal.module.scss";
import { ManifestValidationErrorDisplay } from "../ManifestValidationErrorDisplay";
import { ManifestValidationError } from "../utils";

interface ToggleUiModalContentProps {
  error: string | ManifestValidationError;
  hasUiValue: boolean;
}

const ToggleUiModalContent: React.FC<ToggleUiModalContentProps> = ({ error, hasUiValue }) => (
  <div className={styles.content}>
    <FlexContainer direction="column" gap="lg">
      <Text>
        <FormattedMessage id="connectorBuilder.toggleModal.text.incompatibleYaml" />
      </Text>
      {isString(error) ? <Text bold>{error}</Text> : <ManifestValidationErrorDisplay error={error} />}
      <Text>
        <FormattedMessage
          id={
            hasUiValue
              ? "connectorBuilder.toggleModal.text.uiValueAvailable"
              : "connectorBuilder.toggleModal.text.uiValueUnavailable"
          }
        />
      </Text>
    </FlexContainer>
  </div>
);

export const useToggleUiModal = () => {
  const { openModal } = useModalService();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();

  const openNoUiValueModal = (error: ToggleUiModalContentProps["error"]) =>
    openModal({
      title: <FormattedMessage id="connectorBuilder.toggleModal.title" />,
      size: "md",
      content: () => (
        <Box p="lg">
          <ToggleUiModalContent error={error} hasUiValue={false} />
        </Box>
      ),
    });

  const openHasUiValueModal = (error: ToggleUiModalContentProps["error"], onSubmit: () => void) =>
    openConfirmationModal({
      title: "connectorBuilder.toggleModal.title",
      text: <ToggleUiModalContent error={error} hasUiValue />,
      submitButtonText: "connectorBuilder.toggleModal.submitButton",
      onSubmit: () => {
        onSubmit();
        closeConfirmationModal();
      },
    });

  return {
    openNoUiValueModal,
    openHasUiValueModal,
  };
};
