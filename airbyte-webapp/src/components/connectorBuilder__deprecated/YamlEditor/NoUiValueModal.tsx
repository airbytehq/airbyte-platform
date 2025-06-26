import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Text } from "components/ui/Text";

import { useModalService } from "hooks/services/Modal";

import styles from "./NoUiValueModal.module.scss";

interface NoUiValueModalProps {
  errorMessage: string;
}

export const NoUiValueModal: React.FC<NoUiValueModalProps> = ({ errorMessage }) => (
  <Box p="lg">
    <Text className={styles.content}>
      <FormattedMessage id="connectorBuilder.toggleModal.text.uiValueUnavailable" values={{ error: errorMessage }} />
    </Text>
  </Box>
);

export const useNoUiValueModal = () => {
  const { openModal } = useModalService();

  const openNoUiValueModal = (errorMessage: string) =>
    openModal({
      title: <FormattedMessage id="connectorBuilder.toggleModal.title" />,
      size: "md",
      content: () => <NoUiValueModal errorMessage={errorMessage} />,
    });

  return openNoUiValueModal;
};
