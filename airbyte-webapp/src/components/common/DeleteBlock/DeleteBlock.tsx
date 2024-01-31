import React from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useDeleteModal } from "hooks/useDeleteModal";

import styles from "./DeleteBlock.module.scss";

interface DeleteBlockProps {
  type: "source" | "destination" | "connection";
  onDelete: () => Promise<unknown>;
  modalAdditionalContent?: React.ReactNode;
}

export const DeleteBlock: React.FC<DeleteBlockProps> = ({ type, onDelete }) => {
  const { mode } = useConnectionFormService();
  const onDeleteButtonClick = useDeleteModal(type, onDelete);

  return (
    <Card>
      <FlexContainer direction="row" justifyContent="space-between" alignItems="center" className={styles.text}>
        <FlexContainer direction="column">
          <Text size="lg">
            <FormattedMessage id={`tables.${type}Delete.title`} />
          </Text>
          <Text size="xs" color="grey">
            <FormattedMessage id={`tables.${type}DataDelete`} />
          </Text>
        </FlexContainer>
        <Button
          variant="danger"
          onClick={onDeleteButtonClick}
          data-id="open-delete-modal"
          disabled={mode === "readonly"}
        >
          <FormattedMessage id={`tables.${type}Delete`} />
        </Button>
      </FlexContainer>
    </Card>
  );
};
