import React from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { useDeleteModal } from "hooks/useDeleteModal";

import styles from "./DeleteBlock.module.scss";

interface IProps {
  type: "source" | "destination" | "connection";
  onDelete: () => Promise<unknown>;
  modalAdditionalContent?: React.ReactNode;
}

export const DeleteBlock: React.FC<IProps> = ({ type, onDelete }) => {
  const onDeleteButtonClick = useDeleteModal(type, onDelete);

  return (
    <Card className={styles.deleteBlock}>
      <FlexContainer direction="column" className={styles.text}>
        <Text size="lg">
          <FormattedMessage id={`tables.${type}Delete.title`} />
        </Text>
        <Text size="xs" color="grey">
          <FormattedMessage id={`tables.${type}DataDelete`} />
        </Text>
      </FlexContainer>
      <Button variant="danger" onClick={onDeleteButtonClick} data-id="open-delete-modal">
        <FormattedMessage id={`tables.${type}Delete`} />
      </Button>
    </Card>
  );
};
