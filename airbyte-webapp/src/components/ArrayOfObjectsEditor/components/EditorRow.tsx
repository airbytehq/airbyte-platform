import React from "react";
import { useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import styles from "./EditorRow.module.scss";

interface EditorRowProps {
  name?: React.ReactNode;
  description?: React.ReactNode;
  id: number;
  onEdit: (id: number) => void;
  onRemove: (id: number) => void;
  disabled?: boolean;
}

export const EditorRow: React.FC<EditorRowProps> = ({ name, id, description, onEdit, onRemove, disabled }) => {
  const { formatMessage } = useIntl();

  const body = (
    <FlexContainer justifyContent="space-between" alignItems="center" gap="xs" className={styles.body}>
      <Text size="sm" className={styles.name}>
        {name || id}
      </Text>
      <FlexContainer gap="none">
        <Button
          size="xs"
          type="button"
          variant="clear"
          arial-label={formatMessage({ id: "form.edit" })}
          onClick={() => onEdit(id)}
          disabled={disabled}
          icon={<Icon type="pencil" />}
        />
        <Button
          size="xs"
          type="button"
          variant="clear"
          aria-label={formatMessage({ id: "form.delete" })}
          onClick={() => onRemove(id)}
          disabled={disabled}
          icon={<Icon type="cross" />}
        />
      </FlexContainer>
    </FlexContainer>
  );

  return (
    <div className={styles.container}>
      {description ? (
        <Tooltip control={body} placement="top" containerClassName={styles.tooltipContainer}>
          {description}
        </Tooltip>
      ) : (
        body
      )}
    </div>
  );
};
