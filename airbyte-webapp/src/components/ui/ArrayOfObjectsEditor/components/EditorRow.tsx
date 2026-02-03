import { useSortable } from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import React from "react";
import { useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import styles from "./EditorRow.module.scss";

interface EditorRowProps {
  id: string;
  index: number;
  name?: React.ReactNode;
  description?: React.ReactNode;
  onEdit: (id: number) => void;
  onRemove: (id: number) => void;
  disabled?: boolean;
}

export const EditorRow: React.FC<EditorRowProps> = ({ name, id, description, onEdit, onRemove, disabled, index }) => {
  const { formatMessage } = useIntl();
  const { setNodeRef, attributes, listeners, transform, transition, isDragging } = useSortable({ id });

  const body = (
    <FlexContainer justifyContent="space-between" alignItems="center" gap="xs" className={styles.body}>
      <FlexContainer alignItems="center">
        <Icon type="drag" color="action" className={styles.dragHandle} {...attributes} {...listeners} />
        <Text size="sm" className={styles.name}>
          {name || id}
        </Text>
      </FlexContainer>
      <FlexContainer gap="none">
        <Button
          size="xs"
          type="button"
          variant="clear"
          arial-label={formatMessage({ id: "form.edit" })}
          onClick={() => onEdit(index)}
          disabled={disabled}
          icon="pencil"
        />
        <Button
          size="xs"
          type="button"
          variant="clear"
          aria-label={formatMessage({ id: "form.delete" })}
          onClick={() => onRemove(index)}
          disabled={disabled}
          icon="cross"
        />
      </FlexContainer>
    </FlexContainer>
  );

  const style = {
    transform: CSS.Transform.toString(transform ? { ...transform, x: 0 } : null),
    transition,
    zIndex: isDragging ? 1 : undefined,
  };

  return (
    <div className={styles.container} style={style} ref={setNodeRef}>
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
