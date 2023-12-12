import React from "react";
import { FieldArrayWithId } from "react-hook-form";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";

import styles from "./ArrayOfObjectsEditor.module.scss";
import { EditorHeader } from "./components/EditorHeader";
import { EditorRow } from "./components/EditorRow";

export interface ArrayOfObjectsEditorProps<T> {
  fields: T[];
  mainTitle?: React.ReactNode;
  addButtonText?: React.ReactNode;
  renderItemName?: (item: T, index: number) => React.ReactNode | undefined;
  renderItemDescription?: (item: T, index: number) => React.ReactNode | undefined;
  onAddItem: () => void;
  onStartEdit: (n: number) => void;
  onRemove: (index: number) => void;
}

/**
 * The component is used to render a list of react-hook-form FieldArray items with the ability to add, edit and remove items.
 * @param fields
 * @param mainTitle
 * @param addButtonText
 * @param onAddItem
 * @param renderItemName
 * @param renderItemDescription
 * @param onStartEdit
 * @param onRemove
 * @param mode
 */
export const ArrayOfObjectsEditor = <T extends FieldArrayWithId>({
  fields,
  mainTitle,
  addButtonText,
  onAddItem,
  renderItemName,
  renderItemDescription,
  onStartEdit,
  onRemove,
}: ArrayOfObjectsEditorProps<T>) => (
  <Box mb="xl">
    <EditorHeader
      mainTitle={mainTitle}
      itemsCount={fields.length}
      addButtonText={addButtonText}
      onAddItem={onAddItem}
    />
    {fields.length ? (
      <FlexContainer direction="column" gap="xs" className={styles.list}>
        {fields.map((field, index) => (
          <EditorRow
            key={field.id}
            name={renderItemName?.(field, index)}
            description={renderItemDescription?.(field, index)}
            id={index}
            onEdit={onStartEdit}
            onRemove={onRemove}
          />
        ))}
      </FlexContainer>
    ) : null}
  </Box>
);
