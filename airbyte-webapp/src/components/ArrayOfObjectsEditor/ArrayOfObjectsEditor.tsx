import { closestCenter, DndContext, useSensor, useSensors, DragEndEvent } from "@dnd-kit/core";
import { SortableContext, sortableKeyboardCoordinates, verticalListSortingStrategy } from "@dnd-kit/sortable";
import React from "react";
import { FieldArrayWithId } from "react-hook-form";

import { KeyboardSensor, PointerSensor } from "components/connectorBuilder/Builder/dndSensors";
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
  onMove: (source: number, destination: number) => void;
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
  onMove,
}: ArrayOfObjectsEditorProps<T>) => {
  const sensors = useSensors(
    useSensor(PointerSensor),
    useSensor(KeyboardSensor, {
      coordinateGetter: sortableKeyboardCoordinates,
    })
  );

  const handleDragEnd = (event: DragEndEvent) => {
    const { active, over } = event;

    if (active.id !== over?.id) {
      const oldIndex = fields.findIndex((item) => item.id === active.id);
      const newIndex = fields.findIndex((item) => item.id === over?.id);
      return onMove(oldIndex, newIndex);
    }
  };

  return (
    <Box pb="xl">
      <EditorHeader
        mainTitle={mainTitle}
        itemsCount={fields.length}
        addButtonText={addButtonText}
        onAddItem={onAddItem}
      />
      <DndContext sensors={sensors} collisionDetection={closestCenter} onDragEnd={handleDragEnd}>
        <SortableContext items={fields} strategy={verticalListSortingStrategy}>
          <FlexContainer direction="column" gap="xs" className={styles.list}>
            {fields.map((field, index) => (
              <EditorRow
                key={field.id}
                id={field.id}
                index={index}
                name={renderItemName?.(field, index)}
                description={renderItemDescription?.(field, index)}
                onEdit={onStartEdit}
                onRemove={onRemove}
              />
            ))}
          </FlexContainer>
        </SortableContext>
      </DndContext>
    </Box>
  );
};
