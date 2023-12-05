import { DndContext, closestCenter, useSensor, useSensors, DragEndEvent } from "@dnd-kit/core";
import {
  arrayMove,
  SortableContext,
  sortableKeyboardCoordinates,
  verticalListSortingStrategy,
  useSortable,
} from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import React, { useMemo, useState } from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { BuilderConfigView } from "./BuilderConfigView";
import { KeyboardSensor, PointerSensor } from "./dndSensors";
import DragHandleIcon from "./drag-handle.svg?react";
import { InputForm, InputInEditing, newInputInEditing } from "./InputsForm";
import styles from "./InputsView.module.scss";
import { BuilderFormInput, orderInputs, useBuilderWatch } from "../types";
import { useInferredInputs } from "../useInferredInputs";

const supportedTypes = ["string", "integer", "number", "array", "boolean", "enum", "unknown"] as const;

export const InputsView: React.FC = () => {
  const { formatMessage } = useIntl();
  const inputs = useBuilderWatch("formValues.inputs");
  const storedInputOrder = useBuilderWatch("formValues.inputOrder");
  const { setValue } = useFormContext();
  const [inputInEditing, setInputInEditing] = useState<InputInEditing | undefined>(undefined);
  const inferredInputs = useInferredInputs();
  const sensors = useSensors(
    useSensor(PointerSensor),
    useSensor(KeyboardSensor, {
      coordinateGetter: sortableKeyboardCoordinates,
    })
  );

  const { orderedInputs, inputOrder } = useMemo(() => {
    const orderedInputs = orderInputs(inputs, inferredInputs, storedInputOrder);
    const inputOrder = orderedInputs.map((input) => input.id);
    return { orderedInputs, inputOrder };
  }, [inferredInputs, storedInputOrder, inputs]);

  const handleDragEnd = (event: DragEndEvent) => {
    const { active, over } = event;

    if (over !== null && active.id !== over.id) {
      const oldIndex = inputOrder.indexOf(active.id.toString());
      const newIndex = inputOrder.indexOf(over.id.toString());
      setValue("formValues.inputOrder", arrayMove(inputOrder, oldIndex, newIndex));
    }
  };

  return (
    <BuilderConfigView heading={formatMessage({ id: "connectorBuilder.inputsTitle" })}>
      <Text align="center" className={styles.inputsDescription}>
        <FormattedMessage id="connectorBuilder.inputsDescription" />
      </Text>

      <DndContext sensors={sensors} collisionDetection={closestCenter} onDragEnd={handleDragEnd}>
        <SortableContext items={orderedInputs} strategy={verticalListSortingStrategy}>
          {orderedInputs.map((input) => (
            <SortableInput key={input.id} {...input} setInputInEditing={setInputInEditing} />
          ))}
        </SortableContext>
      </DndContext>

      <Button
        className={styles.addInputButton}
        onClick={() => {
          setInputInEditing(newInputInEditing());
        }}
        icon={<Icon type="plus" />}
        iconPosition="left"
        variant="secondary"
        type="button"
        data-no-dnd="true"
      >
        <FormattedMessage id="connectorBuilder.addInputButton" />
      </Button>

      {inputInEditing && (
        <InputForm
          inputInEditing={inputInEditing}
          onClose={() => {
            setInputInEditing(undefined);
          }}
        />
      )}
    </BuilderConfigView>
  );
};

// Return user input type for a given schema definition
function getType(definition: BuilderFormInput["definition"]): InputInEditing["type"] {
  if (definition.format === "date") {
    return "date";
  }
  if (definition.format === "date-time") {
    return "date-time";
  }
  const supportedType = supportedTypes.find((type) => type === definition.type) || "unknown";
  if (supportedType !== "unknown" && definition.enum) {
    return "enum";
  }
  return supportedType;
}

function formInputToInputInEditing(
  { key, definition, required }: BuilderFormInput,
  isInferredInputOverride: boolean
): InputInEditing {
  return {
    key,
    previousKey: key,
    definition,
    required,
    isNew: false,
    showDefaultValueField: Boolean(definition.default),
    type: getType(definition),
    isInferredInputOverride,
  };
}

interface SortableInputProps {
  input: BuilderFormInput;
  isInferred: boolean;
  id: string;
  setInputInEditing: (inputInEditing: InputInEditing) => void;
}

const SortableInput: React.FC<SortableInputProps> = ({ input, isInferred, id, setInputInEditing }) => {
  const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({ id });

  const style = {
    // set x transform to 0 so that the inputs only move up and down
    transform: CSS.Transform.toString(transform ? { ...transform, x: 0 } : null),
    transition,
    zIndex: isDragging ? 1 : undefined,
  };

  return (
    <div ref={setNodeRef} style={style}>
      <Card className={styles.inputCard} {...attributes} {...listeners}>
        <DragHandleIcon className={styles.dragHandle} />
        <Text size="lg" className={styles.itemLabel}>
          {input.definition.title || input.key}
        </Text>
        <Button
          className={styles.itemButton}
          size="sm"
          variant="secondary"
          aria-label="Edit"
          type="button"
          onClick={() => {
            setInputInEditing(formInputToInputInEditing(input, isInferred));
          }}
          data-no-dnd="true"
        >
          <Icon type="gear" className={styles.icon} />
        </Button>
      </Card>
    </div>
  );
};
