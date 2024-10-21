import { closestCenter, DndContext, DragEndEvent, useSensor, useSensors } from "@dnd-kit/core";
import {
  arrayMove,
  SortableContext,
  sortableKeyboardCoordinates,
  useSortable,
  verticalListSortingStrategy,
} from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import React, { useMemo, useState } from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import { BuilderConfigView } from "./BuilderConfigView";
import { KeyboardSensor, PointerSensor } from "./dndSensors";
import { InputForm, InputInEditing, newInputInEditing } from "./InputsForm";
import styles from "./InputsView.module.scss";
import { BuilderFormInput, useBuilderWatch } from "../types";

const supportedTypes = ["string", "integer", "number", "array", "boolean", "enum", "unknown"] as const;

export const InputsView: React.FC = () => {
  const { formatMessage } = useIntl();
  const inputs = useBuilderWatch("formValues.inputs");
  const { setValue } = useFormContext();
  const { permission } = useConnectorBuilderFormState();
  const [inputInEditing, setInputInEditing] = useState<InputInEditing | undefined>(undefined);
  const sensors = useSensors(
    useSensor(PointerSensor),
    useSensor(KeyboardSensor, {
      coordinateGetter: sortableKeyboardCoordinates,
    })
  );

  const inputsWithIds = useMemo(() => inputs.map((input) => ({ input, id: input.key })), [inputs]);

  const handleDragEnd = (event: DragEndEvent) => {
    const { active, over } = event;

    if (over !== null && active.id !== over.id) {
      const oldIndex = inputs.findIndex((input) => input.key === active.id.toString());
      const newIndex = inputs.findIndex((input) => input.key === over.id.toString());
      setValue("formValues.inputs", arrayMove(inputs, oldIndex, newIndex));
    }
  };

  return (
    <fieldset className={styles.fieldset} disabled={permission === "readOnly"}>
      <BuilderConfigView heading={formatMessage({ id: "connectorBuilder.inputsTitle" })}>
        <Text align="center" className={styles.inputsDescription}>
          <FormattedMessage id="connectorBuilder.inputsDescription" />
        </Text>

        <DndContext sensors={sensors} collisionDetection={closestCenter} onDragEnd={handleDragEnd}>
          <SortableContext items={inputsWithIds} strategy={verticalListSortingStrategy}>
            {inputsWithIds.map((inputWithId) => (
              <SortableInput key={inputWithId.id} {...inputWithId} setInputInEditing={setInputInEditing} />
            ))}
          </SortableContext>
        </DndContext>

        <Button
          className={styles.addInputButton}
          onClick={() => {
            setInputInEditing(newInputInEditing());
          }}
          icon="plus"
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
    </fieldset>
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

function formInputToInputInEditing({ key, definition, required, isLocked }: BuilderFormInput): InputInEditing {
  return {
    key,
    previousKey: key,
    definition,
    required,
    isLocked,
    isNew: false,
    showDefaultValueField: definition.default !== undefined,
    type: getType(definition),
  };
}

interface SortableInputProps {
  input: BuilderFormInput;
  id: string;
  setInputInEditing: (inputInEditing: InputInEditing) => void;
}

const SortableInput: React.FC<SortableInputProps> = ({ input, id, setInputInEditing }) => {
  const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({ id });
  const { permission } = useConnectorBuilderFormState();
  const canEdit = permission !== "readOnly";

  const style = {
    // set x transform to 0 so that the inputs only move up and down
    transform: CSS.Transform.toString(transform ? { ...transform, x: 0 } : null),
    transition,
    zIndex: isDragging ? 1 : undefined,
  };

  return (
    <div ref={setNodeRef} style={style}>
      <Card bodyClassName={styles.inputCard} {...(canEdit ? attributes : {})} {...(canEdit ? listeners : {})}>
        {canEdit && <Icon type="drag" color="action" />}
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
            setInputInEditing(formInputToInputInEditing(input));
          }}
          data-no-dnd="true"
        >
          <Icon type="gear" color="action" />
        </Button>
      </Card>
    </div>
  );
};
