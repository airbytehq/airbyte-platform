import { closestCenter, DndContext, DragEndEvent, useSensor, useSensors } from "@dnd-kit/core";
import {
  arrayMove,
  SortableContext,
  sortableKeyboardCoordinates,
  useSortable,
  verticalListSortingStrategy,
} from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import classNames from "classnames";
import React, { useMemo, useState, useCallback } from "react";
import { useFormContext } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { FormControl } from "components/forms";
import { FormControlErrorMessage, FormControlFooter } from "components/forms/FormControl";
import { ControlLabels } from "components/LabeledControl";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Message } from "components/ui/Message";
import { Text } from "components/ui/Text";

import { Spec, SpecConnectionSpecification } from "core/api/types/ConnectorManifest";
import {
  useConnectorBuilderFormState,
  useConnectorBuilderPermission,
} from "services/connectorBuilder/ConnectorBuilderStateService";

import { BuilderConfigView } from "./BuilderConfigView";
import { KeyboardSensor, PointerSensor } from "./dndSensors";
import { InputForm, InputInEditing, newInputInEditing, supportedTypes } from "./InputsForm";
import styles from "./InputsView.module.scss";
import { SecretField } from "./SecretField";
import { BuilderFormInput } from "../types";
import { useBuilderWatch } from "../useBuilderWatch";

export const InputsView: React.FC = () => {
  const { formatMessage } = useIntl();
  const spec = useBuilderWatch("manifest.spec");
  const inputs = useMemo(() => convertToBuilderFormInputs(spec), [spec]);
  const { setValue } = useFormContext();
  const permission = useConnectorBuilderPermission();
  const [inputInEditing, setInputInEditing] = useState<InputInEditing | undefined>(undefined);
  const sensors = useSensors(
    useSensor(PointerSensor),
    useSensor(KeyboardSensor, {
      coordinateGetter: sortableKeyboardCoordinates,
    })
  );

  const inputsWithIds = useMemo(
    () => inputs.filter((input) => !input.definition.airbyte_hidden).map((input) => ({ input, id: input.key })),
    [inputs]
  );

  const handleDragEnd = (event: DragEndEvent) => {
    const { active, over } = event;

    if (over !== null && active.id !== over.id) {
      const oldIndex = inputs.findIndex((input) => input.key === active.id.toString());
      const newIndex = inputs.findIndex((input) => input.key === over.id.toString());
      setValue(
        "manifest.spec.connection_specification",
        convertToConnectionSpecification(arrayMove(inputs, oldIndex, newIndex))
      );
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
  const supportedType = supportedTypes.find((type) => type === definition.type);
  if (supportedType && definition.enum) {
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
  const permission = useConnectorBuilderPermission();
  const canEdit = permission !== "readOnly";

  const style = {
    // set x translate to 0 so that the inputs only move up and down
    transform: CSS.Translate.toString(transform ? { ...transform, x: 0 } : null),
    transition,
    zIndex: isDragging ? 1 : undefined,
  };

  const openInputForm = useCallback(
    () => setInputInEditing(formInputToInputInEditing(input)),
    [input, setInputInEditing]
  );

  return (
    <div ref={setNodeRef} style={style} className={classNames({ [styles.dragging]: isDragging })}>
      <Card bodyClassName={styles.inputCard}>
        <FlexContainer direction="column" className={styles.fullWidth} gap="none">
          <FlexContainer direction="row" alignItems="center">
            {canEdit && (
              <Icon
                type="drag"
                color="action"
                {...listeners}
                {...attributes}
                className={classNames(styles.dragHandle, { [styles.dragging]: isDragging })}
              />
            )}
            <ControlLabels
              className={styles.itemLabel}
              label={input.definition.title || input.key}
              optional={!input.required}
              infoTooltipContent={input.definition.description}
            />
            <Button
              className={styles.itemButton}
              size="sm"
              variant="secondary"
              aria-label="Edit"
              type="button"
              onClick={openInputForm}
            >
              <Icon type="gear" color="action" />
            </Button>
          </FlexContainer>
          <InputFormControl builderInput={input} openInputForm={openInputForm} />
        </FlexContainer>
      </Card>
    </div>
  );
};

const InputFormControl = ({
  builderInput,
  openInputForm,
}: {
  builderInput: BuilderFormInput;
  openInputForm: () => void;
}) => {
  const { toggleUI } = useConnectorBuilderFormState();
  const { setValue } = useFormContext();
  const { definition } = builderInput;
  const fieldPath = `testingValues.${builderInput.key}`;
  const value = useBuilderWatch(fieldPath);
  switch (definition.type) {
    case "string": {
      if (definition.enum) {
        const options = definition.enum.map((val) => ({ label: String(val), value: String(val) }));
        return <FormControl fieldType="dropdown" options={options} name={fieldPath} />;
      }

      if (definition.format === "date" || definition.format === "date-time") {
        return <FormControl fieldType="date" format={definition.format} name={fieldPath} />;
      }

      if (definition.airbyte_secret) {
        return (
          <FlexContainer direction="column" className={styles.secretField}>
            <SecretField
              name={fieldPath}
              value={value as string}
              onUpdate={(val) => {
                // Remove the value instead of setting it to the empty string, as secret persistence
                // gets mad at empty secrets
                setValue(fieldPath, val || undefined);
              }}
            />
            <FormControlFooter>
              <FormControlErrorMessage name={fieldPath} />
            </FormControlFooter>
          </FlexContainer>
        );
      }

      return <FormControl fieldType="input" name={fieldPath} />;
    }
    case "integer":
    case "number":
      return <FormControl fieldType="input" type={definition.type} name={fieldPath} />;
    case "boolean":
      return <FormControl fieldType="switch" name={fieldPath} />;
    case "array":
      return <FormControl fieldType="array" itemType="string" name={fieldPath} />;
    default:
      return (
        <Message
          type="error"
          text={
            <FormattedMessage
              id="connectorBuilder.unsupportedInputType.primary"
              values={{ type: definition.type ?? "undefined" }}
            />
          }
          secondaryText={
            <FormattedMessage
              id="connectorBuilder.unsupportedInputType.secondary"
              values={{
                openInputButton: (children: React.ReactNode) => (
                  <Button className={styles.unsupportedInputTypeAction} variant="link" onClick={openInputForm}>
                    {children}
                  </Button>
                ),
                switchToYamlButton: (children: React.ReactNode) => (
                  <Button className={styles.unsupportedInputTypeAction} variant="link" onClick={() => toggleUI("yaml")}>
                    {children}
                  </Button>
                ),
              }}
            />
          }
        />
      );
  }
};

export const convertToBuilderFormInputs = (spec: Spec | undefined): BuilderFormInput[] => {
  if (!spec || !("properties" in spec.connection_specification)) {
    return [];
  }
  if (
    typeof spec.connection_specification.properties !== "object" ||
    spec.connection_specification.properties === null
  ) {
    return [];
  }
  const properties = spec.connection_specification.properties;

  const required: string[] =
    spec.connection_specification.required &&
    Array.isArray(spec.connection_specification.required) &&
    spec.connection_specification.required.every((value) => typeof value === "string")
      ? spec.connection_specification.required
      : [];

  return Object.entries(properties)
    .sort(([_keyA, valueA], [_keyB, valueB]) => {
      if (valueA.order !== undefined && valueB.order !== undefined) {
        return valueA.order - valueB.order;
      }
      if (valueA.order !== undefined && valueB.order === undefined) {
        return -1;
      }
      if (valueA.order === undefined && valueB.order !== undefined) {
        return 1;
      }
      return 0;
    })
    .map(([specKey, specDefinition]) => {
      return {
        key: specKey,
        definition: specDefinition,
        required: required?.includes(specKey) || false,
      };
    });
};

export const convertToConnectionSpecification = (inputs: BuilderFormInput[]): SpecConnectionSpecification => {
  return {
    $schema: "http://json-schema.org/draft-07/schema#",
    type: "object",
    required: inputs.filter((input) => input.required).map((input) => input.key),
    properties: Object.fromEntries(inputs.map((input, index) => [input.key, { ...input.definition, order: index }])),
    additionalProperties: true,
  };
};
