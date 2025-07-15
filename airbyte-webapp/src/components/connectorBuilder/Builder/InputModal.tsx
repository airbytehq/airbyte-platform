import escapeStringRegexp from "escape-string-regexp";
import { useCallback, useContext, useMemo } from "react";
import { FieldValues, UseFormSetValue, useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { useEffectOnce, useUpdateEffect } from "react-use";
import { z } from "zod";

import { Form, FormControl } from "components/forms";
import { Button } from "components/ui/Button";
import { Modal, ModalBody, ModalFooter } from "components/ui/Modal";

import { ConnectorBuilderProjectTestingValues } from "core/api/types/AirbyteClient";
import { ConnectorManifest } from "core/api/types/ConnectorManifest";
import { AirbyteJSONSchema } from "core/jsonSchema/types";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { ConnectorBuilderMainRHFContext } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./InputModal.module.scss";
import { DefinitionFormControl, convertToBuilderFormInputs, convertToConnectionSpecification } from "./InputsView";
import { BuilderFormInput } from "../types";

export const supportedTypes = ["string", "integer", "number", "array", "boolean", "enum", "date", "date-time"] as const;

export interface InputInEditing {
  key: string;
  // the old key of the input if it was edited
  previousKey?: string;
  definition: AirbyteJSONSchema;
  required: boolean;
  isLocked?: boolean;
  isNew?: boolean;
  showDefaultValueField: boolean;
  type?: (typeof supportedTypes)[number];
}

function sluggify(str: string) {
  return str.toLowerCase().replaceAll(/[^a-zA-Z\d]/g, "_");
}

export function newInputInEditing(): InputInEditing {
  return {
    key: "",
    definition: {
      type: "string",
    },
    required: true,
    isNew: true,
    showDefaultValueField: false,
    type: "string",
  };
}

const DATE_PATTERN = "^[0-9]{4}-[0-9]{2}-[0-9]{2}$";
const DATE_TIME_PATTERN = "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$";

function inputInEditingToFormInput({
  type,
  showDefaultValueField,
  isNew,
  ...values
}: InputInEditing): BuilderFormInput {
  return {
    ...values,
    definition: {
      ...values.definition,
      type: type === "enum" || type === "date" || type === "date-time" ? "string" : type,
      // only respect the enum values if the user explicitly selected enum as type
      enum: type === "enum" && values.definition.enum?.length ? values.definition.enum : undefined,
      default: showDefaultValueField ? values.definition.default : undefined,
      format: type === "date" ? "date" : type === "date-time" ? "date-time" : values.definition.format,
      pattern: values.definition.pattern,
      airbyte_secret: values.definition.airbyte_secret ? true : undefined,
      airbyte_hidden: values.definition.airbyte_hidden ? true : undefined,
    },
  };
}

export const InputModal = ({
  inputInEditing,
  onClose,
}: {
  inputInEditing: InputInEditing;
  onClose: (newInput?: BuilderFormInput) => void;
}) => {
  const analyticsService = useAnalyticsService();
  const { setValue, watch } = useContext(ConnectorBuilderMainRHFContext) || {};
  if (!setValue || !watch) {
    throw new Error("rhf context not available");
  }
  const manifest: ConnectorManifest | null = watch("manifest");
  const builderFormInputs: BuilderFormInput[] = useMemo(
    () => convertToBuilderFormInputs(manifest?.spec),
    [manifest?.spec]
  );
  const testingValues: ConnectorBuilderProjectTestingValues = watch("testingValues");
  const usedKeys = useMemo(
    () => (builderFormInputs ? builderFormInputs.map((input) => input.key) : []),
    [builderFormInputs]
  );

  const inputModalSchema = z.object({
    // make sure key can only occur once
    key: z
      .string()
      .trim()
      .nonempty("form.empty.error")
      .refine(
        (key) =>
          inputInEditing?.isNew
            ? !usedKeys.includes(key)
            : !usedKeys.filter((k) => k !== inputInEditing?.key).includes(key),
        "connectorBuilder.duplicateFieldID"
      ),
    required: z.boolean(),
    type: z.enum(supportedTypes),
    definition: z.object({
      title: z.string().trim().nonempty("form.empty.error"),
      description: z.string().optional(),
      enum: z
        .array(z.string())
        .optional()
        .refine((values) => (values ? new Set(values).size === values.length : true), "connectorBuilder.enumDuplicate"),
      airbyte_secret: z.boolean().optional(),
      airbyte_hidden: z.boolean().optional(),
      pattern: z.string().optional(),
      default: z.any().optional(),
    }),
    showDefaultValueField: z.boolean(),
    isNew: z.boolean().optional(),
    previousKey: z.string().optional(),
  });

  const onSubmit = useCallback(
    async (submittedInput: InputInEditing) => {
      const previousInput = builderFormInputs.find((input) => input.key === submittedInput.previousKey);
      const newInput = inputInEditingToFormInput(submittedInput);
      await adjustBuilderInputs(submittedInput, setValue, newInput, builderFormInputs, manifest);
      adjustTestingValues(newInput, previousInput, setValue, testingValues);

      onClose(newInput);
      analyticsService.track(
        Namespace.CONNECTOR_BUILDER,
        submittedInput.isNew ? Action.USER_INPUT_CREATE : Action.USER_INPUT_EDIT,
        {
          actionDescription: submittedInput.isNew ? "New user input created" : "Existing user input edited",
          user_input_id: submittedInput.key,
          user_input_name: submittedInput.definition.title,
          hint: submittedInput.definition.description,
          type: submittedInput.type,
          allowed_enum_values: submittedInput.definition.enum,
          secret_field: submittedInput.definition.airbyte_secret,
          hidden_field: submittedInput.definition.airbyte_hidden,
          pattern: submittedInput.definition.pattern,
          required_field: submittedInput.definition.required,
          enable_default_value: submittedInput.showDefaultValueField,
          default_value: submittedInput.definition.default,
        }
      );
    },
    [analyticsService, builderFormInputs, manifest, onClose, setValue, testingValues]
  );

  const onDelete = useCallback(() => {
    setValue(
      "manifest.spec.connection_specification",
      convertToConnectionSpecification(builderFormInputs.filter((input) => input.key !== inputInEditing.key))
    );
    setValue(
      "testingValues",
      {
        ...testingValues,
        [inputInEditing.key]: undefined,
      },
      {
        shouldValidate: true,
        shouldDirty: true,
        shouldTouch: true,
      }
    );
    onClose();
    analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.USER_INPUT_DELETE, {
      actionDescription: "User input deleted",
      user_input_id: inputInEditing.key,
      user_input_name: inputInEditing.definition.title,
    });
  }, [analyticsService, builderFormInputs, inputInEditing, onClose, setValue, testingValues]);

  return (
    <Modal
      size="sm"
      title={
        <FormattedMessage
          id={inputInEditing.isNew ? "connectorBuilder.inputModal.newTitle" : "connectorBuilder.inputModal.editTitle"}
        />
      }
      onCancel={onClose}
    >
      <Form<InputInEditing>
        onSubmit={(values) => {
          return onSubmit(values);
        }}
        defaultValues={inputInEditing}
        zodSchema={inputModalSchema}
      >
        <InputModalContents onDelete={onDelete} onClose={onClose} />
      </Form>
    </Modal>
  );
};

function adjustTestingValues(
  newInput: BuilderFormInput,
  previousInput: BuilderFormInput | undefined,
  setValue: UseFormSetValue<FieldValues>,
  testingValues: ConnectorBuilderProjectTestingValues | undefined
) {
  const defaultValue = newInput.definition.default ?? (newInput.definition.type === "boolean" ? false : undefined);
  const newTestingValue =
    previousInput && previousInput.definition.type === newInput.definition.type
      ? testingValues?.[previousInput.key]
      : defaultValue;

  if (previousInput) {
    setValue(
      "testingValues",
      {
        ...testingValues,
        [previousInput?.key]: undefined,
        [newInput.key]: newTestingValue,
      },
      {
        shouldValidate: true,
        shouldDirty: true,
        shouldTouch: true,
      }
    );
  } else {
    setValue("testingValues", {
      ...testingValues,
      [newInput.key]: newTestingValue,
    });
  }
}

async function adjustBuilderInputs(
  inputInEditing: InputInEditing,
  setValue: UseFormSetValue<FieldValues>,
  newInput: BuilderFormInput,
  currentBuilderFormInputs: BuilderFormInput[],
  manifest: ConnectorManifest | null
) {
  if (inputInEditing.isNew) {
    setValue(
      "manifest.spec.connection_specification",
      convertToConnectionSpecification([...currentBuilderFormInputs, newInput])
    );
  } else if (inputInEditing.key === inputInEditing.previousKey) {
    setValue(
      "manifest.spec.connection_specification",
      convertToConnectionSpecification(
        currentBuilderFormInputs.map((input) => (input.key === inputInEditing.key ? newInput : input))
      )
    );
  } else {
    await updateInputKeyAndReferences(
      inputInEditing.previousKey!,
      newInput,
      currentBuilderFormInputs,
      setValue,
      manifest
    );
  }
}

async function updateInputKeyAndReferences(
  previousKey: string,
  newInput: BuilderFormInput,
  currentBuilderFormInputs: BuilderFormInput[],
  setValue: UseFormSetValue<FieldValues>,
  manifest: ConnectorManifest | null
) {
  if (!manifest) {
    return;
  }
  const newInputs = currentBuilderFormInputs.map((input) => (input.key === previousKey ? newInput : input));
  const newConnectionSpecification = convertToConnectionSpecification(newInputs);

  const stringifiedManifest = JSON.stringify(manifest);
  const escapedPreviousKey = escapeStringRegexp(previousKey);

  // replace {{ ... config.key ... }} style references
  const interpolatedConfigReferenceRegexDot = new RegExp(
    `(?<prefix>{{[^}]*?config\\.)(${escapedPreviousKey})(?<suffix>((\\s|\\.).*?)?}})`,
    "g"
  );
  const dotReferencesReplaced = stringifiedManifest.replaceAll(
    interpolatedConfigReferenceRegexDot,
    `$<prefix>${newInput.key}$<suffix>`
  );

  // replace {{ ... config['key'] ... }} style references
  const interpolatedConfigReferenceRegexBracket = new RegExp(
    `(?<prefix>{{[^}]*?config\\[('|\\\\")+)(${escapedPreviousKey})(?<suffix>('|\\\\")+\\].*?}})`,
    "g"
  );
  const bracketReferencesReplaced = dotReferencesReplaced.replaceAll(
    interpolatedConfigReferenceRegexBracket,
    `$<prefix>${newInput.key}$<suffix>`
  );

  // TODO: update advanced_auth as well

  const parsedUpdatedFormValues = JSON.parse(bracketReferencesReplaced);
  setValue("manifest", {
    ...parsedUpdatedFormValues,
    spec: {
      ...parsedUpdatedFormValues.spec,
      connection_specification: newConnectionSpecification,
    },
  });
}

const InputModalContents = ({ onDelete, onClose }: { onDelete: () => void; onClose: () => void }) => {
  const {
    formState: { isSubmitting },
    setValue,
  } = useFormContext<InputInEditing>();
  const values = useWatch<InputInEditing>();

  const { formatMessage } = useIntl();
  useEffectOnce(() => {
    // key input is always touched so errors are shown right away as it will be auto-set by the user changing the title
    if (values.key) {
      setValue("key", values.key, { shouldValidate: true });
    }
  });

  useUpdateEffect(() => {
    setValue("definition.format", undefined);
    setValue("definition.default", "");
    setValue("definition.enum", undefined);
    setValue("definition.pattern", undefined);
    switch (values.type) {
      case "string":
        setValue("definition.type", "string");
        break;
      case "number":
        setValue("definition.type", "number");
        break;
      case "integer":
        setValue("definition.type", "integer");
        break;
      case "array":
        setValue("definition.type", "array");
        break;
      case "boolean":
        setValue("definition.type", "boolean");
        break;
      case "enum":
        setValue("definition.type", "string");
        setValue("definition.enum", []);
        break;
      case "date":
        setValue("definition.type", "string");
        setValue("definition.format", "date");
        setValue("definition.pattern", DATE_PATTERN);
        break;
      case "date-time":
        setValue("definition.type", "string");
        setValue("definition.format", "date-time");
        setValue("definition.pattern", DATE_TIME_PATTERN);
        break;
      default:
        break;
    }
  }, [values.type]);

  return (
    <>
      <ModalBody className={styles.inputFormBody}>
        <FormControl<InputInEditing>
          name="definition.title"
          fieldType="input"
          onBlur={(e) => {
            if (!values.key) {
              setValue("key", sluggify(e.target.value), { shouldValidate: true });
            }
          }}
          label={formatMessage({ id: "connectorBuilder.inputModal.inputName" })}
          labelTooltip={formatMessage({ id: "connectorBuilder.inputModal.inputNameTooltip" })}
        />
        <FormControl<InputInEditing>
          name="key"
          fieldType="input"
          label={formatMessage({ id: "connectorBuilder.inputModal.fieldId" })}
          labelTooltip={formatMessage(
            { id: "connectorBuilder.inputModal.fieldIdTooltip" },
            {
              syntaxExample: `{{config['${values.key || "my_input"}']}}`,
            }
          )}
        />
        <FormControl<InputInEditing>
          name="definition.description"
          fieldType="input"
          optional
          label={formatMessage({ id: "connectorBuilder.inputModal.description" })}
          labelTooltip={formatMessage({ id: "connectorBuilder.inputModal.descriptionTooltip" })}
        />
        <FormControl<InputInEditing>
          name="type"
          fieldType="dropdown"
          options={[
            { label: "String", value: "string" },
            { label: "Number", value: "number" },
            { label: "Integer", value: "integer" },
            { label: "Array", value: "array" },
            { label: "Boolean", value: "boolean" },
            { label: "Enum", value: "enum" },
            { label: "Date", value: "date" },
            { label: "Date-Time", value: "date-time" },
          ]}
          label={formatMessage({ id: "connectorBuilder.inputModal.type" })}
          labelTooltip={formatMessage({ id: "connectorBuilder.inputModal.typeTooltip" })}
        />
        {values.type === "enum" && (
          <FormControl<InputInEditing>
            name="definition.enum"
            fieldType="array"
            label={formatMessage({ id: "connectorBuilder.inputModal.enum" })}
            labelTooltip={formatMessage({ id: "connectorBuilder.inputModal.enumTooltip" })}
          />
        )}
        {(values.type === "date" || values.type === "date-time") && (
          <FormControl<InputInEditing>
            name="definition.pattern"
            fieldType="input"
            optional
            label={formatMessage({ id: "connectorBuilder.inputModal.pattern" })}
            labelTooltip={formatMessage({ id: "connectorBuilder.inputModal.patternTooltip" })}
          />
        )}
        <FormControl<InputInEditing>
          name="required"
          fieldType="switch"
          optional
          label={formatMessage({ id: "connectorBuilder.inputModal.required" })}
          labelTooltip={formatMessage({ id: "connectorBuilder.inputModal.requiredTooltip" })}
        />
        <FormControl<InputInEditing>
          name="definition.airbyte_secret"
          fieldType="switch"
          optional
          label={formatMessage({ id: "connectorBuilder.inputModal.secret" })}
          labelTooltip={formatMessage({ id: "connectorBuilder.inputModal.secretTooltip" })}
        />
        <FormControl<InputInEditing>
          name="definition.airbyte_hidden"
          fieldType="switch"
          optional
          label={formatMessage({ id: "connectorBuilder.inputModal.hidden" })}
          labelTooltip={formatMessage({ id: "connectorBuilder.inputModal.hiddenTooltip" })}
        />
        <FormControl<InputInEditing>
          name="showDefaultValueField"
          fieldType="switch"
          optional
          label={formatMessage({ id: "connectorBuilder.inputModal.showDefaultValueField" })}
          labelTooltip={formatMessage({ id: "connectorBuilder.inputModal.showDefaultValueFieldTooltip" })}
        />
        {values.showDefaultValueField && (
          <DefinitionFormControl
            key={`${values?.definition?.type}-default`}
            id={`${values?.definition?.type}-default`}
            name="definition.default"
            definition={values.definition as AirbyteJSONSchema}
            unrecognizedTypeElement={null}
            label={formatMessage({ id: "connectorBuilder.inputModal.default" })}
          />
        )}
      </ModalBody>
      <ModalFooter>
        {!values.isNew && (
          <div className={styles.deleteButtonContainer}>
            <Button variant="danger" type="button" onClick={onDelete}>
              <FormattedMessage id="form.delete" />
            </Button>
          </div>
        )}
        <Button variant="secondary" type="reset" onClick={() => onClose()}>
          <FormattedMessage id="form.cancel" />
        </Button>

        <Button type="submit" isLoading={isSubmitting}>
          <FormattedMessage id={values.isNew ? "form.create" : "form.saveChanges"} />
        </Button>
      </ModalFooter>
    </>
  );
};
