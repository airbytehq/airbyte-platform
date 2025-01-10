import { yupResolver } from "@hookform/resolvers/yup";
import escapeStringRegexp from "escape-string-regexp";
import { useContext, useMemo } from "react";
import { FormProvider, UseFormSetValue, useForm, useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { useEffectOnce } from "react-use";
import * as yup from "yup";

import { Button } from "components/ui/Button";
import { Message } from "components/ui/Message";
import { Modal, ModalBody, ModalFooter } from "components/ui/Modal";

import { ConnectorBuilderProjectTestingValues } from "core/api/types/AirbyteClient";
import { AirbyteJSONSchema } from "core/jsonSchema/types";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import {
  ConnectorBuilderMainRHFContext,
  useConnectorBuilderFormState,
  TestingValuesUpdate,
} from "services/connectorBuilder/ConnectorBuilderStateService";

import { BuilderField } from "./BuilderField";
import styles from "./InputsForm.module.scss";
import { BuilderFormInput, BuilderFormValues, BuilderState, builderInputsToSpec } from "../types";

const supportedTypes = [
  "string",
  "integer",
  "number",
  "array",
  "boolean",
  "enum",
  "unknown",
  "date",
  "date-time",
] as const;

export interface InputInEditing {
  key: string;
  // the old key of the input if it was edited
  previousKey?: string;
  definition: AirbyteJSONSchema;
  required: boolean;
  isLocked?: boolean;
  isNew?: boolean;
  showDefaultValueField: boolean;
  type: (typeof supportedTypes)[number];
}

function sluggify(str: string) {
  return str.toLowerCase().replaceAll(/[^a-zA-Z\d]/g, "_");
}

export function newInputInEditing(): InputInEditing {
  return {
    key: "",
    definition: {},
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
      type:
        type === "enum" || type === "date" || type === "date-time"
          ? "string"
          : type === "unknown"
          ? values.definition.type
          : type,
      // only respect the enum values if the user explicitly selected enum as type
      enum: type === "enum" && values.definition.enum?.length ? values.definition.enum : undefined,
      default: showDefaultValueField ? values.definition.default : undefined,
      format: type === "date" ? "date" : type === "date-time" ? "date-time" : values.definition.format,
      pattern: type === "date" ? DATE_PATTERN : type === "date-time" ? DATE_TIME_PATTERN : values.definition.pattern,
      airbyte_secret: values.definition.airbyte_secret ? true : undefined,
    },
  };
}

export const InputForm = ({
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
  const { updateTestingValues } = useConnectorBuilderFormState();
  const formValues = watch("formValues");
  const testingValues = watch("testingValues");
  const usedKeys = useMemo(() => formValues.inputs.map((input) => input.key), [formValues.inputs]);
  const inputInEditValidation = useMemo(
    () =>
      yup.object().shape({
        // make sure key can only occur once
        key: yup
          .string()
          .notOneOf(
            inputInEditing?.isNew ? usedKeys : usedKeys.filter((key) => key !== inputInEditing?.key),
            "connectorBuilder.duplicateFieldID"
          )
          .required("form.empty.error"),
        required: yup.bool(),
        definition: yup.object().shape({
          title: yup.string().required("form.empty.error"),
        }),
      }) as unknown as yup.SchemaOf<InputInEditing, never>,
    [inputInEditing?.isNew, inputInEditing?.key, usedKeys]
  );

  const methods = useForm<InputInEditing>({
    defaultValues: inputInEditing,
    resolver: yupResolver(inputInEditValidation),
    mode: "onChange",
  });
  const onSubmit = async (inputInEditing: InputInEditing) => {
    const newInput = inputInEditingToFormInput(inputInEditing);
    if (inputInEditing.isNew) {
      setValue("formValues.inputs", [...formValues.inputs, newInput]);
    } else if (inputInEditing.key === inputInEditing.previousKey) {
      setValue(
        "formValues.inputs",
        formValues.inputs.map((input) => (input.key === inputInEditing.key ? newInput : input))
      );
    } else {
      await updateInputKeyAndReferences(
        inputInEditing.previousKey!,
        newInput,
        formValues,
        testingValues,
        setValue,
        updateTestingValues
      );
    }

    onClose(newInput);
    analyticsService.track(
      Namespace.CONNECTOR_BUILDER,
      inputInEditing.isNew ? Action.USER_INPUT_CREATE : Action.USER_INPUT_EDIT,
      {
        actionDescription: inputInEditing.isNew ? "New user input created" : "Existing user input edited",
        user_input_id: inputInEditing.key,
        user_input_name: inputInEditing.definition.title,
        hint: inputInEditing.definition.description,
        type: inputInEditing.type,
        allowed_enum_values: inputInEditing.definition.enum,
        secret_field: inputInEditing.definition.airbyte_secret,
        required_field: inputInEditing.definition.required,
        enable_default_value: inputInEditing.showDefaultValueField,
        default_value: inputInEditing.definition.default,
      }
    );
  };

  return (
    <FormProvider {...methods}>
      <InputModal
        onSubmit={onSubmit}
        inputInEditing={inputInEditing}
        onDelete={() => {
          setValue(
            "formValues.inputs",
            formValues.inputs.filter((input) => input.key !== inputInEditing.key)
          );
          onClose();
          analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.USER_INPUT_DELETE, {
            actionDescription: "User input deleted",
            user_input_id: inputInEditing.key,
            user_input_name: inputInEditing.definition.title,
          });
        }}
        onClose={() => {
          onClose();
        }}
      />
    </FormProvider>
  );
};

async function updateInputKeyAndReferences(
  previousKey: string,
  newInput: BuilderFormInput,
  formValues: BuilderFormValues,
  testingValues: ConnectorBuilderProjectTestingValues | undefined,
  setValue: UseFormSetValue<BuilderState>,
  updateTestingValues: TestingValuesUpdate
) {
  const newInputs = formValues.inputs.map((input) => (input.key === previousKey ? newInput : input));

  const stringifiedFormValues = JSON.stringify(formValues);
  const escapedPreviousKey = escapeStringRegexp(previousKey);

  // replace {{ ... config.key ... }} style references
  const interpolatedConfigReferenceRegexDot = new RegExp(
    `(?<prefix>{{[^}]*?config\\.)(${escapedPreviousKey})(?<suffix>((\\s|\\.).*?)?}})`,
    "g"
  );
  const dotReferencesReplaced = stringifiedFormValues.replaceAll(
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

  const parsedUpdatedFormValues = JSON.parse(bracketReferencesReplaced);
  setValue("formValues", {
    ...parsedUpdatedFormValues,
    inputs: newInputs,
  });

  // update key in testing values if present
  const previousTestingValue = testingValues?.[previousKey];
  if (previousTestingValue) {
    try {
      const spec = builderInputsToSpec(newInputs);
      await updateTestingValues({
        spec: spec?.connection_specification ?? {},
        testingValues: {
          ...testingValues,
          [previousKey]: undefined,
          [newInput.key]: previousTestingValue,
        },
      });
    } catch (e) {
      // Could not update persisted testing values, likely because another required field does not have a value.
      // Instead, just update the testing values in the form state so that the testing values menu uses the new key next time it is opened.
      setValue("testingValues", {
        ...testingValues,
        [previousKey]: undefined,
        [newInput.key]: previousTestingValue,
      });
    }
  }
}

const InputModal = ({
  inputInEditing,
  onClose,
  onDelete,
  onSubmit,
}: {
  inputInEditing: InputInEditing;
  onDelete: () => void;
  onClose: () => void;
  onSubmit: (inputInEditing: InputInEditing) => void;
}) => {
  const {
    formState: { isValid, isSubmitting },
    setValue,
    handleSubmit,
  } = useFormContext<InputInEditing>();
  const values = useWatch<InputInEditing>();

  const { formatMessage } = useIntl();
  useEffectOnce(() => {
    // key input is always touched so errors are shown right away as it will be auto-set by the user changing the title
    if (inputInEditing.key) {
      setValue("key", inputInEditing.key, { shouldValidate: true });
    }
  });

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
      <form
        className={styles.inputForm}
        onSubmit={(e) => {
          // stop propagation to avoid submitting the outer form as this form is nested
          e.stopPropagation();
          handleSubmit(onSubmit)(e);
        }}
      >
        <ModalBody className={styles.inputFormBody}>
          <BuilderField
            path="definition.title"
            type="string"
            onBlur={(newValue) => {
              if (!values.key) {
                setValue("key", sluggify(newValue || ""), { shouldValidate: true });
              }
            }}
            label={formatMessage({ id: "connectorBuilder.inputModal.inputName" })}
            tooltip={formatMessage({ id: "connectorBuilder.inputModal.inputNameTooltip" })}
          />
          <BuilderField
            path="key"
            type="string"
            label={formatMessage({ id: "connectorBuilder.inputModal.fieldId" })}
            tooltip={formatMessage(
              { id: "connectorBuilder.inputModal.fieldIdTooltip" },
              {
                syntaxExample: `{{config['${values.key || "my_input"}']}}`,
              }
            )}
          />
          <BuilderField
            path="definition.description"
            optional
            type="string"
            label={formatMessage({ id: "connectorBuilder.inputModal.description" })}
            tooltip={formatMessage({ id: "connectorBuilder.inputModal.descriptionTooltip" })}
          />
          {values.type !== "unknown" && !values.isLocked ? (
            <>
              <BuilderField
                path="type"
                type="enum"
                options={["string", "number", "integer", "array", "boolean", "enum", "date", "date-time"]}
                onChange={() => {
                  setValue("definition.default", undefined);
                  setValue("definition.pattern", undefined);
                  setValue("definition.format", undefined);
                }}
                label={formatMessage({ id: "connectorBuilder.inputModal.type" })}
                tooltip={formatMessage({ id: "connectorBuilder.inputModal.typeTooltip" })}
              />
              {values.type === "enum" && (
                <BuilderField
                  path="definition.enum"
                  type="array"
                  optional
                  uniqueValues
                  label={formatMessage({ id: "connectorBuilder.inputModal.enum" })}
                  tooltip={formatMessage({ id: "connectorBuilder.inputModal.enumTooltip" })}
                />
              )}
              <BuilderField
                path="definition.airbyte_secret"
                type="boolean"
                optional
                label={formatMessage({ id: "connectorBuilder.inputModal.secret" })}
                tooltip={formatMessage({ id: "connectorBuilder.inputModal.secretTooltip" })}
              />
              <BuilderField
                path="required"
                type="boolean"
                optional
                label={formatMessage({ id: "connectorBuilder.inputModal.required" })}
                tooltip={formatMessage({ id: "connectorBuilder.inputModal.requiredTooltip" })}
              />
              <BuilderField
                path="showDefaultValueField"
                type="boolean"
                optional
                label={formatMessage({ id: "connectorBuilder.inputModal.showDefaultValueField" })}
                tooltip={formatMessage({ id: "connectorBuilder.inputModal.showDefaultValueFieldTooltip" })}
              />
              {values.showDefaultValueField && values.type && (
                <BuilderField
                  path="definition.default"
                  type={values.type}
                  options={(values.definition?.enum || []) as string[]}
                  optional
                  label={formatMessage({ id: "connectorBuilder.inputModal.default" })}
                />
              )}
            </>
          ) : (
            <Message
              type="info"
              text={
                <FormattedMessage
                  id={
                    values.type === "unknown"
                      ? "connectorBuilder.inputModal.unsupportedInput"
                      : "connectorBuilder.inputModal.lockedInput"
                  }
                />
              }
            />
          )}
        </ModalBody>
        <ModalFooter>
          {!inputInEditing.isNew && !inputInEditing.isLocked && (
            <div className={styles.deleteButtonContainer}>
              <Button variant="danger" type="button" onClick={onDelete}>
                <FormattedMessage id="form.delete" />
              </Button>
            </div>
          )}
          <Button variant="secondary" type="reset" onClick={onClose}>
            <FormattedMessage id="form.cancel" />
          </Button>
          <Button type="submit" disabled={!isValid} isLoading={isSubmitting}>
            <FormattedMessage id={inputInEditing.isNew ? "form.create" : "form.saveChanges"} />
          </Button>
        </ModalFooter>
      </form>
    </Modal>
  );
};
