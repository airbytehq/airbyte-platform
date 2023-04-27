import { Form, Formik, useFormikContext } from "formik";
import { useContext, useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useEffectOnce } from "react-use";
import * as yup from "yup";

import { Button } from "components/ui/Button";
import { Message } from "components/ui/Message";
import { Modal, ModalBody, ModalFooter } from "components/ui/Modal";

import { Action, Namespace } from "core/analytics";
import { FormikPatch } from "core/form/FormikPatch";
import { AirbyteJSONSchema } from "core/jsonSchema/types";
import { useAnalyticsService } from "hooks/services/Analytics";
import { ConnectorBuilderMainFormikContext } from "services/connectorBuilder/ConnectorBuilderStateService";

import { BuilderField } from "./BuilderField";
import styles from "./InputsForm.module.scss";
import { BuilderFormInput } from "../types";
import { useInferredInputs } from "../useInferredInputs";

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
  definition: AirbyteJSONSchema;
  required: boolean;
  isNew?: boolean;
  showDefaultValueField: boolean;
  type: (typeof supportedTypes)[number];
  isInferredInputOverride: boolean;
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
    isInferredInputOverride: false,
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
  const { values, setFieldValue } = useContext(ConnectorBuilderMainFormikContext) || {};
  if (!values || !setFieldValue) {
    throw new Error("formik context not available");
  }
  const inferredInputs = useInferredInputs();
  const usedKeys = useMemo(
    () => [...values.inputs, ...inferredInputs].map((input) => input.key),
    [values.inputs, inferredInputs]
  );
  const inputInEditValidation = useMemo(
    () =>
      yup.object().shape({
        // make sure key can only occur once
        key: yup
          .string()
          .notOneOf(
            inputInEditing?.isNew ? usedKeys : usedKeys.filter((key) => key !== inputInEditing?.key),
            "connectorBuilder.duplicateFieldID"
          ),
        required: yup.bool(),
        definition: yup.object().shape({
          title: yup.string().required("form.empty.error"),
        }),
      }),
    [inputInEditing?.isNew, inputInEditing?.key, usedKeys]
  );
  return (
    <Formik
      initialValues={inputInEditing}
      validationSchema={inputInEditValidation}
      onSubmit={(inputInEditing: InputInEditing) => {
        if (inputInEditing.isInferredInputOverride) {
          setFieldValue(`inferredInputOverrides.${inputInEditing.key}`, inputInEditing.definition);
          onClose();
        } else {
          const newInput = inputInEditingToFormInput(inputInEditing);
          setFieldValue(
            "inputs",
            inputInEditing.isNew
              ? [...values.inputs, newInput]
              : values.inputs.map((input) => (input.key === inputInEditing.key ? newInput : input))
          );
          onClose(newInput);
        }
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
      }}
    >
      <>
        <FormikPatch />
        <InputModal
          inputInEditing={inputInEditing}
          onDelete={() => {
            setFieldValue(
              "inputs",
              values.inputs.filter((input) => input.key !== inputInEditing.key)
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
      </>
    </Formik>
  );
};

const InputModal = ({
  inputInEditing,
  onClose,
  onDelete,
}: {
  inputInEditing: InputInEditing;
  onDelete: () => void;
  onClose: () => void;
}) => {
  const isInferredInputOverride = inputInEditing.isInferredInputOverride;
  const { isValid, values, setFieldValue, setTouched } = useFormikContext<InputInEditing>();

  const { formatMessage } = useIntl();
  useEffectOnce(() => {
    // key input is always touched so errors are shown right away as it will be auto-set by the user changing the title
    setTouched({ key: true });
  });

  return (
    <Modal
      size="sm"
      title={
        <FormattedMessage
          id={inputInEditing.isNew ? "connectorBuilder.inputModal.newTitle" : "connectorBuilder.inputModal.editTitle"}
        />
      }
      wrapIn={Form}
      onClose={onClose}
    >
      <ModalBody className={styles.inputFormBody}>
        <BuilderField
          path="definition.title"
          type="string"
          onChange={(newValue) => {
            if (!isInferredInputOverride) {
              setFieldValue("key", sluggify(newValue || ""), true);
            }
          }}
          label={formatMessage({ id: "connectorBuilder.inputModal.inputName" })}
          tooltip={formatMessage({ id: "connectorBuilder.inputModal.inputNameTooltip" })}
        />
        <BuilderField
          path="key"
          type="string"
          readOnly
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
        {values.type !== "unknown" && !isInferredInputOverride ? (
          <>
            <BuilderField
              path="type"
              type="enum"
              options={["string", "number", "integer", "array", "boolean", "enum", "date", "date-time"]}
              onChange={() => {
                setFieldValue("definition.default", undefined);
                setFieldValue("definition.pattern", undefined);
                setFieldValue("definition.format", undefined);
              }}
              label={formatMessage({ id: "connectorBuilder.inputModal.type" })}
              tooltip={formatMessage({ id: "connectorBuilder.inputModal.typeTooltip" })}
            />
            {values.type === "enum" && (
              <BuilderField
                path="definition.enum"
                type="array"
                optional
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
            {values.showDefaultValueField && (
              <BuilderField
                path="definition.default"
                type={values.type}
                options={(values.definition.enum || []) as string[]}
                optional
                label={formatMessage({ id: "connectorBuilder.inputModal.default" })}
              />
            )}
          </>
        ) : (
          <Message
            type="info"
            text={
              isInferredInputOverride ? (
                <FormattedMessage id="connectorBuilder.inputModal.inferredInputMessage" />
              ) : (
                <FormattedMessage id="connectorBuilder.inputModal.unsupportedInput" />
              )
            }
          />
        )}
      </ModalBody>
      <ModalFooter>
        {!inputInEditing.isNew && !inputInEditing.isInferredInputOverride && (
          <div className={styles.deleteButtonContainer}>
            <Button variant="danger" type="button" onClick={onDelete}>
              <FormattedMessage id="form.delete" />
            </Button>
          </div>
        )}
        <Button variant="secondary" type="reset" onClick={onClose}>
          <FormattedMessage id="form.cancel" />
        </Button>
        <Button type="submit" disabled={!isValid}>
          <FormattedMessage id={inputInEditing.isNew ? "form.create" : "form.saveChanges"} />
        </Button>
      </ModalFooter>
    </Modal>
  );
};
