import { FormattedMessage, useIntl } from "react-intl";
import { AnySchema, ValidationError } from "yup";

import styles from "./requiredFields.module.scss";

export interface EmptyRequiredField {
  path: string;
  label: string;
}

/** The label comes from the field's schema, which `buildYupFormForJsonSchema` sets to the title the user sees. */
export const getEmptyRequiredFields = (validationSchema: AnySchema, values: unknown): EmptyRequiredField[] => {
  try {
    validationSchema.validateSync(values, { abortEarly: false });
    return [];
  } catch (error) {
    if (!ValidationError.isError(error)) {
      throw error;
    }
    const errors = error.inner.length > 0 ? error.inner : [error];
    return errors.flatMap(({ type, path, params }) =>
      type === "required" && path ? [{ path, label: typeof params?.label === "string" ? params.label : path }] : []
    );
  }
};

const MAX_LISTED_REQUIRED_FIELDS = 3;

/** Returns undefined when no required field is empty. */
export const useFillRequiredFieldsMessage = (
  emptyRequiredFields: string[],
  messageId:
    | "connectorForm.fillRequiredFields"
    | "connectorForm.sections.fillRequiredFields" = "connectorForm.fillRequiredFields"
) => {
  const { formatMessage, formatList } = useIntl();
  if (emptyRequiredFields.length === 0) {
    return undefined;
  }

  const hiddenCount = emptyRequiredFields.length - MAX_LISTED_REQUIRED_FIELDS;
  const listedFields =
    hiddenCount > 0
      ? [
          ...emptyRequiredFields.slice(0, MAX_LISTED_REQUIRED_FIELDS),
          formatMessage({ id: "connectorForm.fillRequiredFields.more" }, { count: hiddenCount }),
        ]
      : emptyRequiredFields;
  return {
    tooltip: (
      <FormattedMessage
        id={messageId}
        values={{
          fields: (
            <ul className={styles.requiredFieldList}>
              {listedFields.map((field) => (
                <li key={field}>{field}</li>
              ))}
            </ul>
          ),
        }}
      />
    ),
    description: formatMessage({ id: messageId }, { fields: formatList(listedFields, { type: "conjunction" }) }),
  };
};
