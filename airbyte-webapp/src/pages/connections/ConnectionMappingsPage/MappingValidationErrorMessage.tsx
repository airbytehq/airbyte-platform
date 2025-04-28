import { FieldNamesMarkedBoolean, FieldValues } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { Text } from "components/ui/Text";

import { MapperValidationError, MapperValidationErrorType } from "core/api/types/AirbyteClient";

interface MappingValidationErrorMessageProps<T extends FieldValues> {
  validationError?: MapperValidationError;
  touchedFields: Partial<Readonly<FieldNamesMarkedBoolean<T>>>;
}

export const MappingValidationErrorMessage = <T extends FieldValues>({
  validationError,
  touchedFields,
}: MappingValidationErrorMessageProps<T>) => {
  if (
    !validationError ||
    // we handle the "field not found" case separately
    validationError.type === MapperValidationErrorType.FIELD_NOT_FOUND ||
    Object.keys(touchedFields).length === 0
  ) {
    return null;
  }

  return (
    <Text italicized color="red">
      <FormattedMessage
        id={`connections.mappings.error.${validationError.type}`}
        defaultMessage={validationError.message}
      />
    </Text>
  );
};
