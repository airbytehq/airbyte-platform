import React, { useEffect, useMemo } from "react";
import { FormProvider, useForm } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import * as yup from "yup";

import { FormControlErrorMessage } from "components/forms/FormControl";
import { Text } from "components/ui/Text";

import { FieldRenamingMapperConfiguration, StreamMapperType } from "core/api/types/AirbyteClient";

import { autoSubmitResolver } from "./autoSubmitResolver";
import { useMappingContext } from "./MappingContext";
import { MappingFormTextInput, MappingRowContent, MappingRowItem } from "./MappingRow";
import { MappingTypeListBox } from "./MappingTypeListBox";
import { SelectTargetField } from "./SelectTargetField";
import { StreamMapperWithId } from "./types";

export const fieldRenamingConfigSchema = yup.object().shape({
  newFieldName: yup.string().required("form.empty.error"),
  originalFieldName: yup.string().required("form.empty.error"),
});

interface FieldRenamingRowProps {
  mapping: StreamMapperWithId<FieldRenamingMapperConfiguration>;
  streamDescriptorKey: string;
}

export const FieldRenamingRow: React.FC<FieldRenamingRowProps> = ({ mapping, streamDescriptorKey }) => {
  const { updateLocalMapping, validatingStreams } = useMappingContext();
  const isStreamValidating = validatingStreams.has(streamDescriptorKey);

  const { formatMessage } = useIntl();

  const defaultValues = useMemo(() => {
    return {
      originalFieldName: mapping?.mapperConfiguration?.originalFieldName ?? "",
      newFieldName: mapping?.mapperConfiguration?.newFieldName ?? "",
    };
  }, [mapping]);

  const methods = useForm<FieldRenamingMapperConfiguration>({
    defaultValues,
    resolver: autoSubmitResolver<FieldRenamingMapperConfiguration>(fieldRenamingConfigSchema, (formValues) => {
      updateLocalMapping(streamDescriptorKey, mapping.id, { mapperConfiguration: formValues });
    }),
    mode: "onBlur",
  });

  useEffect(() => {
    if (mapping.validationError && mapping.validationError.type === "FIELD_NOT_FOUND") {
      methods.setError("originalFieldName", { message: mapping.validationError.message });
    } else {
      methods.clearErrors("originalFieldName");
    }
  }, [mapping.validationError, methods]);

  useEffect(() => {
    updateLocalMapping(streamDescriptorKey, mapping.id, { validationCallback: methods.trigger });
  }, [methods.trigger, streamDescriptorKey, updateLocalMapping, mapping.id]);

  return (
    <FormProvider {...methods}>
      <form>
        <MappingRowContent>
          <MappingRowItem>
            <MappingTypeListBox
              disabled={isStreamValidating}
              selectedValue={StreamMapperType["field-renaming"]}
              streamDescriptorKey={streamDescriptorKey}
              mappingId={mapping.id}
            />
          </MappingRowItem>
          <MappingRowItem>
            <SelectTargetField<FieldRenamingMapperConfiguration>
              disabled={isStreamValidating}
              mappingId={mapping.id}
              streamDescriptorKey={streamDescriptorKey}
              name="originalFieldName"
            />
          </MappingRowItem>
          <MappingRowItem>
            <Text>
              <FormattedMessage id="connections.mappings.to" />
            </Text>
          </MappingRowItem>
          <MappingRowItem>
            <MappingFormTextInput<FieldRenamingMapperConfiguration>
              placeholder={formatMessage({ id: "connections.mappings.value" })}
              disabled={isStreamValidating}
              name="newFieldName"
            />
            <FormControlErrorMessage<FieldRenamingMapperConfiguration> name="newFieldName" />
          </MappingRowItem>
        </MappingRowContent>
        {mapping.validationError && mapping.validationError.type !== "FIELD_NOT_FOUND" && (
          <Text italicized color="red">
            {mapping.validationError.message}
          </Text>
        )}
      </form>
    </FormProvider>
  );
};
