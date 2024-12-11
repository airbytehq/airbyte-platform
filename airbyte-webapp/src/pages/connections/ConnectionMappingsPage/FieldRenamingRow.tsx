import React, { useMemo } from "react";
import { FormProvider, get, useForm, useFormContext, useFormState } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { v4 as uuidv4 } from "uuid";
import * as yup from "yup";

import { FormControlFooterError } from "components/forms/FormControl";
import { FlexContainer } from "components/ui/Flex";
import { Input } from "components/ui/Input";
import { Text } from "components/ui/Text";

import { StreamMapperType } from "core/api/types/AirbyteClient";

import { autoSubmitResolver } from "./autoSubmitResolver";
import { useMappingContext } from "./MappingContext";
import styles from "./MappingRow.module.scss";
import { MappingTypeListBox } from "./MappingTypeListBox";
import { SelectTargetField } from "./SelectTargetField";
import { useGetFieldsInStream } from "./useGetFieldsInStream";

export const fieldRenamingConfigSchema = yup.object().shape({
  id: yup.string().required("id required"),
  newFieldName: yup.string().required("New field name is required"),
  originalFieldName: yup.string().required("Old field name is required"),
});

export const fieldRenamingMapperSchema: yup.SchemaOf<FieldRenamingMapperFormValues> = yup.object().shape({
  type: yup.mixed<StreamMapperType>().oneOf(["field-renaming"]).required(),
  mapperConfiguration: fieldRenamingConfigSchema.required(),
});

interface FieldRenamingRowProps {
  mappingId: number;
  streamName: string;
}
interface FieldRenamingMapperFormValues {
  type: StreamMapperType;
  mapperConfiguration: {
    id: string;
    originalFieldName: string;
    newFieldName: string;
  };
}

export const FieldRenamingRow: React.FC<FieldRenamingRowProps> = ({ mappingId, streamName }) => {
  const { updateLocalMapping, streamsWithMappings, validateMappings } = useMappingContext();
  const mapping = streamsWithMappings[streamName].find((m) => m.mapperConfiguration.id === mappingId);
  const fieldsInStream = useGetFieldsInStream(streamName);

  const defaultValues = useMemo(() => {
    return {
      type: StreamMapperType["field-renaming"],
      mapperConfiguration: {
        id: mapping?.mapperConfiguration.id ?? uuidv4(),
        originalFieldName: mapping?.mapperConfiguration?.originalFieldName ?? "",
        newFieldName: mapping?.mapperConfiguration?.newFieldName ?? "",
      },
    };
  }, [mapping]);

  const methods = useForm<FieldRenamingMapperFormValues>({
    defaultValues,
    resolver: autoSubmitResolver<FieldRenamingMapperFormValues>(fieldRenamingMapperSchema, (data) => {
      updateLocalMapping(streamName, data);
      validateMappings();
    }),
    mode: "onBlur",
  });

  return (
    <FormProvider {...methods}>
      <form>
        <FlexContainer direction="row" alignItems="center" justifyContent="space-between" className={styles.rowContent}>
          <MappingTypeListBox
            selectedValue={StreamMapperType["field-renaming"]}
            streamName={streamName}
            mappingId={mapping?.mapperConfiguration.id}
          />
          <SelectTargetField<FieldRenamingMapperFormValues>
            targetFieldOptions={fieldsInStream}
            name="mapperConfiguration.originalFieldName"
          />
          <Text>
            <FormattedMessage id="connections.mappings.to" />
          </Text>
          <NewFieldNameInput />
        </FlexContainer>
      </form>
    </FormProvider>
  );
};

const NewFieldNameInput = () => {
  const { formatMessage } = useIntl();
  const { register } = useFormContext();
  const { errors } = useFormState<FieldRenamingMapperFormValues>({ name: "mapperConfiguration.newFieldName" });
  const error = get(errors, "mapperConfiguration.newFieldName");
  return (
    <div>
      <Input
        containerClassName={styles.input}
        placeholder={formatMessage({ id: "connections.mappings.newFieldName" })}
        {...register("mapperConfiguration.newFieldName")}
      />
      {error && <FormControlFooterError>{error.message}</FormControlFooterError>}
    </div>
  );
};
