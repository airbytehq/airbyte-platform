import React, { useState } from "react";
import { FormProvider, useForm } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import { v4 as uuidv4 } from "uuid";
import * as yup from "yup";

import { FlexContainer } from "components/ui/Flex";
import { Input } from "components/ui/Input";
import { ListBox } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import { StreamMapperType, MapperConfiguration } from "core/api/types/AirbyteClient";

import { autoSubmitResolver } from "./autoSubmitResolver";
import { useMappingContext } from "./MappingContext";
import styles from "./MappingRow.module.scss";
import { MappingTypeListBox } from "./MappingTypeListBox";
import { SelectFieldOption, SelectTargetField } from "./SelectTargetField";
import { useGetFieldsInStream } from "./useGetFieldsInStream";

export enum Algorithm {
  RSA = "RSA",
  AES = "AES",
}
export type AESMode = "CBC" | "CFB" | "OFB" | "CTR" | "GCM" | "ECB";
export type AESPadding = "PKCS5Padding" | "NoPadding";
export const aesModes: AESMode[] = ["CBC", "CFB", "OFB", "CTR", "GCM", "ECB"];
export const aesPaddings: AESPadding[] = ["PKCS5Padding", "NoPadding"];

export interface EncryptionMapperFormValues {
  type: StreamMapperType;
  mapperConfiguration: {
    id: string;
    algorithm: Algorithm;
    targetField: string;
    fieldNameSuffix: string;
    key?: string;
    mode?: AESMode;
    padding?: AESPadding;
    publicKey?: string;
  };
}

const rsaEncryptionMappingSchema = yup.object().shape({
  id: yup.string().required("id required"),
  algorithm: yup.mixed<Algorithm>().oneOf([Algorithm.RSA]).required("Algorithm is required"),
  targetField: yup.string().required("Target field is required"),
  publicKey: yup.string().required("Public key is required"),
  fieldNameSuffix: yup.string().oneOf(["_encrypted"]).required("Field name suffix is required"),
});

const aesEncryptionMappingSchema = yup.object().shape({
  id: yup.string().required("id required"),
  algorithm: yup.mixed<Algorithm>().oneOf([Algorithm.AES]).required("Algorithm is required"),
  targetField: yup.string().required("Target field is required"),
  key: yup.string().required("Key is required"),
  fieldNameSuffix: yup.string().oneOf(["_encrypted"]).required("Field name suffix is required"),
  mode: yup.mixed<AESMode>().oneOf(aesModes).required("Mode is required"),
  padding: yup.mixed<AESPadding>().oneOf(aesPaddings).required("Padding is required"),
});

const encryptionMapperConfigSchema = yup.lazy((value) => {
  switch (value.algorithm) {
    case Algorithm.AES:
      return aesEncryptionMappingSchema.required("AES configuration is required");
    case Algorithm.RSA:
      return rsaEncryptionMappingSchema.required("RSA configuration is required");
    default:
      return yup.mixed().notRequired();
  }
});

export const encryptionMapperSchema = yup.object().shape({
  type: yup.mixed<StreamMapperType>().oneOf(["encryption"]).required(),
  mapperConfiguration: encryptionMapperConfigSchema,
});

const aesFormSchema = yup.object().shape({
  type: yup.mixed<StreamMapperType>().oneOf([StreamMapperType.encryption]).required(),
  mapperConfiguration: yup
    .object()
    .shape({
      id: yup.string().required("id required"),
      algorithm: yup.mixed<Algorithm>().oneOf([Algorithm.AES]).required(),
      targetField: yup.string().required("Target field is required"),
      key: yup.string().required("Key is required"),
      fieldNameSuffix: yup.string().oneOf(["_encrypted"]).required("Field name suffix is required"),
      mode: yup.mixed<AESMode>().oneOf(aesModes).required("Mode is required"),
      padding: yup.mixed<AESPadding>().oneOf(aesPaddings).required("Padding is required"),
    })
    .required(),
});

interface AESFormProps {
  streamName: string;
  mapping: MapperConfiguration;
  setAlgorithm: (algorithm: Algorithm) => void;
  targetFieldOptions: SelectFieldOption[];
}

export const AESForm: React.FC<AESFormProps> = ({ streamName, mapping, setAlgorithm, targetFieldOptions }) => {
  const { updateLocalMapping, validateMappings } = useMappingContext();
  const { formatMessage } = useIntl();

  const methods = useForm<EncryptionMapperFormValues>({
    defaultValues: {
      type: StreamMapperType.encryption,
      mapperConfiguration: {
        id: mapping.mapperConfiguration.id ?? uuidv4(),
        algorithm: Algorithm.AES,
        targetField: mapping.mapperConfiguration.targetField ?? "",
        key: mapping.mapperConfiguration.key ?? "",
        fieldNameSuffix: mapping.mapperConfiguration.fieldNameSuffix ?? "_encrypted",
        mode: mapping.mapperConfiguration.mode ?? "CBC",
        padding: mapping.mapperConfiguration.padding ?? "PKCS5Padding",
      },
    },
    resolver: autoSubmitResolver<EncryptionMapperFormValues>(aesFormSchema, (data) => {
      updateLocalMapping(streamName, data);
      validateMappings();
    }),
    mode: "onBlur",
  });

  const values = methods.watch();

  return (
    <FormProvider {...methods}>
      <form className={styles.form}>
        <SelectTargetField<EncryptionMapperFormValues>
          targetFieldOptions={targetFieldOptions}
          name="mapperConfiguration.targetField"
        />
        <Text>
          <FormattedMessage id="connections.mappings.using" />
        </Text>
        <ListBox
          selectedValue={Algorithm.AES}
          onSelect={(selectedAlgorithm: Algorithm) => setAlgorithm(selectedAlgorithm)}
          options={[
            { label: "AES", value: Algorithm.AES },
            { label: "RSA", value: Algorithm.RSA },
          ]}
        />
        <Text>
          <FormattedMessage id="connections.mappings.andKey" />
        </Text>
        <Input
          placeholder={formatMessage({ id: "connections.mappings.encryption.key" })}
          {...methods.register("mapperConfiguration.key")}
          containerClassName={styles.input}
        />
        <ListBox
          selectedValue={values.mapperConfiguration.mode}
          onSelect={(mode: AESMode) => methods.setValue("mapperConfiguration.mode", mode, { shouldValidate: true })}
          options={aesModes.map((mode) => ({ label: mode, value: mode }))}
        />
        <ListBox
          selectedValue={values.mapperConfiguration.padding}
          onSelect={(padding: AESPadding) =>
            methods.setValue("mapperConfiguration.padding", padding, { shouldValidate: true })
          }
          options={aesPaddings.map((padding) => ({ label: padding, value: padding }))}
        />
      </form>
    </FormProvider>
  );
};

const rsaFormSchema: yup.SchemaOf<EncryptionMapperFormValues> = yup.object().shape({
  type: yup.mixed<StreamMapperType>().oneOf([StreamMapperType.encryption]).required(),
  mapperConfiguration: yup
    .object()
    .shape({
      id: yup.string().required("id required"),
      algorithm: yup.mixed<Algorithm>().oneOf([Algorithm.RSA]).required(),
      targetField: yup.string().required("Target field is required"),
      publicKey: yup.string().required("Public key is required"),
      fieldNameSuffix: yup.string().oneOf(["_encrypted"]).required("Field name suffix is required"),
    })
    .required(),
});

interface RSAFormProps {
  streamName: string;
  mapping: MapperConfiguration;
  setAlgorithm: (algorithm: Algorithm) => void;
  targetFieldOptions: SelectFieldOption[];
}

export const RSAForm: React.FC<RSAFormProps> = ({ streamName, mapping, setAlgorithm, targetFieldOptions }) => {
  const { formatMessage } = useIntl();
  const { updateLocalMapping, validateMappings } = useMappingContext();

  const methods = useForm<EncryptionMapperFormValues>({
    defaultValues: {
      type: StreamMapperType.encryption,
      mapperConfiguration: {
        id: mapping.mapperConfiguration.id ?? uuidv4(),
        algorithm: Algorithm.RSA,
        targetField: mapping.mapperConfiguration.targetField ?? "",
        publicKey: mapping.mapperConfiguration.publicKey ?? "",
        fieldNameSuffix: mapping.mapperConfiguration.fieldNameSuffix ?? "_encrypted",
      },
    },
    resolver: autoSubmitResolver<EncryptionMapperFormValues>(rsaFormSchema, (data) => {
      updateLocalMapping(streamName, data);
      validateMappings();
    }),
    mode: "onBlur",
  });

  return (
    <FormProvider {...methods}>
      <form className={styles.form}>
        <SelectTargetField<EncryptionMapperFormValues>
          targetFieldOptions={targetFieldOptions}
          name="mapperConfiguration.targetField"
        />
        <Text>
          <FormattedMessage id="connections.mappings.using" />
        </Text>
        <ListBox
          selectedValue={Algorithm.RSA}
          onSelect={(selectedAlgorithm: Algorithm) => setAlgorithm(selectedAlgorithm)}
          options={[
            { label: "AES", value: Algorithm.AES },
            { label: "RSA", value: Algorithm.RSA },
          ]}
        />
        <Text>
          <FormattedMessage id="connections.mappings.andKey" />
        </Text>
        <Input
          placeholder={formatMessage({ id: "connections.mappings.encryption.publicKey" })}
          {...methods.register("mapperConfiguration.publicKey")}
          containerClassName={styles.input}
        />
      </form>
    </FormProvider>
  );
};

export const EncryptionRow: React.FC<{
  mappingId: string;
  streamName: string;
}> = ({ mappingId, streamName }) => {
  const { streamsWithMappings } = useMappingContext();
  const mapping = streamsWithMappings[streamName].find((m) => m.mapperConfiguration.id === mappingId);
  const fieldsInStream = useGetFieldsInStream(streamName);

  const [algorithm, setAlgorithm] = useState<Algorithm>(mapping?.mapperConfiguration.algorithm || Algorithm.RSA);

  if (!mapping) {
    return null;
  }

  return (
    <FlexContainer direction="row" alignItems="center" justifyContent="space-between" className={styles.rowContent}>
      <MappingTypeListBox
        selectedValue={StreamMapperType.encryption}
        streamName={streamName}
        mappingId={mapping.mapperConfiguration.id}
      />

      {algorithm === Algorithm.AES && (
        <AESForm
          streamName={streamName}
          mapping={mapping}
          setAlgorithm={setAlgorithm}
          targetFieldOptions={fieldsInStream}
        />
      )}
      {algorithm === Algorithm.RSA && (
        <RSAForm
          streamName={streamName}
          mapping={mapping}
          setAlgorithm={setAlgorithm}
          targetFieldOptions={fieldsInStream}
        />
      )}
    </FlexContainer>
  );
};
