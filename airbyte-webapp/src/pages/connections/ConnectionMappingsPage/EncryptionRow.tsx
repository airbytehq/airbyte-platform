import React, { useState } from "react";
import { FormProvider, useForm } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";
import * as yup from "yup";

import { Input } from "components/ui/Input";
import { ListBox } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import {
  EncryptionMapperAESConfigurationPadding,
  EncryptionMapperAlgorithm,
  StreamMapperType,
  EncryptionMapperAESConfigurationMode,
} from "core/api/types/AirbyteClient";

import { autoSubmitResolver } from "./autoSubmitResolver";
import { useMappingContext } from "./MappingContext";
import styles from "./MappingRow.module.scss";
import { SelectFieldOption, SelectTargetField } from "./SelectTargetField";
import { StreamMapperWithId, EncryptionMapperConfiguration } from "./types";
import { useGetFieldsInStream } from "./useGetFieldsInStream";

export interface EncryptionMapperFormValues<T extends EncryptionMapperAlgorithm> {
  type: StreamMapperType;
  id: string;
  mapperConfiguration: EncryptionMapperConfiguration<T>;
}

const rsaEncryptionMappingSchema = yup.object().shape({
  algorithm: yup.mixed<EncryptionMapperAlgorithm>().oneOf(["RSA"]).required("Algorithm is required"),
  targetField: yup.string().required("Target field is required"),
  publicKey: yup.string().required("Public key is required"),
  fieldNameSuffix: yup.string().oneOf(["_encrypted"]).required("Field name suffix is required"),
});

const aesEncryptionMappingSchema = yup.object().shape({
  algorithm: yup.mixed<EncryptionMapperAlgorithm>().oneOf(["AES"]).required("Algorithm is required"),
  targetField: yup.string().required("Target field is required"),
  key: yup.string().required("Key is required"),
  fieldNameSuffix: yup.string().oneOf(["_encrypted"]).required("Field name suffix is required"),
  mode: yup
    .mixed<EncryptionMapperAESConfigurationMode>()
    .oneOf(Object.values(EncryptionMapperAESConfigurationMode))
    .required("Mode is required"),
  padding: yup
    .mixed<EncryptionMapperAESConfigurationPadding>()
    .oneOf(Object.values(EncryptionMapperAESConfigurationPadding))
    .required("Padding is required"),
});

const encryptionMapperConfigSchema = yup.lazy((value) => {
  switch (value.algorithm) {
    case "AES":
      return aesEncryptionMappingSchema.required("AES configuration is required");
    case "RSA":
      return rsaEncryptionMappingSchema.required("RSA configuration is required");
    default:
      return yup.mixed().notRequired();
  }
});

export const encryptionMapperSchema = yup.object().shape({
  type: yup.mixed<StreamMapperType>().oneOf(["encryption"]).required(),
  id: yup.string().required("id required"),
  mapperConfiguration: encryptionMapperConfigSchema,
});

const aesFormSchema = yup.object().shape({
  type: yup.mixed<StreamMapperType>().oneOf([StreamMapperType.encryption]).required(),
  id: yup.string().required("id required"),
  mapperConfiguration: yup
    .object()
    .shape({
      algorithm: yup.mixed<EncryptionMapperAlgorithm>().oneOf(["AES", "RSA"]).required(),
      targetField: yup.string().required("Target field is required"),
      key: yup.string().required("Key is required"),
      fieldNameSuffix: yup.string().oneOf(["_encrypted"]).required("Field name suffix is required"),
      mode: yup
        .mixed<EncryptionMapperAESConfigurationMode>()
        .oneOf(Object.values(EncryptionMapperAESConfigurationMode))
        .required("Mode is required"),
      padding: yup
        .mixed<EncryptionMapperAESConfigurationPadding>()
        .oneOf(Object.values(EncryptionMapperAESConfigurationPadding))
        .required("Padding is required"),
    })
    .required(),
});

interface AESFormProps {
  streamName: string;
  mapping: StreamMapperWithId<EncryptionMapperConfiguration<"AES">>;
  setAlgorithm: (algorithm: EncryptionMapperAlgorithm) => void;
  targetFieldOptions: SelectFieldOption[];
}

export const AESForm: React.FC<AESFormProps> = ({ streamName, mapping, setAlgorithm, targetFieldOptions }) => {
  const { updateLocalMapping, validateMappings } = useMappingContext();
  const { formatMessage } = useIntl();

  // needed for type narrowing for schema
  if (mapping.mapperConfiguration.algorithm !== "AES") {
    throw new Error("Invalid configuration: expected AES configuration");
  }

  const methods = useForm<EncryptionMapperFormValues<"AES">>({
    defaultValues: {
      type: StreamMapperType.encryption,
      id: mapping.id,
      mapperConfiguration: {
        algorithm: "AES",
        targetField: mapping.mapperConfiguration.targetField ?? "",
        key: mapping.mapperConfiguration.key ?? "",
        fieldNameSuffix: mapping.mapperConfiguration.fieldNameSuffix ?? "_encrypted",
        mode: mapping.mapperConfiguration.mode ?? "CBC",
        padding: mapping.mapperConfiguration.padding ?? "PKCS5Padding",
      },
    },
    resolver: autoSubmitResolver<EncryptionMapperFormValues<"AES">>(aesFormSchema, (data) => {
      updateLocalMapping(streamName, mapping.id, data);
      validateMappings();
    }),
    mode: "onBlur",
  });

  const values = methods.watch();

  return (
    <FormProvider {...methods}>
      <form className={styles.form}>
        <SelectTargetField<EncryptionMapperFormValues<"AES">>
          targetFieldOptions={targetFieldOptions}
          name="mapperConfiguration.targetField"
        />
        <Text>
          <FormattedMessage id="connections.mappings.using" />
        </Text>
        <ListBox
          selectedValue="AES"
          onSelect={(selectedAlgorithm: EncryptionMapperAlgorithm) => setAlgorithm(selectedAlgorithm)}
          options={[
            { label: "AES", value: "AES" },
            { label: "RSA", value: "RSA" },
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
          onSelect={(mode: EncryptionMapperAESConfigurationMode) =>
            methods.setValue("mapperConfiguration.mode", mode, { shouldValidate: true })
          }
          options={Object.values(EncryptionMapperAESConfigurationMode).map((mode) => ({ label: mode, value: mode }))}
        />
        <ListBox
          selectedValue={values.mapperConfiguration.padding}
          onSelect={(padding: EncryptionMapperAESConfigurationPadding) =>
            methods.setValue("mapperConfiguration.padding", padding, { shouldValidate: true })
          }
          options={Object.values(EncryptionMapperAESConfigurationPadding).map((padding) => ({
            label: padding,
            value: padding,
          }))}
        />
      </form>
    </FormProvider>
  );
};

const rsaFormSchema: yup.SchemaOf<EncryptionMapperFormValues<"RSA">> = yup.object().shape({
  type: yup.mixed<StreamMapperType>().oneOf([StreamMapperType.encryption]).required(),
  id: yup.string().required("id required"),
  mapperConfiguration: yup
    .object()
    .shape({
      id: yup.string().required("id required"),
      algorithm: yup.mixed<EncryptionMapperAlgorithm>().oneOf(["RSA"]).required(),
      targetField: yup.string().required("Target field is required"),
      publicKey: yup.string().required("Public key is required"),
      fieldNameSuffix: yup.string().oneOf(["_encrypted"]).required("Field name suffix is required"),
    })
    .required(),
});

interface RSAFormProps {
  streamName: string;
  mapping: StreamMapperWithId<EncryptionMapperConfiguration<"RSA">>;
  setAlgorithm: (algorithm: EncryptionMapperAlgorithm) => void;
  targetFieldOptions: SelectFieldOption[];
}

export const RSAForm: React.FC<RSAFormProps> = ({ streamName, mapping, setAlgorithm, targetFieldOptions }) => {
  const { formatMessage } = useIntl();
  const { updateLocalMapping, validateMappings } = useMappingContext();

  // needed for type narrowing for schema
  if (mapping.mapperConfiguration.algorithm !== "RSA") {
    throw new Error("Invalid configuration: expected AES configuration");
  }
  const methods = useForm<EncryptionMapperFormValues<"RSA">>({
    defaultValues: {
      type: StreamMapperType.encryption,
      id: mapping.id,
      mapperConfiguration: {
        algorithm: "RSA",
        targetField: mapping.mapperConfiguration.targetField ?? "",
        publicKey: mapping.mapperConfiguration.publicKey ?? "",
        fieldNameSuffix: mapping.mapperConfiguration.fieldNameSuffix ?? "_encrypted",
      },
    },
    resolver: autoSubmitResolver<EncryptionMapperFormValues<"RSA">>(rsaFormSchema, (data) => {
      updateLocalMapping(streamName, mapping.id, data);
      validateMappings();
    }),
    mode: "onBlur",
  });

  return (
    <FormProvider {...methods}>
      <form className={styles.form}>
        <SelectTargetField<EncryptionMapperFormValues<"RSA">>
          targetFieldOptions={targetFieldOptions}
          name="mapperConfiguration.targetField"
        />
        <Text>
          <FormattedMessage id="connections.mappings.using" />
        </Text>
        <ListBox
          selectedValue="RSA"
          onSelect={(selectedAlgorithm: EncryptionMapperAlgorithm) => setAlgorithm(selectedAlgorithm)}
          options={[
            { label: "AES", value: "AES" },
            { label: "RSA", value: "RSA" },
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
  mapping: StreamMapperWithId<EncryptionMapperConfiguration>;
  streamName: string;
}> = ({ mapping, streamName }) => {
  const fieldsInStream = useGetFieldsInStream(streamName);
  const [algorithm, setAlgorithm] = useState<EncryptionMapperAlgorithm>(mapping.mapperConfiguration.algorithm || "RSA");

  if (!mapping) {
    return null;
  }

  if (algorithm === "AES") {
    const aesMapping = mapping as StreamMapperWithId<EncryptionMapperConfiguration<"AES">>;

    return (
      <AESForm
        streamName={streamName}
        mapping={aesMapping}
        setAlgorithm={setAlgorithm}
        targetFieldOptions={fieldsInStream}
      />
    );
  }

  if (algorithm === "RSA") {
    const rsaMapping = mapping as StreamMapperWithId<EncryptionMapperConfiguration<"RSA">>;

    return (
      <RSAForm
        streamName={streamName}
        mapping={rsaMapping}
        setAlgorithm={setAlgorithm}
        targetFieldOptions={fieldsInStream}
      />
    );
  }

  return null;
};
