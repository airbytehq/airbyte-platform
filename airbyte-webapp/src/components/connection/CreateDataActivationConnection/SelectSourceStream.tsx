import { Listbox } from "@headlessui/react";
import { useMemo } from "react";
import { Controller, useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { ConnectorIcon } from "components/ConnectorIcon";
import { FormControlErrorMessage } from "components/forms/FormControl";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { FloatLayout } from "components/ui/ListBox/FloatLayout";
import { ListboxButton } from "components/ui/ListBox/ListboxButton";
import { ListboxOption } from "components/ui/ListBox/ListboxOption";
import { ListboxOptions } from "components/ui/ListBox/ListboxOptions";
import { Text } from "components/ui/Text";

import { useGetSourceFromSearchParams } from "area/connector/utils";
import { useDiscoverSchemaQuery } from "core/api";

import styles from "./SelectSourceStream.module.scss";
import { EMPTY_FIELD, StreamMappingsFormValues } from "./StreamMappings";

export const SelectSourceStream = ({ index }: { index: number }) => {
  const { control, setValue } = useFormContext<StreamMappingsFormValues>();
  const source = useGetSourceFromSearchParams();
  const { data: sourceSchema } = useDiscoverSchemaQuery(source.sourceId);

  const sourceStreams = useMemo(() => sourceSchema?.catalog?.streams ?? [], [sourceSchema]);

  const showNamespace = useMemo(
    () => new Set(sourceStreams.map((stream) => stream.stream?.namespace)).size > 1,
    [sourceStreams]
  );

  const sourceStreamOptions = useMemo(
    () =>
      sourceStreams
        .map((stream) => ({
          label: showNamespace ? `${stream.stream?.namespace}.${stream.stream?.name}` : stream.stream?.name,
          value: { name: stream.stream?.name, namespace: stream.stream?.namespace },
        }))
        .sort((a, b) => {
          return a.label?.localeCompare(b.label ?? "", undefined, { numeric: true }) ?? 0;
        }),
    [sourceStreams, showNamespace]
  );

  return (
    <div className={styles.selectSourceStream}>
      <Controller
        name={`streams.${index}.sourceStreamDescriptor`}
        control={control}
        render={({ field, fieldState }) => (
          <FlexContainer direction="column" gap="xs">
            <Listbox
              onChange={(value) => {
                field.onChange(value);
                // Changing the stream resets the sync mode and selected fieldss
                setValue(`streams.${index}.sourceSyncMode`, null);
                setValue(`streams.${index}.fields`, [EMPTY_FIELD]);
              }}
              value={field.value}
            >
              <FloatLayout adaptiveWidth>
                <ListboxButton hasError={!!fieldState.error}>
                  <FlexContainer alignItems="center" as="span">
                    <ConnectorIcon icon={source.icon} className={styles.selectSourceStream__icon} />
                    <Box py="md" as="span">
                      {field.value?.name ? (
                        <Text size="lg">{field.value.name}</Text>
                      ) : (
                        <Text size="lg" color="grey">
                          <FormattedMessage id="connection.create.selectSourceStream" />
                        </Text>
                      )}
                    </Box>
                  </FlexContainer>
                </ListboxButton>
                <ListboxOptions>
                  {sourceStreamOptions.map(({ label, value }, index) => {
                    return (
                      <ListboxOption value={value} key={index}>
                        {() => (
                          <Box p="md" pr="none" as="span">
                            <Text size="lg">{label}</Text>
                          </Box>
                        )}
                      </ListboxOption>
                    );
                  })}
                </ListboxOptions>
              </FloatLayout>
            </Listbox>
            {fieldState.error && <FormControlErrorMessage name={field.name} />}
          </FlexContainer>
        )}
      />
    </div>
  );
};
