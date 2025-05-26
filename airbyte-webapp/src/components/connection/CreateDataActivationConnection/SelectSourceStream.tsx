import { Listbox } from "@headlessui/react";
import { Controller, useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { ConnectorIcon } from "components/ConnectorIcon";
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
import { StreamMappingsFormValues } from "./StreamMappings";

export const SelectSourceStream = ({ index }: { index: number }) => {
  const { control, setValue } = useFormContext<StreamMappingsFormValues>();
  const source = useGetSourceFromSearchParams();
  const { data: sourceSchema } = useDiscoverSchemaQuery(source.sourceId);

  const sourceStreams = sourceSchema?.catalog?.streams ?? [];

  const sourceStreamOptions = sourceStreams
    .map((stream) => ({
      label: stream.stream?.name,
      value: { name: stream.stream?.name, namespace: stream.stream?.namespace },
    }))
    .sort((a, b) => {
      return a.label?.localeCompare(b.label ?? "", undefined, { numeric: true }) ?? 0;
    });

  return (
    <div className={styles.selectSourceStream}>
      <Controller
        name={`streams.${index}.sourceStreamDescriptor`}
        control={control}
        render={({ field: { onChange, value } }) => (
          <Listbox
            onChange={(value) => {
              onChange(value);
              // Changing the stream resets the sync mode and selected fieldss
              setValue(`streams.${index}.sourceSyncMode`, undefined);
              setValue(`streams.${index}.fields`, []);
            }}
            value={value}
          >
            <FloatLayout adaptiveWidth>
              <ListboxButton>
                <FlexContainer alignItems="center" as="span">
                  <ConnectorIcon icon={source.icon} className={styles.selectSourceStream__icon} />
                  <Box py="md" as="span">
                    {value?.name ? (
                      <Text size="lg">{value.name}</Text>
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
        )}
      />
    </div>
  );
};
