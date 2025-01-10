import { Combobox, ComboboxButton, ComboboxInput, ComboboxOption, ComboboxOptions } from "@headlessui/react";
import { Float } from "@headlessui-float/react";
import classNames from "classnames";
import { Fragment, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { Icon } from "components/ui/Icon";
import { Input } from "components/ui/Input";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

import styles from "./AddStreamForMappingComboBox.module.scss";
import { getKeyForStream, getStreamDescriptorForKey, useMappingContext } from "./MappingContext";
import { useGetStreamsForNewMapping } from "./useGetStreamsForNewMappings";

export const AddStreamForMappingComboBox: React.FC<{ secondary?: boolean }> = ({ secondary = false }) => {
  const { mode } = useConnectionFormService();
  const [selectedStream, setSelectedStream] = useState<string | undefined>(undefined);
  const [query, setQuery] = useState<string>("");
  const streamsToList = useGetStreamsForNewMapping();
  const { addStreamToMappingsList } = useMappingContext();
  const { formatMessage } = useIntl();

  const placeholder = secondary
    ? formatMessage({ id: "connections.mappings.addStream" })
    : formatMessage({ id: "connections.mappings.selectAStream" });

  const options = streamsToList?.map((stream) => ({
    label: stream.stream?.name || "",
    value: stream.stream ? getKeyForStream(stream.stream) : "",
  }));

  const filteredOptions =
    query === "" ? options : options.filter((option) => getStreamDescriptorForKey(option.value).name.includes(query));

  const disabled = !options || options.length === 0 || mode === "readonly";

  const handleStreamSelect = (value: string) => {
    const streamDescriptorKey = filteredOptions.find((option) => option.label === value)?.value;

    if (!streamDescriptorKey) {
      setQuery("");
    } else {
      setSelectedStream(streamDescriptorKey);
      addStreamToMappingsList(streamDescriptorKey);
    }
  };

  return (
    <>
      {!disabled ? (
        <Combobox
          value={selectedStream ?? ""}
          as="div"
          disabled={disabled}
          onChange={handleStreamSelect}
          onClose={() => setQuery("")}
          immediate
          data-testid="add-stream-for-mapping-combobox"
          className={classNames(styles.addStreamForMappingComboBox, {
            [styles.disabled]: disabled,
            [styles["addStreamForMappingComboBox--secondary"]]: secondary,
          })}
        >
          <Float
            adaptiveWidth
            placement="bottom-start"
            flip={5}
            offset={-10} // this gives the same gap as our ListBox
            autoUpdate={{
              elementResize: false, // this will prevent render in wrong place after multiple open/close actions
            }}
          >
            <ComboboxInput as={Fragment}>
              {({ open }) => (
                <Input
                  disabled={disabled}
                  spellCheck={false}
                  autoComplete="off"
                  aria-label={formatMessage({ id: "connections.mappings.addStream" })}
                  placeholder={open ? formatMessage({ id: "connections.mappings.selectAStream" }) : placeholder}
                  value={query || ""}
                  containerClassName={classNames({
                    [styles.disabled]: disabled,
                  })}
                  onChange={(e) => setQuery(e.target.value)}
                  adornment={
                    <ComboboxButton
                      className={styles.caretButton}
                      aria-label={formatMessage({ id: "connections.mappings.addStream" })}
                    >
                      <Icon type="caretDown" />
                    </ComboboxButton>
                  }
                />
              )}
            </ComboboxInput>
            <ComboboxOptions as="ul" className={styles.comboboxOptions}>
              {filteredOptions.map(({ label }) => (
                <ComboboxOption as="li" key={label} value={label} className={styles.comboboxOption}>
                  <Text>{label}</Text>
                </ComboboxOption>
              ))}
            </ComboboxOptions>
          </Float>
        </Combobox>
      ) : mode !== "readonly" ? (
        <Tooltip
          control={
            <Button variant="secondary" disabled>
              {placeholder}
            </Button>
          }
        >
          <FormattedMessage id="connections.mappings.addStream.disabled" />
        </Tooltip>
      ) : (
        <Button variant="secondary" disabled>
          {placeholder}
        </Button>
      )}
    </>
  );
};
