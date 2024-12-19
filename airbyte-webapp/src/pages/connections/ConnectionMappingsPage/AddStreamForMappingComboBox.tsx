import classNames from "classnames";
import { useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { ComboBox } from "components/ui/ComboBox";
import { Tooltip } from "components/ui/Tooltip";

import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

import styles from "./AddStreamForMappingComboBox.module.scss";
import { getKeyForStream, useMappingContext } from "./MappingContext";
import { useGetStreamsForNewMapping } from "./useGetStreamsForNewMappings";

export const AddStreamForMappingComboBox: React.FC<{ secondary?: boolean }> = ({ secondary = false }) => {
  const { mode } = useConnectionFormService();
  const [selectedStream, setSelectedStream] = useState<string | undefined>(undefined);
  const streamsToList = useGetStreamsForNewMapping();
  const { addStreamToMappingsList } = useMappingContext();
  const { formatMessage } = useIntl();

  const placeholder = secondary
    ? formatMessage({ id: "connections.mappings.addStream" })
    : formatMessage({ id: "connections.mappings.selectAStream" });

  const onChange = (streamDescriptorKey: string) => {
    setSelectedStream(streamDescriptorKey);
    addStreamToMappingsList(streamDescriptorKey);
  };

  const options = streamsToList?.map((stream) => ({
    label: stream.stream?.name || "",
    value: stream.stream ? getKeyForStream(stream.stream) : "",
  }));
  const disabled = !options || options.length === 0 || mode === "readonly";

  return (
    <>
      {!disabled ? (
        <ComboBox
          className={classNames(styles.addStreamForMappingComboBox, {
            [styles.disabled]: disabled,
            [styles["addStreamForMappingComboBox--secondary"]]: secondary,
          })}
          value={selectedStream}
          onChange={onChange}
          allowCustomValue={false}
          options={options}
          placeholder={placeholder}
          disabled={disabled}
        />
      ) : (
        <Tooltip
          control={
            <Button variant="secondary" disabled>
              {placeholder}
            </Button>
          }
        >
          <FormattedMessage id="connections.mappings.addStream.disabled" />
        </Tooltip>
      )}
    </>
  );
};
