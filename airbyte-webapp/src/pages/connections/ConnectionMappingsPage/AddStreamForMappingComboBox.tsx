import { useState } from "react";
import { useIntl } from "react-intl";

import { ComboBox } from "components/ui/ComboBox";

import styles from "./AddStreamForMappingComboBox.module.scss";
import { useGetStreamsForNewMapping } from "./useGetStreamsForNewMappings";

interface AddStreamForMappingProps {
  onStreamSelected: (streamName: string) => void;
}

export const AddStreamForMappingComboBox: React.FC<AddStreamForMappingProps> = ({ onStreamSelected }) => {
  const [selectedStream, setSelectedStream] = useState<string | undefined>(undefined);
  const streamsToList = useGetStreamsForNewMapping();
  const { formatMessage } = useIntl();

  const onChange = (streamName: string) => {
    setSelectedStream(streamName);
    onStreamSelected(streamName);
  };

  const options = streamsToList?.map((stream) => ({
    label: stream.stream?.name || "",
    value: stream.stream?.name || "",
  }));

  return (
    <ComboBox
      className={styles.addStreamForMappingComboBox}
      value={selectedStream}
      onChange={onChange}
      allowCustomValue={false}
      options={options}
      placeholder={formatMessage({ id: "connection.mappings.selectAStream" })}
    />
  );
};
