import { useMemo, useState } from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Pre } from "components/ui/Pre";
import { Text } from "components/ui/Text";

import { StreamReadAuxiliaryRequestsItem } from "core/api/types/ConnectorBuilderClient";

import styles from "./GlobalRequestsDisplay.module.scss";
import { InnerListBox } from "./InnerListBox";
import { TabData, TabbedDisplay } from "./TabbedDisplay";
import { formatForDisplay } from "../utils";

interface GlobalRequestsDisplayProps {
  requests: StreamReadAuxiliaryRequestsItem[];
}

export const GlobalRequestsDisplay: React.FC<GlobalRequestsDisplayProps> = ({ requests }) => {
  const [selectedRequestIndex, setSelectedRequestIndex] = useState<number>(0);
  const selectedRequest = requests[selectedRequestIndex];

  const formattedRequest = useMemo(() => formatForDisplay(selectedRequest.request), [selectedRequest.request]);
  const formattedResponse = useMemo(() => formatForDisplay(selectedRequest.response), [selectedRequest.response]);

  const tabs: TabData[] = [
    ...(selectedRequest.request
      ? [
          {
            key: "request",
            title: <FormattedMessage id="connectorBuilder.requestTab" />,
            content: <Pre>{formattedRequest}</Pre>,
          },
        ]
      : []),
    ...(selectedRequest.response
      ? [
          {
            key: "response",
            title: <FormattedMessage id="connectorBuilder.responseTab" />,
            content: <Pre>{formattedResponse}</Pre>,
          },
        ]
      : []),
  ];

  return (
    <FlexContainer className={styles.container} direction="column">
      <FlexContainer>
        <InnerListBox
          data-testid="tag-select-slice"
          options={requests.map((request, index) => {
            return { label: request.title ?? index, value: index };
          })}
          selectedValue={selectedRequestIndex}
          onSelect={(selected) => setSelectedRequestIndex(selected)}
        />
        {requests[selectedRequestIndex].description && (
          <FlexItem grow className={styles.description}>
            <Text>{requests[selectedRequestIndex].description}</Text>
          </FlexItem>
        )}
      </FlexContainer>
      <TabbedDisplay tabs={tabs} />
    </FlexContainer>
  );
};
