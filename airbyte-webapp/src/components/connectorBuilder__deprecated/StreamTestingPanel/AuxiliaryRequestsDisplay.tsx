import { useEffect, useMemo, useState } from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Pre } from "components/ui/Pre";
import { Text } from "components/ui/Text";

import { AuxiliaryRequest } from "core/api/types/ConnectorBuilderClient";

import styles from "./AuxiliaryRequestsDisplay.module.scss";
import { InnerListBox } from "./InnerListBox";
import { TabData, TabbedDisplay } from "./TabbedDisplay";
import { formatForDisplay } from "../utils";

interface AuxiliaryRequestsDisplayProps {
  globalRequests?: AuxiliaryRequest[];
  sliceRequests?: AuxiliaryRequest[];
  sliceIndex?: number;
}

export const AuxiliaryRequestsDisplay: React.FC<AuxiliaryRequestsDisplayProps> = ({
  globalRequests = [],
  sliceRequests = [],
  sliceIndex,
}) => {
  const [selectedRequestIndex, setSelectedRequestIndex] = useState<number>(0);

  const allRequests = useMemo(() => {
    return [...globalRequests, ...sliceRequests];
  }, [globalRequests, sliceRequests]);

  // Reset selected index when the requests change
  useEffect(() => {
    setSelectedRequestIndex(0);
  }, [globalRequests, sliceRequests, sliceIndex]);

  const selectedRequest = allRequests[selectedRequestIndex];

  const formattedRequest = useMemo(() => formatForDisplay(selectedRequest.request), [selectedRequest.request]);
  const formattedResponse = useMemo(() => formatForDisplay(selectedRequest.response), [selectedRequest.response]);

  // If there are no requests, don't render anything
  if (allRequests.length === 0) {
    return null;
  }

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
          data-testid="tag-select-request"
          options={allRequests.map((request, index) => {
            return {
              label: request.title,
              value: index,
            };
          })}
          selectedValue={selectedRequestIndex}
          onSelect={(selected) => setSelectedRequestIndex(selected)}
        />
        <FlexItem grow className={styles.description}>
          <Text>{allRequests[selectedRequestIndex].description}</Text>
        </FlexItem>
      </FlexContainer>
      <TabbedDisplay tabs={tabs} />
    </FlexContainer>
  );
};
