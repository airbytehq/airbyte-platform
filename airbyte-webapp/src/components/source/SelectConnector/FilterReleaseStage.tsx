import { FormattedMessage } from "react-intl";

import { CheckBox } from "components/ui/CheckBox";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { ReleaseStage } from "core/request/AirbyteClient";

import styles from "./FilterReleaseStage.module.scss";

interface FilterReleaseStageProps {
  selectedReleaseStages: ReleaseStage[];
  onUpdateSelectedReleaseStages: React.Dispatch<React.SetStateAction<ReleaseStage[]>>;
  availableReleaseStages: ReleaseStage[];
}

export const FilterReleaseStage: React.FC<FilterReleaseStageProps> = ({
  selectedReleaseStages,
  onUpdateSelectedReleaseStages,
  availableReleaseStages,
}) => {
  const handleChange = (stage: ReleaseStage, isSelected: boolean) => {
    if (isSelected) {
      onUpdateSelectedReleaseStages((stages) => [...stages, stage]);
    } else {
      onUpdateSelectedReleaseStages((stages) => stages.filter((s) => s !== stage));
    }
  };

  return (
    <FlexContainer gap="xl">
      {availableReleaseStages.map((stage) => {
        const id = `filter-release-stage-${stage}`;
        const isChecked = selectedReleaseStages.includes(stage);
        return (
          <label htmlFor={id} className={styles.checkboxLabel}>
            <FlexContainer alignItems="center" gap="sm">
              <CheckBox
                checkboxSize="sm"
                checked={isChecked}
                onChange={() => handleChange(stage, !isChecked)}
                id={id}
              />
              <Text>
                <ReleaseStageLabel stage={stage} />
              </Text>
            </FlexContainer>
          </label>
        );
      })}
    </FlexContainer>
  );
};

interface ReleaseStageLabelProps {
  stage: ReleaseStage;
}

const ReleaseStageLabel: React.FC<ReleaseStageLabelProps> = ({ stage }) => {
  switch (stage) {
    case "generally_available":
      return <FormattedMessage id="connector.releaseStage.generallyAvailable.expanded" />;
    case "beta":
      return <FormattedMessage id="connector.releaseStage.beta" />;
    case "alpha":
      return <FormattedMessage id="connector.releaseStage.alpha" />;
    case "custom":
      return <FormattedMessage id="connector.releaseStage.custom" />;
  }
};
