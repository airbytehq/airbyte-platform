import { useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { CheckBox } from "components/ui/CheckBox";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { ReleaseStage } from "core/request/AirbyteClient";

import styles from "./FilterReleaseStage.module.scss";

interface FilterReleaseStageProps {
  selectedReleaseStages: ReleaseStage[];
  onUpdateSelectedReleaseStages: (newReleaseStages: ReleaseStage[]) => void;
  availableReleaseStages: ReleaseStage[];
}

export const FilterReleaseStage: React.FC<FilterReleaseStageProps> = ({
  selectedReleaseStages,
  onUpdateSelectedReleaseStages,
  availableReleaseStages,
}) => {
  const handleChange = (stage: ReleaseStage, isSelected: boolean) => {
    if (isSelected) {
      onUpdateSelectedReleaseStages([...selectedReleaseStages, stage]);
    } else {
      onUpdateSelectedReleaseStages(selectedReleaseStages.filter((s) => s !== stage));
    }
  };

  // It's possible that there are no custom connectors, so that filter is hidden. But that filter might
  // still be technically selected, because we cache the user's selection in local storage.
  // In that case we want to know how many of the filters that _are_ visible have been selected.
  const numberOfVisiblySelectedReleaseStages = useMemo(() => {
    return selectedReleaseStages.filter((stage) => availableReleaseStages.includes(stage)).length;
  }, [selectedReleaseStages, availableReleaseStages]);

  return (
    <FlexContainer gap="xl">
      <Text>
        <FormattedMessage id="connector.filterBy" />
      </Text>
      {availableReleaseStages.map((stage) => {
        const id = `filter-release-stage-${stage}`;
        const isChecked = selectedReleaseStages.includes(stage);
        return (
          <label htmlFor={id} className={styles.checkboxLabel} key={id}>
            <FlexContainer alignItems="center" gap="sm">
              <CheckBox
                checkboxSize="sm"
                checked={isChecked}
                disabled={isChecked && numberOfVisiblySelectedReleaseStages <= 1}
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
