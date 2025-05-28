import isEmpty from "lodash/isEmpty";
import { useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { Collapsible } from "components/ui/Collapsible";

import { BuilderCard } from "./BuilderCard";
import { BuilderYamlField } from "./BuilderYamlField";
import styles from "./UnknownFieldsSection.module.scss";
import { AnyDeclarativeStreamPathFn } from "../types";
import { useBuilderWatch } from "../useBuilderWatch";

interface UnknownFieldsSectionProps {
  streamFieldPath: AnyDeclarativeStreamPathFn;
}

export const UnknownFieldsSection: React.FC<UnknownFieldsSectionProps> = ({ streamFieldPath }) => {
  const { getFieldState } = useFormContext();
  const unknownFields = useBuilderWatch(streamFieldPath("unknownFields"));
  const { error } = getFieldState(streamFieldPath("unknownFields"));

  return (
    <Collapsible
      className={styles.collapsible}
      label="Additional fields"
      initiallyOpen={!isEmpty(unknownFields)}
      infoTooltipContent={<FormattedMessage id="connectorBuilder.unknownFields.tooltip" />}
      showErrorIndicator={!!error}
    >
      <BuilderCard>
        <BuilderYamlField path={streamFieldPath("unknownFields")} />
      </BuilderCard>
    </Collapsible>
  );
};
