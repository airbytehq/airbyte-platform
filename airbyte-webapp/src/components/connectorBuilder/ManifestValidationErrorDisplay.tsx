import { FormattedMessage, useIntl } from "react-intl";

import { Collapsible } from "components/ui/Collapsible";
import { FlexContainer } from "components/ui/Flex";
import { Pre } from "components/ui/Pre";
import { Text } from "components/ui/Text";

import styles from "./ManifestValidationErrorDisplay.module.scss";
import { ManifestValidationError } from "./utils";

export const ManifestValidationErrorDisplay = ({
  error: { errors, nestedErrors },
}: {
  error: ManifestValidationError;
}) => {
  const { formatMessage } = useIntl();

  return (
    <FlexContainer direction="column" gap="none" className={styles.container}>
      <Text bold>
        <FormattedMessage id="connectorBuilder.invalidManifestError" />
      </Text>
      <ul>
        {errors.map((error, index) => (
          <li key={`${error}__${index}`}>
            <Pre wrapText>{error}</Pre>
          </li>
        ))}
      </ul>
      {nestedErrors && (
        <Collapsible label={formatMessage({ id: "connectorBuilder.viewFullErrors" })} className={styles.collapsible}>
          <ul>
            {nestedErrors.map((error, index) => (
              <li key={`${error}__${index}`}>
                <Pre wrapText>{error}</Pre>
              </li>
            ))}
          </ul>
        </Collapsible>
      )}
    </FlexContainer>
  );
};
