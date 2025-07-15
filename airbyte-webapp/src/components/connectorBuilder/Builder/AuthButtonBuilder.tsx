import { FormattedMessage } from "react-intl";

import { removeFieldFocusedAttribute } from "components/connectorBuilder/useFocusField";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";

import styles from "./AuthButtonBuilder.module.scss";
import { useFormOauthAdapterBuilder } from "../../../views/Connector/ConnectorForm/components/Sections/auth/useOauthFlowAdapter";

export const AuthButtonBuilder = ({
  builderProjectId,
  onComplete,
  onClick,
  disabled,
  "data-field-path": dataFieldPath,
}: {
  builderProjectId: string;
  onComplete: (authPayload: Record<string, unknown>) => void;
  onClick?: () => void;
  disabled?: boolean;
  "data-field-path"?: string;
}) => {
  const { loading, run } = useFormOauthAdapterBuilder(builderProjectId, onComplete);

  return (
    <FlexContainer alignItems="center" data-field-path={dataFieldPath} className={styles.container}>
      <Button
        disabled={disabled}
        isLoading={loading}
        type="button"
        data-testid="oauth-button"
        onClick={() => {
          dataFieldPath && removeFieldFocusedAttribute(dataFieldPath);
          (onClick ?? run)();
        }}
        className={styles.button}
      >
        <FormattedMessage id="connectorBuilder.authentication.oauthButton.label" />
      </Button>
    </FlexContainer>
  );
};
