import { useCallback, useEffect, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { Navigate, useParams } from "react-router-dom";

import { FlexContainer } from "components/ui/Flex";
import { Link } from "components/ui/Link";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { Message } from "components/ui/Message";
import { Text } from "components/ui/Text";

import { useAuthService } from "core/services/auth";
import { CloudRoutes } from "packages/cloud/cloudRoutePaths";

import styles from "./SSOBookmarkPage.module.scss";

// A bookmarkable route that redirects to the SSO login page with the provided company identifier
export const SSOBookmarkPage = () => {
  const { changeRealmAndRedirectToSignin } = useAuthService();
  const { companyIdentifier } = useParams();
  const [state, setState] = useState<"loading" | "error">("loading");
  const { formatMessage } = useIntl();

  if (!changeRealmAndRedirectToSignin) {
    throw new Error("Rendered SSOBookmarkPage while AuthService does not provide changeRealmAndRedirectToSignin");
  }

  const validateCompanyIdentifier = useCallback(
    async (companyIdentifier: string) => {
      try {
        return await changeRealmAndRedirectToSignin(companyIdentifier);
      } catch (e) {
        setState("error");
        return Promise.reject(formatMessage({ id: "login.sso.invalidCompanyIdentifier" }));
      }
    },
    [changeRealmAndRedirectToSignin, formatMessage]
  );

  useEffect(() => {
    if (!companyIdentifier) {
      return;
    }

    validateCompanyIdentifier(companyIdentifier);
  }, [validateCompanyIdentifier, companyIdentifier]);

  if (!companyIdentifier) {
    return <Navigate to={CloudRoutes.Login} />;
  }

  if (state === "loading") {
    return <LoadingSpinner />;
  }

  return (
    <FlexContainer direction="column" className={styles.ssoBookmarkPage} gap="xl">
      <FlexContainer alignItems="center">
        <Message
          type="error"
          text={<FormattedMessage id="login.sso.companyIdentifierNotFound" values={{ companyIdentifier }} />}
        />
      </FlexContainer>
      <Text>
        <Link to={CloudRoutes.Sso}>
          <FormattedMessage id="login.sso.backToSsoLogin" />
        </Link>
      </Text>
    </FlexContainer>
  );
};
