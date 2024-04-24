import { useRef, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { createSearchParams, useNavigate, useSearchParams } from "react-router-dom";
import { useUnmount } from "react-use";
import { Subscription } from "rxjs";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Link } from "components/ui/Link";
import { LoadingSpinner } from "components/ui/LoadingSpinner";
import { SignInButton } from "components/ui/SignInButton";
import { Spinner } from "components/ui/Spinner";

import { AuthOAuthLogin, OAuthProviders } from "core/services/auth";
import { useLocalStorage } from "core/utils/useLocalStorage";
import { useExperiment } from "hooks/services/Experiment";
import { CloudRoutes } from "packages/cloud/cloudRoutePaths";
import { useKeycloakService } from "packages/cloud/services/auth/KeycloakService";

import githubLogo from "./assets/github-logo.svg";
import googleLogo from "./assets/google-logo.svg";
import styles from "./OAuthLogin.module.scss";

type PendingRedirect = "github" | "google" | "password" | null;

interface LoginButtonProps {
  onClick: () => void;
  pendingRedirect: PendingRedirect;
}

const GitHubButton: React.FC<LoginButtonProps> = ({ pendingRedirect, onClick }) => {
  return (
    <button className={styles.github} onClick={onClick} data-testid="githubOauthLogin" disabled={!!pendingRedirect}>
      {pendingRedirect === "github" ? <LoadingSpinner /> : <img src={githubLogo} alt="" />}
      <FormattedMessage id="login.oauth.github" tagName="span" />
    </button>
  );
};

const GoogleButton: React.FC<LoginButtonProps> = ({ onClick, pendingRedirect }) => {
  return (
    <button className={styles.google} onClick={onClick} data-testid="googleOauthLogin" disabled={!!pendingRedirect}>
      {pendingRedirect === "google" ? <LoadingSpinner /> : <img src={googleLogo} alt="" />}
      <FormattedMessage id="login.oauth.google" tagName="span" />
    </button>
  );
};

const SsoButton: React.FC = () => {
  const [searchParams] = useSearchParams();
  const loginRedirectString = searchParams.get("loginRedirect");
  const linkLocation = loginRedirectString
    ? { pathname: CloudRoutes.Sso, search: createSearchParams({ loginRedirect: loginRedirectString }).toString() }
    : CloudRoutes.Sso;

  return (
    <Link className={styles.sso} to={linkLocation}>
      <Icon type="idCard" />
      <FormattedMessage id="login.sso.continueWithSSO" tagName="span" />
    </Link>
  );
};

interface OAuthLoginProps {
  type: "login" | "signup";
  loginWithOAuth: AuthOAuthLogin;
}

export const OAuthLogin: React.FC<OAuthLoginProps> = ({ loginWithOAuth, type }) => {
  const [pendingRedirect, setPendingRedirect] = useState<"google" | "github" | "password" | null>(null);
  const { formatMessage } = useIntl();
  const stateSubscription = useRef<Subscription>();
  const [errorCode, setErrorCode] = useState<string>();
  const [isLoading, setLoading] = useState(false);
  const [searchParams] = useSearchParams();
  const loginRedirect = searchParams.get("loginRedirect");
  const navigate = useNavigate();
  const [keycloakAuthEnabledLocalStorage] = useLocalStorage("airbyte_keycloak-auth-ui", true);
  const keycloakAuthEnabledExperiment = useExperiment("authPage.keycloak", true);
  const keycloakAuthEnabled = keycloakAuthEnabledExperiment || keycloakAuthEnabledLocalStorage;
  const {
    redirectToSignInWithGithub,
    redirectToSignInWithGoogle,
    redirectToSignInWithPassword,
    redirectToRegistrationWithPassword,
  } = useKeycloakService();

  useUnmount(() => {
    stateSubscription.current?.unsubscribe();
  });

  const getErrorMessage = (error: string): string | undefined => {
    switch (error) {
      // The following error codes are not really errors, thus we'll ignore them.
      case "auth/popup-closed-by-user":
      case "auth/user-cancelled":
      case "auth/cancelled-popup-request":
        return undefined;
      case "auth/account-exists-with-different-credential":
        // Happens if a user requests and sets a password for an originally OAuth account.
        // From them on they can't login via OAuth anymore unless it's Google OAuth.
        return formatMessage({ id: "login.oauth.differentCredentialsError" });
      default:
        return formatMessage({ id: "login.oauth.unknownError" }, { error });
    }
  };

  const login = (provider: OAuthProviders) => {
    setErrorCode(undefined);
    stateSubscription.current?.unsubscribe();
    stateSubscription.current = loginWithOAuth(provider).subscribe({
      next: (value) => {
        if (value === "loading") {
          setLoading(true);
        }
        if (value === "done") {
          setLoading(false);
          navigate(loginRedirect ?? "/", { replace: true });
        }
      },
      error: (error) => {
        if ("code" in error && typeof error.code === "string") {
          setErrorCode(error.code);
        }
      },
    });
  };

  const errorMessage = errorCode ? getErrorMessage(errorCode) : undefined;

  const doRedirectToSignInWithGithub = () => {
    setPendingRedirect("github");
    redirectToSignInWithGithub().catch(() => setPendingRedirect(null));
  };

  const doRedirectToSignInWithGoogle = () => {
    setPendingRedirect("google");
    redirectToSignInWithGoogle().catch(() => setPendingRedirect(null));
  };

  const handleEmailButtonClick = () => {
    setPendingRedirect("password");
    if (type === "signup") {
      redirectToRegistrationWithPassword().catch(() => setPendingRedirect(null));
    } else {
      redirectToSignInWithPassword().catch(() => setPendingRedirect(null));
    }
  };

  return (
    <>
      {isLoading && (
        <FlexContainer justifyContent="center" alignItems="center" className={styles.spinner}>
          <Spinner />
        </FlexContainer>
      )}
      {!isLoading && (
        <>
          <GoogleButton
            onClick={() => (keycloakAuthEnabled ? doRedirectToSignInWithGoogle() : login("google"))}
            pendingRedirect={pendingRedirect}
          />
          <GitHubButton
            onClick={() => (keycloakAuthEnabled ? doRedirectToSignInWithGithub() : login("github"))}
            pendingRedirect={pendingRedirect}
          />
          <SsoButton />

          {keycloakAuthEnabled &&
            (type === "login" ? (
              <SignInButton onClick={handleEmailButtonClick} disabled={!!pendingRedirect}>
                {pendingRedirect === "password" ? <LoadingSpinner /> : <Icon type="envelope" />}
                <FormattedMessage id="login.email" />
              </SignInButton>
            ) : (
              <Button
                onClick={handleEmailButtonClick}
                variant="clear"
                isLoading={pendingRedirect === "password"}
                size="sm"
                icon="envelope"
              >
                <FormattedMessage id="signup.method.email" />
              </Button>
            ))}
        </>
      )}
      {errorMessage && <div className={styles.error}>{errorMessage}</div>}
    </>
  );
};
