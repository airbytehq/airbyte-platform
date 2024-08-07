import { useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { createSearchParams, useSearchParams } from "react-router-dom";

import { Button } from "components/ui/Button";
import { Icon } from "components/ui/Icon";
import { Link } from "components/ui/Link";
import { LoadingSpinner } from "components/ui/LoadingSpinner";

import { useAuthService } from "core/services/auth";
import { trackError } from "core/utils/datadog";
import { useNotificationService } from "hooks/services/Notification";
import { CloudRoutes } from "packages/cloud/cloudRoutePaths";

import githubLogo from "./assets/github-logo.svg";
import googleLogo from "./assets/google-logo.svg";
import styles from "./LoginButtons.module.scss";

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

const SignInButton: React.FC<LoginButtonProps> = ({ onClick, pendingRedirect }) => {
  return (
    <button className={styles.login} onClick={onClick}>
      {pendingRedirect === "password" ? <LoadingSpinner /> : <Icon type="envelope" />}
      <FormattedMessage id="login.email" />
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

interface LoginButtonsProps {
  type: "login" | "signup";
}

export const LoginButtons: React.FC<LoginButtonsProps> = ({ type }) => {
  const { formatMessage } = useIntl();
  const { registerNotification } = useNotificationService();
  const [pendingRedirect, setPendingRedirect] = useState<"google" | "github" | "password" | null>(null);
  const {
    redirectToSignInWithGithub,
    redirectToSignInWithGoogle,
    redirectToSignInWithPassword,
    redirectToRegistrationWithPassword,
  } = useAuthService();

  const doRedirectToSignInWithGithub = () => {
    setPendingRedirect("github");
    redirectToSignInWithGithub?.().catch(() => setPendingRedirect(null));
  };

  const doRedirectToSignInWithGoogle = () => {
    setPendingRedirect("google");
    redirectToSignInWithGoogle?.().catch(() => setPendingRedirect(null));
  };

  const handleEmailButtonClick = () => {
    setPendingRedirect("password");
    if (type === "signup") {
      redirectToRegistrationWithPassword?.().catch((error) => {
        trackError(error);
        registerNotification({
          id: "redirectToRegistrationWithPasswordError",
          type: "error",
          text: formatMessage({ id: "signup.email.redirectError" }),
        });
        setPendingRedirect(null);
      });
    } else {
      redirectToSignInWithPassword?.().catch((error) => {
        trackError(error);
        registerNotification({
          id: "redirectToSignInWithPasswordError",
          type: "error",
          text: formatMessage({ id: "login.email.redirectError" }),
        });
        setPendingRedirect(null);
      });
    }
  };

  return (
    <>
      {redirectToSignInWithGoogle && (
        <GoogleButton onClick={() => doRedirectToSignInWithGoogle()} pendingRedirect={pendingRedirect} />
      )}
      {redirectToSignInWithGithub && (
        <GitHubButton onClick={() => doRedirectToSignInWithGithub()} pendingRedirect={pendingRedirect} />
      )}
      <SsoButton />
      {type === "login" ? (
        <SignInButton onClick={handleEmailButtonClick} pendingRedirect={pendingRedirect} />
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
      )}
    </>
  );
};
