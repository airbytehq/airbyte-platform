import React, { useCallback, useEffect, useRef, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate, useSearchParams } from "react-router-dom";

import { Button } from "components/ui/Button";
import { CheckBox } from "components/ui/CheckBox";
import { Heading } from "components/ui/Heading";
import { HeadTitle } from "components/ui/HeadTitle";
import { ExternalLink, Link } from "components/ui/Link";
import LoadingPage from "components/ui/LoadingPage";
import { Text } from "components/ui/Text";

import { useGetDestinationFromParams, useGetSourceFromParams } from "area/connector/utils";
import { useCurrentOrganizationId } from "area/organization/utils";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import {
  fusionEnablementState,
  useCurrentOrganizationInfo,
  useEnrollOrganizationInAgents,
  useFusionActorEnablement,
  useSetFusionActorEnablement,
} from "core/api";
import { DefaultErrorBoundary } from "core/errors";
import { links } from "core/utils/links";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { RoutePaths } from "pages/routePaths";

import { useShowActorContextLayerToggles } from "./ActorContextLayerToggles";
import styles from "./EnableDirectAccessPage.module.scss";
import { useContextLayerEligibility } from "./useContextLayerEligibility";

type ActorType = "source" | "destination";

interface EnableDirectAccessPageProps {
  actorType: ActorType;
}

interface EnableDirectAccessContentProps extends EnableDirectAccessPageProps {
  actorId: string;
  actorName: string;
  definitionName: string;
}

const OrganizationEnrollmentNotice: React.FC = () => {
  const organizationInfo = useCurrentOrganizationInfo();

  return (
    <Text>
      <FormattedMessage
        id="cloud.contextLayer.enableDirectAccess.organization"
        values={{ orgName: organizationInfo?.organizationName ?? "" }}
      />
    </Text>
  );
};

const ContextLayerTerms: React.FC = () => (
  <div className={styles.terms}>
    <Heading as="h2" size="sm">
      <FormattedMessage id="cloud.contextLayer.semanticSearch.terms.title" />
    </Heading>
    <Text size="sm">
      <FormattedMessage id="cloud.contextLayer.terms.subtitle" />
    </Text>
    <Text size="sm" bold>
      <FormattedMessage id="cloud.contextLayer.terms.intro" />
    </Text>
    <Text size="sm">
      <FormattedMessage id="cloud.contextLayer.terms.dataProcessing" />
    </Text>
    <Text size="sm">
      <FormattedMessage id="cloud.contextLayer.terms.privacyModeling" />
    </Text>
    <Text size="sm">
      <FormattedMessage id="cloud.contextLayer.terms.complianceResidency" />
    </Text>
    <Text size="sm">
      <FormattedMessage id="cloud.contextLayer.terms.liability" />
    </Text>
    <Text size="sm">
      <FormattedMessage id="cloud.contextLayer.terms.serviceLevel" />
    </Text>
    <Text size="sm">
      <FormattedMessage id="cloud.contextLayer.terms.intellectualProperty" />
    </Text>
    <ExternalLink href={links.termsLinkAgentEngine} opensInNewTab>
      <FormattedMessage id="cloud.contextLayer.semanticSearch.terms.link" />
    </ExternalLink>
    <Text size="xs" color="grey">
      <FormattedMessage id="cloud.contextLayer.terms.footnote" />
    </Text>
  </div>
);

const EnableDirectAccessError: React.FC<{
  actorName: string;
  reasonId: string;
  settingsPath: string;
}> = ({ actorName, reasonId, settingsPath }) => (
  <div className={styles.page}>
    <Heading as="h1">
      <FormattedMessage id="cloud.contextLayer.enableDirectAccess.error.title" values={{ name: actorName }} />
    </Heading>
    <Text>
      <FormattedMessage id={reasonId} />
    </Text>
    <Link to={settingsPath}>
      <FormattedMessage id="cloud.contextLayer.enableDirectAccess.settingsLink" />
    </Link>
  </div>
);

const EnableDirectAccessSuccess: React.FC<{
  actorName: string;
  settingsPath: string;
}> = ({ actorName, settingsPath }) => (
  <div className={styles.page}>
    <Heading as="h1">
      <FormattedMessage id="cloud.contextLayer.enableDirectAccess.success.title" values={{ name: actorName }} />
    </Heading>
    <Text>
      <FormattedMessage
        id="cloud.contextLayer.enableDirectAccess.success.description"
        values={{
          lnk: (chunks) => (
            <Link to={settingsPath} opensInNewTab rel="noopener noreferrer">
              {chunks}
            </Link>
          ),
        }}
      />
    </Text>
    <ExternalLink href={links.contextLayerDocs} opensInNewTab>
      <FormattedMessage id="cloud.contextLayer.docs" />
    </ExternalLink>
  </div>
);

const EnableDirectAccessContent: React.FC<EnableDirectAccessContentProps> = ({
  actorType,
  actorId,
  actorName,
  definitionName,
}) => {
  const organizationId = useCurrentOrganizationId();
  const workspaceId = useCurrentWorkspaceId();
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();
  const { formatMessage } = useIntl();
  const showActorContextLayerToggles = useShowActorContextLayerToggles();
  const { statusQuery, status, isEligible } = useContextLayerEligibility();
  const canManage = useGeneratedIntent(
    actorType === "source" ? Intent.CreateOrEditSource : Intent.CreateOrEditDestination
  );
  const canManageOrganizationPermissions = useGeneratedIntent(Intent.UpdateOrganizationPermissions, {
    organizationId,
  });
  const isEnrolled = status?.is_enrolled === true;
  const actor = { actorId, actorKind: actorType, workspaceId };
  const enablementQuery = useFusionActorEnablement(actor, showActorContextLayerToggles && isEnrolled);
  const enablementState = enablementQuery.data ? fusionEnablementState(enablementQuery.data) : undefined;
  const { mutateAsync: setFusionActorEnablement } = useSetFusionActorEnablement();
  const { mutateAsync: enrollOrganization } = useEnrollOrganizationInAgents();
  const [understood, setUnderstood] = useState(false);
  const [mutationState, setMutationState] = useState<"loading" | "success" | "error">();
  const autoEnableAttempted = useRef<string | null>(null);
  const skipConsentScreen = searchParams.get("skip-consent-screen") === "true";
  const settingsPath = `/${RoutePaths.Workspaces}/${workspaceId}/${actorType}/${actorId}`;
  const autoEnableKey = `${workspaceId}:${actorType}:${actorId}`;
  const settingsLink = (chunks: React.ReactNode) => (
    <Link to={settingsPath} opensInNewTab rel="noopener noreferrer">
      {chunks}
    </Link>
  );

  const enableDirectAccess = useCallback(
    async (enrollFirst: boolean) => {
      setMutationState("loading");
      try {
        if (enrollFirst) {
          await enrollOrganization({ workspaceIds: [workspaceId], addAllSupportedActors: false });
        }
        await setFusionActorEnablement({
          actorId,
          actorKind: actorType,
          workspaceId,
          enabled: true,
          ...(!enrollFirst && enablementState ? { expectedState: enablementState } : {}),
        });
        setMutationState("success");
      } catch {
        setMutationState("error");
      }
    },
    [actorId, actorType, enablementState, enrollOrganization, setFusionActorEnablement, workspaceId]
  );

  useEffect(() => {
    if (
      !skipConsentScreen ||
      !showActorContextLayerToggles ||
      !canManage ||
      statusQuery.isInitialLoading ||
      statusQuery.isError ||
      !isEligible ||
      !isEnrolled ||
      enablementQuery.isInitialLoading ||
      enablementQuery.isError ||
      enablementState?.enable_agent_access !== false ||
      mutationState !== undefined ||
      autoEnableAttempted.current === autoEnableKey
    ) {
      return;
    }

    autoEnableAttempted.current = autoEnableKey;
    void enableDirectAccess(false);
  }, [
    autoEnableKey,
    canManage,
    enableDirectAccess,
    enablementQuery.isError,
    enablementQuery.isInitialLoading,
    enablementState?.enable_agent_access,
    isEligible,
    isEnrolled,
    mutationState,
    showActorContextLayerToggles,
    skipConsentScreen,
    statusQuery.isError,
    statusQuery.isInitialLoading,
  ]);

  const permissionReasonId =
    actorType === "source"
      ? "cloud.contextLayer.sourceOptIn.noPermission"
      : "cloud.contextLayer.destinationOptIn.noPermission";

  if (!showActorContextLayerToggles) {
    return (
      <EnableDirectAccessError
        actorName={actorName}
        reasonId="cloud.contextLayer.unavailable.description"
        settingsPath={settingsPath}
      />
    );
  }

  if (mutationState === "success") {
    return <EnableDirectAccessSuccess actorName={actorName} settingsPath={settingsPath} />;
  }

  if (mutationState === "loading" || statusQuery.isInitialLoading) {
    return <LoadingPage />;
  }

  if (statusQuery.isError) {
    return (
      <EnableDirectAccessError
        actorName={actorName}
        reasonId="cloud.contextLayer.loadError.description"
        settingsPath={settingsPath}
      />
    );
  }

  if (!isEligible) {
    return (
      <EnableDirectAccessError
        actorName={actorName}
        reasonId="cloud.contextLayer.unavailable.description"
        settingsPath={settingsPath}
      />
    );
  }

  if (isEnrolled && enablementQuery.isInitialLoading) {
    return <LoadingPage />;
  }

  if (isEnrolled && enablementQuery.isError) {
    return (
      <EnableDirectAccessError
        actorName={actorName}
        reasonId="cloud.contextLayer.actor.loadFailed"
        settingsPath={settingsPath}
      />
    );
  }

  if (enablementState?.enable_agent_access) {
    return <EnableDirectAccessSuccess actorName={actorName} settingsPath={settingsPath} />;
  }

  if (!isEnrolled && !canManageOrganizationPermissions) {
    return (
      <EnableDirectAccessError
        actorName={actorName}
        reasonId="cloud.contextLayer.enable.adminRequired"
        settingsPath={settingsPath}
      />
    );
  }

  if (!canManage) {
    return <EnableDirectAccessError actorName={actorName} reasonId={permissionReasonId} settingsPath={settingsPath} />;
  }

  if (mutationState === "error") {
    return (
      <EnableDirectAccessError
        actorName={actorName}
        reasonId="cloud.contextLayer.enable.error"
        settingsPath={settingsPath}
      />
    );
  }

  if (skipConsentScreen && isEnrolled) {
    return <LoadingPage />;
  }

  const needsOrganizationEnrollment = !isEnrolled;

  return (
    <>
      <Heading as="h1">
        <FormattedMessage id="cloud.contextLayer.enableDirectAccess.title" values={{ name: actorName }} />
      </Heading>
      <div className={styles.content}>
        <Text>
          <FormattedMessage
            id="cloud.contextLayer.enableDirectAccess.description"
            values={{
              definitionName,
              name: actorName,
              b: (chunks) => <strong>{chunks}</strong>,
              lnk: settingsLink,
              settingsLnk: settingsLink,
            }}
          />
        </Text>
        {needsOrganizationEnrollment && (
          <>
            <OrganizationEnrollmentNotice />
            <ContextLayerTerms />
          </>
        )}
        <div className={styles.checkboxRow}>
          <CheckBox
            checkboxSize="sm"
            checked={understood}
            onChange={(event) => setUnderstood(event.target.checked)}
            aria-label={formatMessage({ id: "cloud.contextLayer.enableDirectAccess.understand" })}
          />
          <Text as="span" size="sm">
            <FormattedMessage id="cloud.contextLayer.enableDirectAccess.understand" />
          </Text>
        </div>
        <div className={styles.actions}>
          <Button type="button" variant="secondary" onClick={() => navigate(settingsPath)}>
            <FormattedMessage id="form.cancel" />
          </Button>
          <Button
            type="button"
            variant="primaryDark"
            disabled={!understood}
            onClick={() => void enableDirectAccess(needsOrganizationEnrollment)}
          >
            <FormattedMessage id="cloud.contextLayer.enableDirectAccess.confirm" />
          </Button>
        </div>
      </div>
    </>
  );
};

const SourceEnableDirectAccessPage: React.FC = () => {
  const source = useGetSourceFromParams();

  return (
    <>
      <HeadTitle titles={[{ id: "admin.sources" }, { title: source.name }]} />
      <EnableDirectAccessContent
        actorType="source"
        actorId={source.sourceId}
        actorName={source.name}
        definitionName={source.sourceName}
      />
    </>
  );
};

const DestinationEnableDirectAccessPage: React.FC = () => {
  const destination = useGetDestinationFromParams();

  return (
    <>
      <HeadTitle titles={[{ id: "admin.destinations" }, { title: destination.name }]} />
      <EnableDirectAccessContent
        actorType="destination"
        actorId={destination.destinationId}
        actorName={destination.name}
        definitionName={destination.destinationName}
      />
    </>
  );
};

export const EnableDirectAccessPage: React.FC<EnableDirectAccessPageProps> = ({ actorType }) => (
  <DefaultErrorBoundary>
    <React.Suspense fallback={<LoadingPage />}>
      <div className={styles.page}>
        {actorType === "source" ? <SourceEnableDirectAccessPage /> : <DestinationEnableDirectAccessPage />}
      </div>
    </React.Suspense>
  </DefaultErrorBoundary>
);

export default EnableDirectAccessPage;
