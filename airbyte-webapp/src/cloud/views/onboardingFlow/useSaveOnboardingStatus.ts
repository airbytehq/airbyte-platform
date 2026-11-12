import { useState } from "react";
import { useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { useCurrentOrganizationId } from "area/organization/utils";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useFetchOrganizationInfo, useUpdateOrganizationOnboarding } from "core/api";
import { OrganizationOnboardingStatus } from "core/api/types/AirbyteClient";
import { useNotificationService } from "core/services/Notification";
import { RoutePaths } from "pages/routePaths";

import { isInProgress } from "./useShouldEnterOnboardingFlow";

/**
 * Saves the status of the screen to open, then opens it. A failed save shows an error and keeps the current screen.
 * Onboarding can be skipped or completed in another tab or device while this flow is open, so a step status is saved
 * only while the stored status is still a step status. Otherwise the normal app opens.
 */
export const useSaveOnboardingStatus = () => {
  const organizationId = useCurrentOrganizationId();
  const workspaceId = useCurrentWorkspaceId();
  const navigate = useNavigate();
  const fetchOrganizationInfo = useFetchOrganizationInfo();
  const { mutate: updateOnboarding, isLoading } = useUpdateOrganizationOnboarding();
  const [isCheckingStatus, setIsCheckingStatus] = useState(false);
  const { registerNotification } = useNotificationService();
  const { formatMessage } = useIntl();

  const notifySaveFailure = () =>
    registerNotification({
      id: "onboardingFlow/status-failure",
      text: formatMessage({ id: "onboardingFlow.statusError" }),
      type: "error",
    });

  const openNormalApp = () => navigate(`/${RoutePaths.Workspaces}/${workspaceId}`, { replace: true });

  const save = (status: OrganizationOnboardingStatus, onSaved: () => void) =>
    updateOnboarding({ organizationId, status }, { onSuccess: onSaved, onError: notifySaveFailure });

  const saveStepStatus = async (status: OrganizationOnboardingStatus, onSaved: () => void) => {
    setIsCheckingStatus(true);
    try {
      const { onboardingStatus } = await fetchOrganizationInfo(organizationId);
      if (isInProgress(onboardingStatus)) {
        save(status, onSaved);
      } else {
        openNormalApp();
      }
    } catch {
      notifySaveFailure();
    } finally {
      setIsCheckingStatus(false);
    }
  };

  const saveStatus = (status: OrganizationOnboardingStatus, onSaved: () => void) =>
    isInProgress(status) ? saveStepStatus(status, onSaved) : save(status, onSaved);

  return { saveStatus, isSaving: isCheckingStatus || isLoading };
};
