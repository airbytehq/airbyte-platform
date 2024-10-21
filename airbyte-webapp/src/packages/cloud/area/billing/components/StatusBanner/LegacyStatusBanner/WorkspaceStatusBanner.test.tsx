import { render } from "@testing-library/react";
import { Suspense } from "react";

import { TestWrapper } from "test-utils";

import {
  CloudWorkspaceRead,
  CloudWorkspaceReadCreditStatus as CreditStatus,
  CloudWorkspaceReadWorkspaceTrialStatus as WorkspaceTrialStatus,
} from "core/api/types/CloudApi";
import { I18nProvider } from "core/services/i18n";

import { WorkspaceStatusBanner } from "./WorkspaceStatusBanner";

const defaultCloudWorkspace = { workspaceId: "123" };

const renderWorkspaceBanner = (cloudWorkspace: CloudWorkspaceRead) => {
  return render(
    <TestWrapper>
      <Suspense fallback="this should not render">
        <I18nProvider locale="en">
          <WorkspaceStatusBanner cloudWorkspace={cloudWorkspace} />
        </I18nProvider>
      </Suspense>
    </TestWrapper>
  );
};

describe("WorkspaceCreditsBanner", () => {
  it("should render credits problem banner for credits problem pre-trial", () => {
    const cloudWorkspace = {
      ...defaultCloudWorkspace,
      workspaceTrialStatus: WorkspaceTrialStatus.pre_trial,
      creditStatus: CreditStatus.negative_beyond_grace_period,
    };

    const { getByText } = renderWorkspaceBanner(cloudWorkspace);
    expect(getByText(/You’re out of credits!/)).toBeTruthy();
  });
  it("should render credits problem banner for credits problem during trial", () => {
    const cloudWorkspace = {
      ...defaultCloudWorkspace,
      workspaceTrialStatus: WorkspaceTrialStatus.in_trial,
      creditStatus: CreditStatus.negative_beyond_grace_period,
    };

    const { getByText } = renderWorkspaceBanner(cloudWorkspace);
    expect(getByText(/You’re out of credits!/)).toBeTruthy();
  });

  it("should render credits problem banner for credits problem after trial", () => {
    const cloudWorkspace = {
      ...defaultCloudWorkspace,
      workspaceTrialStatus: WorkspaceTrialStatus.out_of_trial,
      creditStatus: CreditStatus.negative_beyond_grace_period,
    };

    const { getByText } = renderWorkspaceBanner(cloudWorkspace);
    expect(getByText(/You’re out of credits!/)).toBeTruthy();
  });

  it("should render pre-trial banner if user's trial has not started", () => {
    const cloudWorkspace = {
      ...defaultCloudWorkspace,
      creditStatus: CreditStatus.positive,
      workspaceTrialStatus: WorkspaceTrialStatus.pre_trial,
      trialExpiryTimestamp: undefined,
    };

    const { getByText } = renderWorkspaceBanner(cloudWorkspace);

    expect(getByText(/Your 14-day trial of Airbyte will start/)).toBeTruthy();
  });

  it("should render trial banner if user is in trial", () => {
    // create a date that is 1 day in the future
    const oneDayFromNow = Date.now() + 60_000 * 60 * 24;
    const cloudWorkspace = {
      ...defaultCloudWorkspace,
      creditStatus: CreditStatus.positive,
      workspaceTrialStatus: WorkspaceTrialStatus.in_trial,
      trialExpiryTimestamp: oneDayFromNow,
    };

    const { getByText } = renderWorkspaceBanner(cloudWorkspace);
    expect(getByText(/You are using a trial of Airbyte/)).toBeTruthy();
    expect(getByText(/1 day/)).toBeTruthy();
  });
  it("should render an empty div if user is out of trial", () => {
    const cloudWorkspace = {
      ...defaultCloudWorkspace,
      creditStatus: CreditStatus.positive,
      workspaceTrialStatus: WorkspaceTrialStatus.out_of_trial,
    };

    const { queryByTestId } = renderWorkspaceBanner(cloudWorkspace);
    expect(queryByTestId("workspace-status-banner")).toBeNull();
  });
});
