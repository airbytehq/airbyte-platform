import { render } from "@testing-library/react";
import { Suspense } from "react";
import { TestWrapper } from "test-utils";
import { mockExperiments } from "test-utils/mockExperiments";

import { useGetCloudWorkspace } from "core/api/cloud";
import {
  CloudWorkspaceReadCreditStatus as CreditStatus,
  CloudWorkspaceReadWorkspaceTrialStatus as WorkspaceTrialStatus,
} from "core/api/types/CloudApi";
import { I18nProvider } from "core/i18n";

import { WorkspaceStatusBanner } from "./WorkspaceStatusBanner";
import cloudLocales from "../../../locales/en.json";

jest.mock("services/workspaces/WorkspacesService", () => ({
  useCurrentWorkspace: () => ({
    workspace: { workspaceId: "123" },
  }),
}));

jest.mock("core/api/cloud");
const mockUseGetCloudWorkspace = useGetCloudWorkspace as unknown as jest.Mock<Partial<typeof useGetCloudWorkspace>>;

const workspaceBannerWithFlagTrue = (
  <TestWrapper>
    <Suspense fallback="this should not render">
      <I18nProvider messages={cloudLocales} locale="en">
        <WorkspaceStatusBanner />
      </I18nProvider>
    </Suspense>
  </TestWrapper>
);

const workspaceBannerWithFlagDefault = (
  <TestWrapper>
    <Suspense fallback="this should not render">
      <I18nProvider messages={cloudLocales} locale="en">
        <WorkspaceStatusBanner />
      </I18nProvider>
    </Suspense>
  </TestWrapper>
);

describe("WorkspaceCreditsBanner", () => {
  // create a date that is 1 day in the future
  const oneDayFromNow = Date.now() + 60_000 * 60 * 24;

  describe("With flag on", () => {
    beforeAll(() => {
      mockExperiments({ "billing.newTrialPolicy": true });
    });
    it("should render credits problem banner for credits problem pre-trial", () => {
      mockUseGetCloudWorkspace.mockImplementationOnce(() => {
        return {
          workspaceTrialStatus: WorkspaceTrialStatus.pre_trial,
          creditStatus: CreditStatus.negative_beyond_grace_period,
        };
      });

      const { getByText } = render(workspaceBannerWithFlagTrue);
      expect(getByText(/You’re out of credits!/)).toBeTruthy();
    });
    it("should render credits problem banner for credits problem during trial", () => {
      mockUseGetCloudWorkspace.mockImplementationOnce(() => {
        return {
          workspaceTrialStatus: WorkspaceTrialStatus.in_trial,
          creditStatus: CreditStatus.negative_beyond_grace_period,
        };
      });

      const { getByText } = render(workspaceBannerWithFlagTrue);
      expect(getByText(/You’re out of credits!/)).toBeTruthy();
    });

    it("should render credits problem banner for credits problem after trial", () => {
      mockUseGetCloudWorkspace.mockImplementationOnce(() => {
        return {
          workspaceTrialStatus: WorkspaceTrialStatus.out_of_trial,
          creditStatus: CreditStatus.negative_beyond_grace_period,
        };
      });

      const { getByText } = render(workspaceBannerWithFlagTrue);
      expect(getByText(/You’re out of credits!/)).toBeTruthy();
    });

    it("should render pre-trial banner if user's trial has not started", () => {
      mockUseGetCloudWorkspace.mockImplementationOnce(() => {
        return {
          creditStatus: CreditStatus.positive,
          workspaceTrialStatus: WorkspaceTrialStatus.pre_trial,
          trialExpiryTimestamp: null,
        };
      });

      const { getByText } = render(workspaceBannerWithFlagTrue);
      expect(getByText(/Your 14-day trial of Airbyte will start/)).toBeTruthy();
    });

    it("should render trial banner if user is in trial", () => {
      // create a date that is 1 day in the future
      const oneDayFromNow = Date.now() + 60_000 * 60 * 24;

      mockUseGetCloudWorkspace.mockImplementationOnce(() => {
        return {
          creditStatus: CreditStatus.positive,
          workspaceTrialStatus: WorkspaceTrialStatus.in_trial,
          trialExpiryTimestamp: oneDayFromNow,
        };
      });

      const { getByText } = render(workspaceBannerWithFlagTrue);
      expect(getByText(/You are using a trial of Airbyte/)).toBeTruthy();
      expect(getByText(/1 day/)).toBeTruthy();
    });
    it("should render an empty div if user is out of trial", () => {
      mockUseGetCloudWorkspace.mockImplementationOnce(() => {
        return {
          creditStatus: CreditStatus.positive,
          workspaceTrialStatus: WorkspaceTrialStatus.out_of_trial,
        };
      });

      const { queryByTestId } = render(workspaceBannerWithFlagTrue);
      expect(queryByTestId("workspace-status-banner")).toBeNull();
    });
  });

  describe("With flag off", () => {
    beforeAll(() => {
      mockExperiments({ "billing.newTrialPolicy": false });
    });
    it("should render credits problem banner for credits problem during trial", () => {
      mockUseGetCloudWorkspace.mockImplementationOnce(() => {
        return {
          creditStatus: CreditStatus.negative_beyond_grace_period,
          trialExpiryTimestamp: oneDayFromNow,
        };
      });

      const { getByText } = render(workspaceBannerWithFlagDefault);
      expect(getByText(/You’re out of credits!/)).toBeTruthy();
    });

    it("should render credits problem banner for credits problem after trial", () => {
      mockUseGetCloudWorkspace.mockImplementationOnce(() => {
        return {
          creditStatus: CreditStatus.negative_beyond_grace_period,
          trialExpiryTimestamp: null,
        };
      });

      const { getByText } = render(workspaceBannerWithFlagDefault);
      expect(getByText(/You’re out of credits!/)).toBeTruthy();
    });

    it("should render trial banner if user is in trial", () => {
      mockUseGetCloudWorkspace.mockImplementationOnce(() => {
        return {
          creditStatus: CreditStatus.positive,
          trialExpiryTimestamp: oneDayFromNow,
        };
      });

      const { getByText } = render(workspaceBannerWithFlagDefault);
      expect(getByText(/You are using a trial of Airbyte/)).toBeTruthy();
      expect(getByText(/1 day/)).toBeTruthy();
    });
    it("should render an empty div if user is out of trial", () => {
      mockUseGetCloudWorkspace.mockImplementationOnce(() => {
        return {
          trialExpiryTimestamp: null,
        };
      });

      const { queryByTestId } = render(workspaceBannerWithFlagDefault);
      expect(queryByTestId("workspace-status-banner")).toBeNull();
    });
  });
});
