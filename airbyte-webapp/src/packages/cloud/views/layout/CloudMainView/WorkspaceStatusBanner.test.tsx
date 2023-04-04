import { render } from "@testing-library/react";
import { Suspense } from "react";
import { TestWrapper } from "test-utils";
import { mockExperiments } from "test-utils/mockExperiments";

import { I18nProvider } from "core/i18n";
import { CreditStatus, WorkspaceTrialStatus } from "packages/cloud/lib/domain/cloudWorkspaces/types";
import { useGetCloudWorkspace } from "packages/cloud/services/workspaces/CloudWorkspacesService";

import { WorkspaceStatusBanner } from "./WorkspaceStatusBanner";
import cloudLocales from "../../../locales/en.json";

jest.mock("services/workspaces/WorkspacesService", () => ({
  useCurrentWorkspace: () => ({
    workspace: { workspaceId: "123" },
  }),
}));

jest.mock("packages/cloud/services/workspaces/CloudWorkspacesService");
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
          workspaceTrialStatus: WorkspaceTrialStatus.PRE_TRIAL,
          creditStatus: CreditStatus.NEGATIVE_BEYOND_GRACE_PERIOD,
        };
      });

      const { getByText } = render(workspaceBannerWithFlagTrue);
      expect(getByText(/You’re out of credits!/)).toBeTruthy();
    });
    it("should render credits problem banner for credits problem during trial", () => {
      mockUseGetCloudWorkspace.mockImplementationOnce(() => {
        return {
          workspaceTrialStatus: WorkspaceTrialStatus.IN_TRIAL,
          creditStatus: CreditStatus.NEGATIVE_BEYOND_GRACE_PERIOD,
        };
      });

      const { getByText } = render(workspaceBannerWithFlagTrue);
      expect(getByText(/You’re out of credits!/)).toBeTruthy();
    });

    it("should render credits problem banner for credits problem after trial", () => {
      mockUseGetCloudWorkspace.mockImplementationOnce(() => {
        return {
          workspaceTrialStatus: WorkspaceTrialStatus.OUT_OF_TRIAL,
          creditStatus: CreditStatus.NEGATIVE_BEYOND_GRACE_PERIOD,
        };
      });

      const { getByText } = render(workspaceBannerWithFlagTrue);
      expect(getByText(/You’re out of credits!/)).toBeTruthy();
    });

    it("should render pre-trial banner if user's trial has not started", () => {
      mockUseGetCloudWorkspace.mockImplementationOnce(() => {
        return {
          creditStatus: CreditStatus.POSITIVE,
          workspaceTrialStatus: WorkspaceTrialStatus.PRE_TRIAL,
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
          creditStatus: CreditStatus.POSITIVE,
          workspaceTrialStatus: WorkspaceTrialStatus.IN_TRIAL,
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
          creditStatus: CreditStatus.POSITIVE,
          workspaceTrialStatus: WorkspaceTrialStatus.OUT_OF_TRIAL,
        };
      });

      const { container } = render(workspaceBannerWithFlagTrue);
      expect(container).toBeEmptyDOMElement();
    });
  });

  describe("With flag off", () => {
    beforeAll(() => {
      mockExperiments({ "billing.newTrialPolicy": false });
    });
    it("should render credits problem banner for credits problem during trial", () => {
      mockUseGetCloudWorkspace.mockImplementationOnce(() => {
        return {
          creditStatus: CreditStatus.NEGATIVE_BEYOND_GRACE_PERIOD,
          trialExpiryTimestamp: oneDayFromNow,
        };
      });

      const { getByText } = render(workspaceBannerWithFlagDefault);
      expect(getByText(/You’re out of credits!/)).toBeTruthy();
    });

    it("should render credits problem banner for credits problem after trial", () => {
      mockUseGetCloudWorkspace.mockImplementationOnce(() => {
        return {
          creditStatus: CreditStatus.NEGATIVE_BEYOND_GRACE_PERIOD,
          trialExpiryTimestamp: null,
        };
      });

      const { getByText } = render(workspaceBannerWithFlagDefault);
      expect(getByText(/You’re out of credits!/)).toBeTruthy();
    });

    it("should render trial banner if user is in trial", () => {
      mockUseGetCloudWorkspace.mockImplementationOnce(() => {
        return {
          creditStatus: CreditStatus.POSITIVE,
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

      const { container } = render(workspaceBannerWithFlagDefault);
      expect(container).toBeEmptyDOMElement();
    });
  });
});
