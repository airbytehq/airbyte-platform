import { fireEvent, render } from "@testing-library/react";

import { TestWrapper } from "test-utils/testutils";

import { FeatureItem } from "core/services/features";
import * as FeatureService from "core/services/features/FeatureService";
import * as ModalService from "hooks/services/Modal/ModalService";

import { InviteUsersHint } from "./InviteUsersHint";

const createUseFeatureMock = (options: { visible?: boolean }) => (key: FeatureItem) => {
  switch (key) {
    case FeatureItem.ShowInviteUsersHint:
      return options.visible ?? false;
    default:
      throw new Error(`${key} is not mocked`);
  }
};

jest.mock("core/api", () => ({
  useCurrentWorkspace: () => ({ workspaceId: "workspaceId" }),
}));

jest.mock("core/utils/rbac", () => ({
  useIntent: jest.fn().mockReturnValue(true),
  useGeneratedIntent: jest.fn().mockReturnValue(true),
  Intent: {
    UpdateWorkspacePermissions: "UpdateWorkspacePermissions",
    CreateOrEditConnection: "CreateOrEditConnection",
  },
}));

describe("InviteUsersHint", () => {
  it("does not render by default", () => {
    const { queryByTestId } = render(<InviteUsersHint connectorType="source" />, { wrapper: TestWrapper });
    expect(queryByTestId("inviteUsersHint")).not.toBeInTheDocument();
  });

  it("renders when `SHOW_INVITE_USERS_HINT` is set to `true`", () => {
    jest.spyOn(FeatureService, "useFeature").mockImplementation(createUseFeatureMock({ visible: true }));

    const { getByTestId } = render(<InviteUsersHint connectorType="source" />, { wrapper: TestWrapper });
    const element = getByTestId("inviteUsersHint");
    expect(element).toBeInTheDocument();
  });

  it("opens modal when clicking on CTA by default", () => {
    const mockOpenModal = jest.fn();
    jest.spyOn(ModalService, "useModalService").mockImplementationOnce(() => ({
      openModal: mockOpenModal,
      getCurrentModalTitle: jest.fn(),
    }));
    jest.spyOn(FeatureService, "useFeature").mockImplementation(createUseFeatureMock({ visible: true }));

    const { getByTestId } = render(<InviteUsersHint connectorType="source" />, { wrapper: TestWrapper });
    const element = getByTestId("inviteUsersHint-cta");

    expect(element).not.toHaveAttribute("href");

    fireEvent.click(element);
    expect(mockOpenModal).toHaveBeenCalledTimes(1);
  });
});
