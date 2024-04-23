import { render } from "@testing-library/react";
import userEvents from "@testing-library/user-event";
import { EMPTY } from "rxjs";

import { TestWrapper } from "test-utils/testutils";

import { OAuthLogin } from "./OAuthLogin";

const mockLoginWithOAuth = jest.fn();

const mockRedirectToSignInWithGithub = jest.fn().mockReturnValue(Promise.resolve());
const mockRedirectToSignInWithGoogle = jest.fn().mockReturnValue(Promise.resolve());

jest.mock("packages/cloud/services/auth/KeycloakService", () => ({
  useKeycloakService: () => ({
    redirectToSignInWithGithub: mockRedirectToSignInWithGithub,
    redirectToSignInWithGoogle: mockRedirectToSignInWithGoogle,
  }),
}));

describe("OAuthLogin", () => {
  beforeEach(() => {
    mockLoginWithOAuth.mockReturnValue(EMPTY);
  });

  it("should call auth service for Google", async () => {
    const { getByTestId } = render(<OAuthLogin loginWithOAuth={mockLoginWithOAuth} type="login" />, {
      wrapper: TestWrapper,
    });
    await userEvents.click(getByTestId("googleOauthLogin"));
    expect(mockRedirectToSignInWithGoogle).toHaveBeenCalled();
  });

  it("should call auth service for GitHub", async () => {
    const { getByTestId } = render(<OAuthLogin loginWithOAuth={mockLoginWithOAuth} type="login" />, {
      wrapper: TestWrapper,
    });
    await userEvents.click(getByTestId("githubOauthLogin"));
    expect(mockRedirectToSignInWithGithub).toHaveBeenCalled();
  });
});
