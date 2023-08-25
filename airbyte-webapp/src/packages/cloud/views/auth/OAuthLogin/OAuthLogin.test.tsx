import { render } from "@testing-library/react";
import userEvents from "@testing-library/user-event";
import { EMPTY } from "rxjs";

import { TestWrapper } from "test-utils/testutils";

import { OAuthLogin } from "./OAuthLogin";

const mockLoginWithOAuth = jest.fn();

describe("OAuthLogin", () => {
  beforeEach(() => {
    mockLoginWithOAuth.mockReturnValue(EMPTY);
  });

  it("should call auth service for Google", async () => {
    const { getByTestId } = render(<OAuthLogin loginWithOAuth={mockLoginWithOAuth} />, { wrapper: TestWrapper });
    await userEvents.click(getByTestId("googleOauthLogin"));
    expect(mockLoginWithOAuth).toHaveBeenCalledWith("google");
  });

  it("should call auth service for GitHub", async () => {
    const { getByTestId } = render(<OAuthLogin loginWithOAuth={mockLoginWithOAuth} />, { wrapper: TestWrapper });
    await userEvents.click(getByTestId("githubOauthLogin"));
    expect(mockLoginWithOAuth).toHaveBeenCalledWith("github");
  });
});
