import { screen, render } from "@testing-library/react";

import { TestWrapper } from "test-utils/testutils";

import { useFormOauthAdapter } from "area/connector/components/ConnectorForm/components/Sections/auth/useOauthFlowAdapter";
import { useConnectorForm } from "area/connector/components/ConnectorForm/connectorFormContext";
import { useAuthentication } from "area/connector/components/ConnectorForm/useAuthentication";
import { ConnectorIds } from "area/connector/utils";
import { ConnectorDefinition, ConnectorDefinitionSpecificationRead } from "core/domain/connector";

import { AuthButton } from "./AuthButton";
jest.setTimeout(10000);

/**
 * Mock services to reuse multiple times with different values:
 * 1. mock the service
 * 2. cast the type as shown below.  `as unknown` is required to get the typing to play nice as there isn't crossover
 *    Partial is optional, but required if you do not want to mock the entire object
 *
 * Then, can implement in tests using useWhateverServiceYouMocked.mockImplementationOnce or useWhateverServiceYouMocked.mockImplementation
 */

jest.mock("area/connector/components/ConnectorForm/components/Sections/auth/useOauthFlowAdapter");
const mockUseFormOauthAdapter = useFormOauthAdapter as unknown as jest.Mock<Partial<typeof useFormOauthAdapter>>;
const baseUseFormOauthAdapterValues = {
  run: jest.fn(),
  loading: false,
};

jest.mock("area/connector/components/ConnectorForm/connectorFormContext");
const mockUseConnectorForm = useConnectorForm as unknown as jest.Mock<Partial<typeof useConnectorForm>>;
const baseUseConnectorFormValues: Partial<ReturnType<typeof useConnectorForm>> = {
  selectedConnectorDefinition: { sourceDefinitionId: "abcde", name: "Acme" } as ConnectorDefinition,
  selectedConnectorDefinitionSpecification: {} as ConnectorDefinitionSpecificationRead,
};

jest.mock("area/connector/components/ConnectorForm/useAuthentication");
const mockUseAuthentication = useAuthentication as unknown as jest.Mock<Partial<typeof useAuthentication>>;

describe("auth button", () => {
  beforeEach(() => {
    jest.clearAllMocks();

    mockUseAuthentication.mockReturnValue({ hiddenAuthFieldErrors: {} });
  });

  it("initially renders with correct message and no status message", () => {
    // no auth errors
    mockUseConnectorForm.mockImplementationOnce(() => {
      const { selectedConnectorDefinition } = baseUseConnectorFormValues;

      return { selectedConnectorDefinition };
    });

    // not done
    mockUseFormOauthAdapter.mockImplementationOnce(() => {
      const done = false;
      const { run, loading } = baseUseFormOauthAdapterValues;

      return { done, run, loading };
    });

    render(
      <TestWrapper>
        <AuthButton
          selectedConnectorDefinitionSpecification={
            baseUseConnectorFormValues.selectedConnectorDefinitionSpecification as ConnectorDefinitionSpecificationRead
          }
        />
      </TestWrapper>
    );

    // correct heading and button text
    expect(screen.getByText("Authenticate your Acme account")).toBeInTheDocument();
    const button = screen.getByRole("button", { name: "Authenticate" });
    expect(button).toHaveAccessibleDescription("Authenticate your Acme account");

    // no error message
    const errorMessage = screen.queryByText(/Authentication required/i);
    expect(errorMessage).not.toBeInTheDocument();
  });

  it("after authenticating in this session, it announces success, shows a secondary re-authenticate button and hides the manual setup link", () => {
    mockUseAuthentication.mockReturnValue({ hiddenAuthFieldErrors: {}, canUseManualOAuthMode: true });

    mockUseConnectorForm.mockImplementationOnce(() => {
      const { selectedConnectorDefinition } = baseUseConnectorFormValues;

      return { selectedConnectorDefinition };
    });

    // done
    mockUseFormOauthAdapter.mockImplementationOnce(() => {
      const done = true;
      const { run, loading } = baseUseFormOauthAdapterValues;

      return { done, run, loading, hasRun: done };
    });

    render(
      <TestWrapper>
        <AuthButton
          selectedConnectorDefinitionSpecification={
            baseUseConnectorFormValues.selectedConnectorDefinitionSpecification as ConnectorDefinitionSpecificationRead
          }
        />
      </TestWrapper>
    );

    // success message and secondary button
    expect(screen.getByRole("status")).toHaveTextContent("Authentication successful");
    const button = screen.getByRole("button", { name: "Re-authenticate" });
    expect(button).toHaveClass("button--secondary");
    expect(button).toHaveAccessibleDescription("Authentication successful");
    expect(screen.queryByRole("button", { name: "Set up manually" })).not.toBeInTheDocument();
  });

  it("with saved credentials and no authentication in this session, it keeps the heading and offers a secondary re-authenticate button", () => {
    mockUseAuthentication.mockReturnValue({ hiddenAuthFieldErrors: {}, canUseManualOAuthMode: true });

    mockUseConnectorForm.mockImplementationOnce(() => {
      const { selectedConnectorDefinition } = baseUseConnectorFormValues;

      return { selectedConnectorDefinition };
    });

    // saved credentials make done true, but no flow ran in this session
    mockUseFormOauthAdapter.mockImplementationOnce(() => {
      const { run, loading } = baseUseFormOauthAdapterValues;

      return { done: true, hasRun: false, run, loading };
    });

    render(
      <TestWrapper>
        <AuthButton
          selectedConnectorDefinitionSpecification={
            baseUseConnectorFormValues.selectedConnectorDefinitionSpecification as ConnectorDefinitionSpecificationRead
          }
        />
      </TestWrapper>
    );

    expect(screen.getByText("Authenticate your Acme account")).toBeInTheDocument();
    expect(screen.queryByText("Authentication successful")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Re-authenticate" })).toHaveClass("button--secondary");
    expect(screen.getByRole("button", { name: "Set up manually" })).toBeInTheDocument();
  });

  it("keeps the branded Google button after successful authentication", () => {
    mockUseConnectorForm.mockImplementationOnce(() => {
      const { selectedConnectorDefinition } = baseUseConnectorFormValues;

      return { selectedConnectorDefinition };
    });

    // done
    mockUseFormOauthAdapter.mockImplementationOnce(() => {
      const done = true;
      const { run, loading } = baseUseFormOauthAdapterValues;

      return { done, run, loading, hasRun: done };
    });

    render(
      <TestWrapper>
        <AuthButton
          selectedConnectorDefinitionSpecification={
            { sourceDefinitionId: ConnectorIds.Sources.GoogleAds } as ConnectorDefinitionSpecificationRead
          }
        />
      </TestWrapper>
    );

    expect(screen.getByText("Authentication successful")).toBeInTheDocument();
    const button = screen.getByRole("button", { name: "Re-authenticate" });
    expect(button).not.toHaveClass("button--secondary");
    expect(button.querySelector("img")).toBeInTheDocument();
  });

  it("shows the branded Google sign-in button before authentication", () => {
    mockUseConnectorForm.mockImplementationOnce(() => {
      const { selectedConnectorDefinition } = baseUseConnectorFormValues;

      return { selectedConnectorDefinition };
    });

    mockUseFormOauthAdapter.mockImplementationOnce(() => {
      const { run, loading } = baseUseFormOauthAdapterValues;

      return { done: false, hasRun: false, run, loading };
    });

    render(
      <TestWrapper>
        <AuthButton
          selectedConnectorDefinitionSpecification={
            { sourceDefinitionId: ConnectorIds.Sources.GoogleAds } as ConnectorDefinitionSpecificationRead
          }
        />
      </TestWrapper>
    );

    expect(screen.getByText("Authenticate your Acme account")).toBeInTheDocument();
    const button = screen.getByRole("button", { name: "Sign in with Google" });
    expect(button).not.toHaveClass("button--primary");
    expect(button.querySelector("img")).toBeInTheDocument();
  });

  it("shows the branded QuickBooks button", () => {
    mockUseConnectorForm.mockImplementationOnce(() => {
      const { selectedConnectorDefinition } = baseUseConnectorFormValues;

      return { selectedConnectorDefinition };
    });

    mockUseFormOauthAdapter.mockImplementationOnce(() => {
      const { run, loading } = baseUseFormOauthAdapterValues;

      return { done: false, hasRun: false, run, loading };
    });

    render(
      <TestWrapper>
        <AuthButton
          selectedConnectorDefinitionSpecification={
            { sourceDefinitionId: ConnectorIds.Sources.QuickBooks } as ConnectorDefinitionSpecificationRead
          }
        />
      </TestWrapper>
    );

    expect(screen.getByRole("button", { name: "Connect to QuickBook" })).toHaveClass("qbAuthButton");
    expect(screen.getByText("Authenticate")).toBeInTheDocument();
  });

  it("renders an error if there are any auth fields with empty values", () => {
    // auth errors
    mockUseAuthentication.mockReturnValue({ hiddenAuthFieldErrors: { field: "required" } });

    mockUseConnectorForm.mockImplementationOnce(() => {
      const { selectedConnectorDefinition } = baseUseConnectorFormValues;

      return { selectedConnectorDefinition };
    });

    // not done
    mockUseFormOauthAdapter.mockImplementationOnce(() => {
      const done = false;
      const { run, loading } = baseUseFormOauthAdapterValues;

      return { done, run, loading };
    });

    render(
      <TestWrapper>
        <AuthButton
          selectedConnectorDefinitionSpecification={
            baseUseConnectorFormValues.selectedConnectorDefinitionSpecification as ConnectorDefinitionSpecificationRead
          }
        />
      </TestWrapper>
    );

    // correct button
    const button = screen.getByRole("button", { name: "Authenticate" });
    expect(button).toBeInTheDocument();

    // error message
    const errorMessage = screen.getByText(/Authentication required/i);
    expect(errorMessage).toBeInTheDocument();
  });
});
