import { render } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { mockConnection, TestWrapper } from "test-utils/testutils";

import { SchemaChange } from "core/api/types/AirbyteClient";
import { FeatureItem } from "core/services/features";
const mockUseConnectionEditService = jest.fn();

jest.doMock("hooks/services/ConnectionEdit/ConnectionEditService", () => ({
  useConnectionEditService: mockUseConnectionEditService,
}));

const TestWrapperWithAutoDetectSchema: React.FC<React.PropsWithChildren<Record<string, unknown>>> = ({ children }) => (
  <TestWrapper features={[FeatureItem.AllowAutoDetectSchema]}>{children}</TestWrapper>
);

const buttonSpy = jest.fn();

const renderComponent = () =>
  render(
    <SchemaChangeBackdrop>
      <button data-testid="bg-button" onClick={() => buttonSpy}>
        don't click
      </button>
    </SchemaChangeBackdrop>,
    { wrapper: TestWrapperWithAutoDetectSchema }
  );

// eslint-disable-next-line @typescript-eslint/no-var-requires
const { SchemaChangeBackdrop } = require("./SchemaChangeBackdrop");

describe("SchemaChangesBackdrop", () => {
  it("renders with breaking changes and prevents background interaction", async () => {
    mockUseConnectionEditService.mockReturnValue({
      connection: { mockConnection, schemaChange: SchemaChange.breaking },
      schemaHasBeenRefreshed: false,
      schemaRefreshing: false,
    });

    const { getByTestId } = renderComponent();

    expect(getByTestId("schemaChangesBackdrop")).toMatchSnapshot();
    await userEvent.click(getByTestId("bg-button"));
    expect(buttonSpy).not.toHaveBeenCalled();
  });

  it("does not render if there are non-breaking changes", async () => {
    mockUseConnectionEditService.mockReturnValue({
      connection: { mockConnection, schemaChange: SchemaChange.non_breaking },
      schemaHasBeenRefreshed: false,
      schemaRefreshing: false,
    });

    const { queryByTestId, getByTestId } = renderComponent();

    expect(queryByTestId("schemaChangesBackdrop")).toBeFalsy();

    await userEvent.click(getByTestId("bg-button"));
    expect(buttonSpy).not.toHaveBeenCalled();
  });

  it("does not render if there are no changes", async () => {
    mockUseConnectionEditService.mockReturnValue({
      connection: { mockConnection, schemaChange: SchemaChange.no_change },
      schemaHasBeenRefreshed: false,
      schemaRefreshing: false,
    });

    const { queryByTestId, getByTestId } = renderComponent();

    expect(queryByTestId("schemaChangesBackdrop")).toBeFalsy();

    await userEvent.click(getByTestId("bg-button"));
    expect(buttonSpy).not.toHaveBeenCalled();
  });

  it("does not render if schema has been refreshed", async () => {
    mockUseConnectionEditService.mockReturnValue({
      connection: mockConnection,
      schemaHasBeenRefreshed: true,
      schemaRefreshing: false,
    });

    const { queryByTestId, getByTestId } = renderComponent();
    expect(queryByTestId("schemaChangesBackdrop")).toBeFalsy();

    await userEvent.click(getByTestId("bg-button"));
    expect(buttonSpy).not.toHaveBeenCalled();
  });
});
