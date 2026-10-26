import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { render } from "test-utils";
import { mockSourceDefinition } from "test-utils/mock-data/mockSource";

import { useGetBuilderProjectIdByDefinitionId } from "core/api";
import { BuilderProjectForDefinitionResponse } from "core/api/types/AirbyteClient";
import { useGeneratedIntent } from "core/utils/rbac";

import { ForkConnectorButton } from "./ForkConnectorButton";

jest.mock("area/workspace/utils", () => ({
  useCurrentWorkspaceLink: () => (path: string) => path,
  useWorkspaceLink: () => (path: string) => path,
}));
jest.mock("core/api", () => ({
  useGetBuilderProjectIdByDefinitionId: jest.fn(),
  useCreateSourceDefForkedBuilderProject: () => ({ mutateAsync: jest.fn(), isLoading: false }),
}));
jest.mock("core/utils/rbac", () => ({
  Intent: { CreateOrEditConnectorBuilder: "CreateOrEditConnectorBuilder" },
  useGeneratedIntent: jest.fn(),
}));

const manifestOnlySourceDefinition = { ...mockSourceDefinition, language: "manifest-only" };
const builderProject = { builderProjectId: "builder-project-id", workspaceId: "builder-project-workspace-id" };

const mockBuilderProject = (data: BuilderProjectForDefinitionResponse | undefined) =>
  jest
    .mocked(useGetBuilderProjectIdByDefinitionId)
    .mockReturnValue({ data } as ReturnType<typeof useGetBuilderProjectIdByDefinitionId>);

beforeEach(() => {
  mockBuilderProject(undefined);
  jest.mocked(useGeneratedIntent).mockReturnValue(true);
});

describe("ForkConnectorButton as an icon button", () => {
  it("names the button and shows the name in a tooltip on hover", async () => {
    await render(<ForkConnectorButton sourceDefinition={manifestOnlySourceDefinition} iconOnly />);

    const button = screen.getByRole("button", { name: "Fork in Connector Builder" });
    expect(button).toBeEnabled();
    expect(button).not.toHaveTextContent("Fork in Connector Builder");

    await userEvent.hover(button);
    expect(await screen.findByRole("tooltip")).toHaveTextContent("Fork in Connector Builder");
  });

  it("stays disabled and explains why for a connector the Connector Builder cannot fork", async () => {
    await render(<ForkConnectorButton sourceDefinition={mockSourceDefinition} iconOnly />);

    const button = screen.getByRole("button", { name: "Fork in Connector Builder" });
    expect(button).toBeDisabled();

    // A disabled button gets no pointer events, so hover the tooltip trigger that wraps it
    await userEvent.hover(button.parentElement as HTMLElement);
    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      `The "${mockSourceDefinition.name}" connector is incompatible with the Connector Builder, so it cannot be forked.`
    );
  });
});

describe("ForkConnectorButton for a connector with an existing Builder project", () => {
  it("links to the Builder project in a new tab when the user can edit it", async () => {
    mockBuilderProject(builderProject);

    await render(<ForkConnectorButton sourceDefinition={manifestOnlySourceDefinition} iconOnly />);

    const button = screen.getByRole("button", { name: "Edit in Builder" });
    expect(button).toBeEnabled();

    const link = button.closest("a");
    expect(link).toHaveAttribute("href", `/connector-builder/edit/${builderProject.builderProjectId}`);
    expect(link).toHaveAttribute("target", "_blank");

    await userEvent.hover(button);
    expect(await screen.findByRole("tooltip")).toHaveTextContent("Edit in Builder");
  });

  it("stays disabled and names the Builder project workspace when the user cannot edit it", async () => {
    mockBuilderProject(builderProject);
    jest.mocked(useGeneratedIntent).mockReturnValue(false);

    await render(<ForkConnectorButton sourceDefinition={manifestOnlySourceDefinition} iconOnly />);

    const button = screen.getByRole("button", { name: "Edit in Builder" });
    expect(button).toBeDisabled();
    expect(useGeneratedIntent).toHaveBeenCalledWith("CreateOrEditConnectorBuilder", {
      workspaceId: builderProject.workspaceId,
    });

    await userEvent.hover(button.parentElement as HTMLElement);
    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      `This custom connector belongs to workspace "${builderProject.workspaceId}", which you do not have access to`
    );
  });
});

describe("ForkConnectorButton when disabled", () => {
  it("disables the fork button for a connector the Connector Builder can fork and keeps its tooltip", async () => {
    await render(<ForkConnectorButton sourceDefinition={manifestOnlySourceDefinition} iconOnly disabled />);

    const button = screen.getByRole("button", { name: "Fork in Connector Builder" });
    expect(button).toBeDisabled();

    await userEvent.hover(button.parentElement as HTMLElement);
    expect(await screen.findByRole("tooltip")).toHaveTextContent("Fork in Connector Builder");
  });

  it("disables the edit button without a link to the Builder project and keeps its tooltip", async () => {
    mockBuilderProject(builderProject);

    await render(<ForkConnectorButton sourceDefinition={manifestOnlySourceDefinition} iconOnly disabled />);

    const button = screen.getByRole("button", { name: "Edit in Builder" });
    expect(button).toBeDisabled();
    expect(button.closest("a")).toBeNull();

    await userEvent.hover(button.parentElement as HTMLElement);
    expect(await screen.findByRole("tooltip")).toHaveTextContent("Edit in Builder");
  });

  it("disables the fork button when the Builder project has no workspace", async () => {
    mockBuilderProject({ builderProjectId: builderProject.builderProjectId, workspaceId: null });

    await render(<ForkConnectorButton sourceDefinition={manifestOnlySourceDefinition} iconOnly disabled />);

    expect(screen.getByRole("button", { name: "Fork in Connector Builder" })).toBeDisabled();
    expect(screen.queryByRole("button", { name: "Edit in Builder" })).not.toBeInTheDocument();
  });
});

describe("ForkConnectorButton as a text button", () => {
  it("keeps the Fork in Builder text", async () => {
    await render(<ForkConnectorButton sourceDefinition={manifestOnlySourceDefinition} />);

    expect(screen.getByRole("button", { name: "Fork in Builder" })).toHaveTextContent("Fork in Builder");
  });
});
