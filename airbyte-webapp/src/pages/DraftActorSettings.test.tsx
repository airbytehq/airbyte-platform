import { act } from "@testing-library/react";

import { mockDestination, mockSource, render } from "test-utils";

import { useGetDestinationFromParams, useGetSourceFromParams } from "area/connector/utils";
import { useInvalidateDestination, useInvalidateSource, useUpdateDestination, useUpdateSource } from "core/api";

import { DestinationSettingsPage } from "./destination/DestinationSettingsPage/DestinationSettingsPage";
import { SourceSettingsPage } from "./source/SourceSettingsPage/SourceSettingsPage";

const mockConnectorCard = jest.fn((_props: unknown) => null);

jest.mock("area/connector/components/ConnectorCard", () => ({
  ConnectorCard: (props: unknown) => mockConnectorCard(props),
}));

jest.mock("area/connector/utils", () => ({
  useGetSourceFromParams: jest.fn(),
  useGetDestinationFromParams: jest.fn(),
}));

jest.mock("cloud/components/AgentsOptIn", () => ({ ActorContextLayerCard: () => null }));

jest.mock("core/api", () => ({
  useSourceDefinition: () => ({ sourceDefinitionId: "source-definition" }),
  useSourceDefinitionVersion: () => ({ supportLevel: "certified" }),
  useGetSourceDefinitionSpecification: () => ({ sourceDefinitionId: "source-definition" }),
  useDestinationDefinition: () => ({ destinationDefinitionId: "destination-definition" }),
  useDestinationDefinitionVersion: () => ({ supportLevel: "certified" }),
  useGetDestinationDefinitionSpecification: () => ({ destinationDefinitionId: "destination-definition" }),
  useUpdateSource: jest.fn(),
  useUpdateDestination: jest.fn(),
  useInvalidateSource: jest.fn(),
  useInvalidateDestination: jest.fn(),
  useDeleteSource: () => ({ mutateAsync: jest.fn() }),
  useDeleteDestination: () => ({ mutateAsync: jest.fn() }),
}));

jest.mock("core/services/analytics", () => ({
  useTrackPage: () => undefined,
  PageTrackingCodes: { SOURCE_ITEM_SETTINGS: "source", DESTINATION_ITEM_SETTINGS: "destination" },
}));

jest.mock("core/services/FormChangeTracker", () => ({
  useFormChangeTrackerService: () => ({ clearFormChange: jest.fn() }),
  useUniqueFormId: () => "form-id",
}));

jest.mock("core/utils/datadog", () => ({ trackTiming: jest.fn() }));
jest.mock("core/utils/useDeleteModal", () => ({ useDeleteModal: () => jest.fn() }));
jest.mock("react-use", () => ({ useEffectOnce: () => undefined }));

const sourceValues = {
  name: "Draft source",
  serviceType: "source-definition",
  connectionConfiguration: { token: "updated" },
};

const destinationValues = {
  name: "Draft destination",
  serviceType: "destination-definition",
  connectionConfiguration: { token: "updated" },
};

describe("draft actor settings", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("resumes a source draft with draft-aware save and setup callbacks", async () => {
    const draft = { ...mockSource, isDraft: true };
    const updateSource = jest.fn().mockResolvedValue(draft);
    const reloadSource = jest.fn();
    jest.mocked(useGetSourceFromParams).mockReturnValue(draft);
    jest
      .mocked(useUpdateSource)
      .mockReturnValue({ mutateAsync: updateSource } as unknown as ReturnType<typeof useUpdateSource>);
    jest.mocked(useInvalidateSource).mockReturnValue(reloadSource);

    await render(<SourceSettingsPage />);
    const props = mockConnectorCard.mock.calls[0][0] as {
      onSaveDraft?: (values: typeof sourceValues) => Promise<unknown>;
      onDraftPromoted?: () => Promise<void>;
    };

    expect(props.onSaveDraft).toEqual(expect.any(Function));
    await act(async () => props.onSaveDraft?.(sourceValues));
    expect(updateSource).toHaveBeenCalledWith({ values: sourceValues, sourceId: draft.sourceId });
    await act(async () => props.onDraftPromoted?.());
    expect(reloadSource).toHaveBeenCalled();
  });

  it("resumes a destination draft with draft-aware save and setup callbacks", async () => {
    const draft = { ...mockDestination, isDraft: true };
    const updateDestination = jest.fn().mockResolvedValue(draft);
    const reloadDestination = jest.fn();
    jest.mocked(useGetDestinationFromParams).mockReturnValue(draft);
    jest
      .mocked(useUpdateDestination)
      .mockReturnValue({ mutateAsync: updateDestination } as unknown as ReturnType<typeof useUpdateDestination>);
    jest.mocked(useInvalidateDestination).mockReturnValue(reloadDestination);

    await render(<DestinationSettingsPage />);
    const props = mockConnectorCard.mock.calls[0][0] as {
      onSaveDraft?: (values: typeof destinationValues) => Promise<unknown>;
      onDraftPromoted?: () => Promise<void>;
    };

    expect(props.onSaveDraft).toEqual(expect.any(Function));
    await act(async () => props.onSaveDraft?.(destinationValues));
    expect(updateDestination).toHaveBeenCalledWith({ values: destinationValues, destinationId: draft.destinationId });
    await act(async () => props.onDraftPromoted?.());
    expect(reloadDestination).toHaveBeenCalled();
  });
});
