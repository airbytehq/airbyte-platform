import { renderHook } from "@testing-library/react";

import { FormConnectionFormValues, useInitialFormValues } from "components/connection/ConnectionForm/formConfig";
import { mocked } from "test-utils";

import { AirbyteStreamAndConfiguration } from "core/api/types/AirbyteClient";

import { useStreamsConfigTableRowProps } from "./useStreamsConfigTableRowProps";

const mockStream: Partial<AirbyteStreamAndConfiguration> = {
  stream: {
    name: "stream_name",
    namespace: "stream_namespace",
  },
  config: { selected: true, syncMode: "full_refresh", destinationSyncMode: "overwrite" },
};

const mockInitialValues: FormConnectionFormValues = {
  name: "connection_name",
  scheduleType: "manual",
  syncCatalog: {
    streams: [
      {
        stream: {
          name: "stream_name",
          namespace: "stream_namespace",
        },
        config: { selected: true, syncMode: "full_refresh", destinationSyncMode: "overwrite" },
      },
    ],
  },
  namespaceDefinition: "source",
  prefix: "",
};

const mockDisabledInitialValues: FormConnectionFormValues = {
  ...mockInitialValues,
  syncCatalog: {
    streams: [
      {
        ...mockInitialValues.syncCatalog?.streams[0],
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        config: { ...mockInitialValues.syncCatalog.streams[0].config!, selected: false },
      },
    ],
  },
};

jest.mock("components/connection/ConnectionForm/formConfig", () => ({
  useInitialFormValues: jest.fn().mockReturnValue({}),
}));

jest.mock("hooks/services/ConnectionForm/ConnectionFormService", () => ({
  useConnectionFormService: () => ({ mode: "edit" }),
}));

describe(`${useStreamsConfigTableRowProps.name}`, () => {
  it("should return default styles for a row that starts enabled", () => {
    mocked(useInitialFormValues).mockReturnValueOnce(mockInitialValues);

    const { result } = renderHook(() => useStreamsConfigTableRowProps(mockStream));

    expect(result.current.streamHeaderContentStyle).toEqual("streamHeaderContent");
    expect(result.current.pillButtonVariant).toEqual("grey");
  });
  it("should return disabled styles for a row that starts disabled", () => {
    mocked(useInitialFormValues).mockReturnValueOnce(mockDisabledInitialValues);

    const { result } = renderHook(() =>
      useStreamsConfigTableRowProps({
        ...mockStream,
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        config: { ...mockStream.config!, selected: false },
      })
    );

    expect(result.current.streamHeaderContentStyle).toEqual("streamHeaderContent disabled");
    expect(result.current.pillButtonVariant).toEqual("grey");
  });
  it("should return added styles for a row that is added", () => {
    mocked(useInitialFormValues).mockReturnValueOnce(mockDisabledInitialValues);

    const { result } = renderHook(() => useStreamsConfigTableRowProps(mockStream));

    expect(result.current.streamHeaderContentStyle).toEqual("streamHeaderContent added");
    expect(result.current.pillButtonVariant).toEqual("green");
  });
  it("should return removed styles for a row that is removed", () => {
    mocked(useInitialFormValues).mockReturnValueOnce(mockInitialValues);

    const { result } = renderHook(() =>
      useStreamsConfigTableRowProps({
        ...mockStream,
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        config: { ...mockStream.config!, selected: false },
      })
    );

    expect(result.current.streamHeaderContentStyle).toEqual("streamHeaderContent removed");
    expect(result.current.pillButtonVariant).toEqual("red");
  });
  it("should return updated styles for a row that is updated", () => {
    mocked(useInitialFormValues).mockReturnValueOnce(mockInitialValues);

    const { result } = renderHook(() =>
      useStreamsConfigTableRowProps({
        ...mockStream,
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        config: { ...mockStream.config!, syncMode: "incremental", destinationSyncMode: "append_dedup" },
      })
    );

    expect(result.current.streamHeaderContentStyle).toEqual("streamHeaderContent changed");
    expect(result.current.pillButtonVariant).toEqual("blue");
  });

  it("should return added styles for a row that is both added and updated", () => {
    mocked(useInitialFormValues).mockReturnValueOnce(mockDisabledInitialValues);

    const { result } = renderHook(() =>
      useStreamsConfigTableRowProps({
        ...mockStream,
        config: { selected: true, syncMode: "incremental", destinationSyncMode: "append_dedup" }, // selected true, new sync, mode and destination sync mode
      })
    );

    expect(result.current.streamHeaderContentStyle).toEqual("streamHeaderContent added");
    expect(result.current.pillButtonVariant).toEqual("green");
  });
});
