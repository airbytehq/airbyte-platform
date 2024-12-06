import { generateDiagnosticReport } from "../generated/AirbyteClient";
import { useRequestOptions } from "../useRequestOptions";

export const useDiagnosticReport = () => {
  const requestOptions = useRequestOptions();
  return async (organizationId: string) => {
    const result = await generateDiagnosticReport({ organizationId }, requestOptions);
    const resultFile = new File([result], "diagnostic_report.zip", { type: "application/zip" });
    const url = URL.createObjectURL(resultFile);
    const a = document.createElement("a");
    a.href = url;
    a.download = "diagnostic_report.zip";
    a.click();
  };
};
