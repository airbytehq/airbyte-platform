import { JobConfigType, JobRead, JobStatus } from "core/api/types/AirbyteClient";

export const mockJob: JobRead = {
  id: 1,
  status: JobStatus.succeeded,
  createdAt: 0,
  updatedAt: 0,
  configType: JobConfigType.sync,
  configId: "5f9c9b4f-5669-4440-a870-eabb09ae166b",
};
