// This file contains infrastructure utilities for the API

// Utility to get API base URL
export const getApiBaseUrl = () => {
  const serverHost = process.env.AIRBYTE_SERVER_HOST;
  return serverHost ? `${serverHost}/api/v1` : "https://localhost:3000/api/v1";
};
