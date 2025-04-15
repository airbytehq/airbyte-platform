import { maskSecrets } from "./maskSecrets";

describe("maskSecrets", () => {
  it("should return the config as is if schema doesn't have properties", () => {
    const config = { key: "value" };
    const schema = {};
    expect(maskSecrets(config, schema)).toEqual(config);
  });

  it("should mask values marked as airbyte_secret", () => {
    const config = {
      username: "user123",
      password: "secret123",
      apiKey: "key123",
    };
    const schema = {
      properties: {
        username: { type: "string" },
        password: { type: "string", airbyte_secret: true },
        apiKey: { type: "string", airbyte_secret: true },
      },
    };
    expect(maskSecrets(config, schema)).toEqual({
      username: "user123",
      password: "******",
      apiKey: "******",
    });
  });

  it("should handle nested objects", () => {
    const config = {
      credentials: {
        username: "user123",
        password: "secret123",
      },
      settings: {
        timeout: 30,
        apiKey: "key123",
      },
    };
    const schema = {
      properties: {
        credentials: {
          type: "object",
          properties: {
            username: { type: "string" },
            password: { type: "string", airbyte_secret: true },
          },
        },
        settings: {
          type: "object",
          properties: {
            timeout: { type: "integer" },
            apiKey: { type: "string", airbyte_secret: true },
          },
        },
      },
    };
    expect(maskSecrets(config, schema)).toEqual({
      credentials: {
        username: "user123",
        password: "******",
      },
      settings: {
        timeout: 30,
        apiKey: "******",
      },
    });
  });

  it("should handle oneOf schemas", () => {
    const config = {
      authType: "oauth2",
      clientId: "client123",
      clientSecret: "secret123",
    };
    const schema = {
      oneOf: [
        {
          properties: {
            authType: { const: "basic" },
            username: { type: "string" },
            password: { type: "string", airbyte_secret: true },
          },
        },
        {
          properties: {
            authType: { const: "oauth2" },
            clientId: { type: "string" },
            clientSecret: { type: "string", airbyte_secret: true },
          },
        },
      ],
    };
    expect(maskSecrets(config, schema)).toEqual({
      authType: "oauth2",
      clientId: "client123",
      clientSecret: "******",
    });
  });

  it("should handle oneOf in property schema", () => {
    const config = {
      auth: {
        type: "oauth2",
        oauth2: {
          clientId: "client123",
          clientSecret: "secret123",
        },
      },
    };
    const schema = {
      properties: {
        auth: {
          type: "object",
          oneOf: [
            {
              properties: {
                type: { const: "basic" },
                basic: {
                  type: "object",
                  properties: {
                    username: { type: "string" },
                    password: { type: "string", airbyte_secret: true },
                  },
                },
              },
            },
            {
              properties: {
                type: { const: "oauth2" },
                oauth2: {
                  type: "object",
                  properties: {
                    clientId: { type: "string" },
                    clientSecret: { type: "string", airbyte_secret: true },
                  },
                },
              },
            },
          ],
        },
      },
    };
    expect(maskSecrets(config, schema)).toEqual({
      auth: {
        type: "oauth2",
        oauth2: {
          clientId: "client123",
          clientSecret: "******",
        },
      },
    });
  });

  it("should not mask empty, null, or undefined values", () => {
    const config = {
      password1: "",
      password2: null,
      password3: undefined,
      password4: "secret",
    };
    const schema = {
      properties: {
        password1: { type: "string", airbyte_secret: true },
        password2: { type: "string", airbyte_secret: true },
        password3: { type: "string", airbyte_secret: true },
        password4: { type: "string", airbyte_secret: true },
      },
    };
    expect(maskSecrets(config, schema)).toEqual({
      password1: "",
      password2: null,
      password3: undefined,
      password4: "******",
    });
  });
});
