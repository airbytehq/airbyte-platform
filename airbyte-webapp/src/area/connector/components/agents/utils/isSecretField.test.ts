import { isSecretField } from "./isSecretField";

describe("isSecretField", () => {
  describe("simple fields", () => {
    it("returns true for simple secret field", () => {
      const schema = {
        type: "object",
        properties: {
          connectionConfiguration: {
            type: "object",
            properties: {
              password: { type: "string", airbyte_secret: true },
            },
          },
        },
      };
      expect(isSecretField("connectionConfiguration.password", schema)).toBe(true);
    });

    it("returns false for non-secret field", () => {
      const schema = {
        type: "object",
        properties: {
          connectionConfiguration: {
            type: "object",
            properties: {
              host: { type: "string" },
            },
          },
        },
      };
      expect(isSecretField("connectionConfiguration.host", schema)).toBe(false);
    });

    it("returns false for field without airbyte_secret property", () => {
      const schema = {
        type: "object",
        properties: {
          connectionConfiguration: {
            type: "object",
            properties: {
              username: { type: "string" },
            },
          },
        },
      };
      expect(isSecretField("connectionConfiguration.username", schema)).toBe(false);
    });

    it("returns false when airbyte_secret is false", () => {
      const schema = {
        type: "object",
        properties: {
          connectionConfiguration: {
            type: "object",
            properties: {
              apiKey: { type: "string", airbyte_secret: false },
            },
          },
        },
      };
      expect(isSecretField("connectionConfiguration.apiKey", schema)).toBe(false);
    });
  });

  describe("nested fields", () => {
    it("returns true for nested secret field", () => {
      const schema = {
        type: "object",
        properties: {
          connectionConfiguration: {
            type: "object",
            properties: {
              tunnel_method: {
                type: "object",
                properties: {
                  tunnel_user_password: { type: "string", airbyte_secret: true },
                },
              },
            },
          },
        },
      };
      expect(isSecretField("connectionConfiguration.tunnel_method.tunnel_user_password", schema)).toBe(true);
    });

    it("returns false for nested non-secret field", () => {
      const schema = {
        type: "object",
        properties: {
          connectionConfiguration: {
            type: "object",
            properties: {
              tunnel_method: {
                type: "object",
                properties: {
                  tunnel_host: { type: "string" },
                },
              },
            },
          },
        },
      };
      expect(isSecretField("connectionConfiguration.tunnel_method.tunnel_host", schema)).toBe(false);
    });

    it("returns true for deeply nested secret field", () => {
      const schema = {
        type: "object",
        properties: {
          connectionConfiguration: {
            type: "object",
            properties: {
              level1: {
                type: "object",
                properties: {
                  level2: {
                    type: "object",
                    properties: {
                      level3: {
                        type: "object",
                        properties: {
                          secret: { type: "string", airbyte_secret: true },
                        },
                      },
                    },
                  },
                },
              },
            },
          },
        },
      };
      expect(isSecretField("connectionConfiguration.level1.level2.level3.secret", schema)).toBe(true);
    });
  });

  describe("oneOf schemas", () => {
    it("handles oneOf schemas with discriminator - basic auth", () => {
      const schema = {
        type: "object",
        properties: {
          connectionConfiguration: {
            type: "object",
            properties: {
              auth: {
                oneOf: [
                  {
                    properties: {
                      authType: { const: "basic" },
                      password: { type: "string", airbyte_secret: true },
                    },
                  },
                  {
                    properties: {
                      authType: { const: "oauth2" },
                      clientSecret: { type: "string", airbyte_secret: true },
                    },
                  },
                ],
              },
            },
          },
        },
      };

      expect(isSecretField("connectionConfiguration.auth.password", schema)).toBe(true);
    });

    it("handles oneOf schemas with discriminator - oauth2", () => {
      const schema = {
        type: "object",
        properties: {
          connectionConfiguration: {
            type: "object",
            properties: {
              auth: {
                oneOf: [
                  {
                    properties: {
                      authType: { const: "basic" },
                      password: { type: "string", airbyte_secret: true },
                    },
                  },
                  {
                    properties: {
                      authType: { const: "oauth2" },
                      clientSecret: { type: "string", airbyte_secret: true },
                    },
                  },
                ],
              },
            },
          },
        },
      };

      expect(isSecretField("connectionConfiguration.auth.clientSecret", schema)).toBe(true);
    });

    it("returns true for field that is secret in any oneOf branch", () => {
      const schema = {
        type: "object",
        properties: {
          connectionConfiguration: {
            type: "object",
            properties: {
              auth: {
                oneOf: [
                  {
                    properties: {
                      authType: { const: "basic" },
                      password: { type: "string", airbyte_secret: true },
                    },
                  },
                  {
                    properties: {
                      authType: { const: "oauth2" },
                      clientSecret: { type: "string", airbyte_secret: true },
                    },
                  },
                ],
              },
            },
          },
        },
      };

      // clientSecret is a secret in the oauth2 branch, so we conservatively treat it as a secret
      // This is safer for preserving user-edited values
      expect(isSecretField("connectionConfiguration.auth.clientSecret", schema)).toBe(true);
    });

    it("handles oneOf at root level", () => {
      const schema = {
        oneOf: [
          {
            properties: {
              connectionConfiguration: {
                type: "object",
                properties: {
                  type: { const: "postgres" },
                  password: { type: "string", airbyte_secret: true },
                },
              },
            },
          },
          {
            properties: {
              connectionConfiguration: {
                type: "object",
                properties: {
                  type: { const: "mysql" },
                  apiKey: { type: "string", airbyte_secret: true },
                },
              },
            },
          },
        ],
      };

      expect(isSecretField("connectionConfiguration.password", schema)).toBe(true);
    });

    it("handles nested oneOf inside object", () => {
      const schema = {
        type: "object",
        properties: {
          connectionConfiguration: {
            type: "object",
            properties: {
              ssl_mode: {
                type: "object",
                oneOf: [
                  {
                    properties: {
                      mode: { const: "verify-ca" },
                      ca_certificate: { type: "string", airbyte_secret: true },
                    },
                  },
                  {
                    properties: {
                      mode: { const: "disable" },
                    },
                  },
                ],
              },
            },
          },
        },
      };

      expect(isSecretField("connectionConfiguration.ssl_mode.ca_certificate", schema)).toBe(true);
    });
  });

  describe("edge cases", () => {
    it("returns false for missing field path", () => {
      const schema = {
        type: "object",
        properties: {
          connectionConfiguration: {
            type: "object",
            properties: {
              host: { type: "string" },
            },
          },
        },
      };
      expect(isSecretField("connectionConfiguration.nonexistent", schema)).toBe(false);
    });

    it("returns false when schema is null", () => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      expect(isSecretField("connectionConfiguration.password", null as any)).toBe(false);
    });

    it("returns false when schema is undefined", () => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      expect(isSecretField("connectionConfiguration.password", undefined as any)).toBe(false);
    });

    it("returns false for empty field path", () => {
      const schema = {
        type: "object",
        properties: {
          connectionConfiguration: {
            type: "object",
            properties: {
              password: { type: "string", airbyte_secret: true },
            },
          },
        },
      };
      expect(isSecretField("", schema)).toBe(false);
    });

    it("returns false when schema has no properties", () => {
      const schema = {
        type: "object",
      };
      expect(isSecretField("connectionConfiguration.password", schema)).toBe(false);
    });

    it("returns false when intermediate path doesn't exist in schema", () => {
      const schema = {
        type: "object",
        properties: {
          connectionConfiguration: {
            type: "object",
            properties: {
              host: { type: "string" },
            },
          },
        },
      };
      expect(isSecretField("connectionConfiguration.tunnel.password", schema)).toBe(false);
    });

    it("handles config value being null", () => {
      const schema = {
        type: "object",
        properties: {
          connectionConfiguration: {
            type: "object",
            properties: {
              tunnel: {
                type: "object",
                properties: {
                  password: { type: "string", airbyte_secret: true },
                },
              },
            },
          },
        },
      };
      // Should still return true based on schema, even if config value is null
      expect(isSecretField("connectionConfiguration.tunnel.password", schema)).toBe(true);
    });

    it("handles config value being undefined", () => {
      const schema = {
        type: "object",
        properties: {
          connectionConfiguration: {
            type: "object",
            properties: {
              password: { type: "string", airbyte_secret: true },
            },
          },
        },
      };
      // Should still return true based on schema, even if config value is undefined
      expect(isSecretField("connectionConfiguration.password", schema)).toBe(true);
    });
  });

  describe("real-world scenarios", () => {
    it("handles Postgres SSL configuration", () => {
      const schema = {
        type: "object",
        properties: {
          connectionConfiguration: {
            type: "object",
            properties: {
              host: { type: "string" },
              port: { type: "integer" },
              password: { type: "string", airbyte_secret: true },
              ssl_mode: {
                type: "object",
                oneOf: [
                  {
                    properties: {
                      mode: { const: "verify-ca" },
                      ca_certificate: { type: "string", airbyte_secret: true },
                      client_certificate: { type: "string", airbyte_secret: true },
                      client_key: { type: "string", airbyte_secret: true },
                      client_key_password: { type: "string", airbyte_secret: true },
                    },
                  },
                  {
                    properties: {
                      mode: { const: "disable" },
                    },
                  },
                ],
              },
            },
          },
        },
      };

      // Non-secret fields
      expect(isSecretField("connectionConfiguration.host", schema)).toBe(false);
      expect(isSecretField("connectionConfiguration.port", schema)).toBe(false);

      // Secret fields
      expect(isSecretField("connectionConfiguration.password", schema)).toBe(true);
      expect(isSecretField("connectionConfiguration.ssl_mode.ca_certificate", schema)).toBe(true);
      expect(isSecretField("connectionConfiguration.ssl_mode.client_certificate", schema)).toBe(true);
      expect(isSecretField("connectionConfiguration.ssl_mode.client_key", schema)).toBe(true);
      expect(isSecretField("connectionConfiguration.ssl_mode.client_key_password", schema)).toBe(true);
    });

    it("handles SSH tunnel configuration", () => {
      const schema = {
        type: "object",
        properties: {
          connectionConfiguration: {
            type: "object",
            properties: {
              host: { type: "string" },
              tunnel_method: {
                type: "object",
                oneOf: [
                  {
                    properties: {
                      tunnel_method: { const: "NO_TUNNEL" },
                    },
                  },
                  {
                    properties: {
                      tunnel_method: { const: "SSH_PASSWORD_AUTH" },
                      tunnel_host: { type: "string" },
                      tunnel_user_password: { type: "string", airbyte_secret: true },
                    },
                  },
                  {
                    properties: {
                      tunnel_method: { const: "SSH_KEY_AUTH" },
                      tunnel_host: { type: "string" },
                      ssh_key: { type: "string", airbyte_secret: true },
                    },
                  },
                ],
              },
            },
          },
        },
      };

      expect(isSecretField("connectionConfiguration.host", schema)).toBe(false);
      expect(isSecretField("connectionConfiguration.tunnel_method.tunnel_host", schema)).toBe(false);
      expect(isSecretField("connectionConfiguration.tunnel_method.tunnel_user_password", schema)).toBe(true);

      expect(isSecretField("connectionConfiguration.tunnel_method.ssh_key", schema)).toBe(true);
      // tunnel_user_password is a secret in the password auth branch, so we conservatively treat it as secret
      expect(isSecretField("connectionConfiguration.tunnel_method.tunnel_user_password", schema)).toBe(true);
    });
  });
});
