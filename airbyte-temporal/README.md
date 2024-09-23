# airbyte-temporal

This module implements a custom version of what the Temporal autosetup image is doing. Because Temporal does not recommend the autosetup be used in production, we had to add some modifications. It ensures that the temporalDB schema will get upgraded if the temporal version is updated.
