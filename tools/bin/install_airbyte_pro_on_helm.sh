#!/usr/bin/env bash

set -e

# Get the directory of the current script
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Now use this to get the relative path of the airbyte.yml file and airbyte-pro-values.yml file
airbyte_yml_file_path="$script_dir/../../configs/airbyte.yml"
airbyte_pro_values_yml_file_path="$script_dir/../../charts/airbyte/airbyte-pro-values.yaml"

# Define the helm release name for this installation of Airbyte Pro.
if [ ! -z "$RELEASE_NAME" ]; then
  airbyte_pro_release_name="$RELEASE_NAME"
else
  # Default release name. Change this to your liking.
  airbyte_pro_release_name="airbyte-pro"
fi

# run this script with DEV=true to install using images tagged with 'dev' instead of an official Airbyte release tag.
# This is helpful for installing a locally-built instance of Airbyte.
if [ "$DEV" == "true" ]; then
  set_dev_image_tag_if_true="--set global.image.tag=dev"
else
  set_dev_image_tag_if_true=""
fi

if [ "$KEYCLOAK_RESET_REALM" == "true" ]; then
  set_reset_realm_if_true="--set keycloak-setup.env_vars.KEYCLOAK_RESET_REALM=true"
else
  set_reset_realm_if_true=""
fi

# run this script with LOCAL=true to install using the local version of the chart.
# Otherwise, it will install using the latest version of the chart from the airbyte/airbyte repo
if [ "$LOCAL" == "true" ]; then
  pushd $script_dir/../../charts/airbyte

  # make sure we have the latest version of the local chart
  helm dep update

  # additional arguments to this script are appended at the end of the command, so that further customization can be done
  helm upgrade --install "$airbyte_pro_release_name" . $set_dev_image_tag_if_true $set_reset_realm_if_true --values "$airbyte_pro_values_yml_file_path" --values "$airbyte_yml_file_path" "$@"
  popd
else
  airbyte_chart="airbyte/airbyte"

  # additional arguments to this script are appended at the end of the command, so that further customization can be done
  helm upgrade --install "$airbyte_pro_release_name" "$airbyte_chart" $set_dev_image_tag_if_true $set_reset_realm_if_true --values "$airbyte_pro_values_yml_file_path" "$@"
fi

echo "Airbyte Pro installation complete!"

echo "Restarting webapp deployment to pick up new configuration..."
kubectl rollout restart "deployment/$airbyte_pro_release_name-webapp"
echo "Webapp deployment restarted! Once all pods are running, your Airbyte Pro instance will be ready to use."
