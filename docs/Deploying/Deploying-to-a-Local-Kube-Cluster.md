# Local Development

## Building

### Build Cloud
```
./gradlew build 
```
This will build:
* all the OSS code that Cloud uses
* all Cloud code
* create all the images needed to run Cloud locally 

Note: It does _not_ include the tests for oss code.
Note: It does _not_ build images needed to run OSS.

## Build OSS
This lets you build OSS images and runs test / check in OSS.
```
./gradlew -p oss build 
```

## Running Cloud Locally
### Deploying Cloud to a Local Kube Cluster (currently broken)

Following steps are for devs attempting to run local changes. This assumes the presence of a local Kubernetes cluster, which can be enabled in Docker Desktop > Preferences > Kubernetes > Enable Kubernetes.

1. Ensure your kubectl context is set correctly.
```
kubectl config get-contexts
kubectl config use-context docker-desktop
```
2. Build all images.
```
./gradlew build
```
3. Install the firebase CLI, which is needed to run the local authentication emulator (for more info, see Firebase docs [here](https://firebase.google.com/docs/emulator-suite/connect_and_prototype#locally_initialize_a_firebase_project))
```
curl -sL firebase.tools | bash
```
4. Run the local deploy script
```
./tools/bin/deploy_cloud_locally.sh
```

This will deploy the locally-build airbyte-cloud on your local Kubernetes cluster, using a local Firebase auth emulator for login authentication.
Create a new account in the airbyte cloud UI with any email and password, and the authentication link will be printed in your terminal where you ran the local deploy script.
Clicking this link will verify the new account in the authenticator, and once you refresh the app you should be logged in!

The local deploy script will continue to stay open and print out any other authentication links. To shut down the local deployment, just press Ctrl-C. This will remove the local deployment as well as shut down the firebase emulator and kubernetes port-forwards that were set up.

### Troubleshooting

#### Rendered Manifests Error

If you run into an error like `Error: rendered manifests contain a resource that already exists.`, this may be because the local deployment is trying to create a resource with helm that already exists in your local kube cluster. To resolve this, you can reset your kubernetes cluster by opening the Docker Desktop app and going to Settings -> Kubernetes -> Reset Kubernetes Cluster. Once the reset is complete, try re-running the local deploy script.

#### Pod Error Status

If you run into an issue with one or more pods failing to start/in an `Error` status state, this may be due to the local Kubernetes
cluster not having enough resources.  To increase the resources provided by Docker Desktop, use the following steps:

1. Open the Docker Desktop application
2. Click on the gear widget on the right-hand side of the toolbar
3. Select "Resources" from the "Preferences" window.
4. Adjust the resources.  Suggested values are:
   * CPUs:  >= 6
   * Memory: >= 12 GB
   * Swap: >= 1 GB
   * Disk Image Size: >= 96 GB

## Running OSS Locally

### Running with local images from source.
Note: `./gradlew build` from the root does not build the OSS containers. Before trying any of the run strategies below you must run:

```
./gradlew -p oss buildDockerImage
```
Now run airbyte:
```
# from the root of the repo
VERSION=dev docker-compose -f oss/docker-compose.yaml --env-file oss/.env up
```

### Run with the latest released version of OSS.
The extra build step is not needed since we are pulling the images from the docker repository.
```
# from the root of the repo
docker-compose -f oss/docker-compose.yaml --env-file oss/.env up
```

### Formatting (both Cloud and OSS)
```
./gradlew format
```
