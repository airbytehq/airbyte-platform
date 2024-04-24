<p align="center">
  <a href="https://airbyte.com"><img src="https://assets.website-files.com/605e01bc25f7e19a82e74788/624d9c4a375a55100be6b257_Airbyte_logo_color_dark.svg" alt="Airbyte"></a>
</p>
<p align="center">
    <em>Data integration platform for ELT pipelines from APIs, databases & files to databases, warehouses & lakes</em>
</p>
<p align="center">
<a href="https://github.com/airbytehq/airbyte/stargazers/" target="_blank">
    <img src="https://img.shields.io/github/stars/airbytehq/airbyte?style=social&label=Star&maxAge=2592000" alt="Test">
</a>
<a href="https://github.com/airbytehq/airbyte/releases" target="_blank">
    <img src="https://img.shields.io/github/v/release/airbytehq/airbyte?color=white" alt="Release">
</a>
<a href="https://airbytehq.slack.com/" target="_blank">
    <img src="https://img.shields.io/badge/slack-join-white.svg?logo=slack" alt="Slack">
</a>
<a href="https://www.youtube.com/c/AirbyteHQ/?sub_confirmation=1" target="_blank">
    <img alt="YouTube Channel Views" src="https://img.shields.io/youtube/channel/views/UCQ_JWEFzs1_INqdhIO3kmrw?style=social">
</a>
<a href="https://github.com/airbytehq/airbyte/actions/workflows/gradle.yml" target="_blank">
    <img src="https://img.shields.io/github/actions/workflow/status/airbytehq/airbyte/gradle.yml?branch=master" alt="Build">
</a>
<a href="https://github.com/airbytehq/airbyte/tree/master/docs/project-overview/licenses" target="_blank">
    <img src="https://img.shields.io/static/v1?label=license&message=MIT&color=white" alt="License">
</a>
<a href="https://github.com/airbytehq/airbyte/tree/master/docs/project-overview/licenses" target="_blank">
    <img src="https://img.shields.io/static/v1?label=license&message=ELv2&color=white" alt="License">
</a>
</p>

We believe that only an **open-source** solution to data movement can cover the **long tail of data sources** while empowering data engineers to **customize existing connectors**. Our ultimate vision is to help you move data from any source to any destination. Airbyte already provides [300+ connectors](https://docs.airbyte.com/integrations/) for popular APIs, databases, data warehouses and data lakes.

Airbyte connectors can be implemented in any language and take the form of a Docker image that follows the [Airbyte specification](https://docs.airbyte.com/understanding-airbyte/airbyte-protocol/). You can create new connectors very fast with:
- The [low-code Connector Development Kit](https://docs.airbyte.com/connector-development/config-based/low-code-cdk-overview) (CDK) for API connectors ([demo](https://www.youtube.com/watch?v=i7VSL2bDvmw))
- The [Python CDK](https://docs.airbyte.com/connector-development/cdk-python/) ([tutorial](https://docs.airbyte.com/connector-development/tutorials/cdk-speedrun))

Airbyte has a built-in scheduler and uses [Temporal](https://airbyte.com/blog/scale-workflow-orchestration-with-temporal) to orchestrate jobs and ensure reliability at scale. Airbyte leverages [dbt](https://www.youtube.com/watch?v=saXwh6SpeHA) to normalize extracted data and can trigger custom transformations in SQL and dbt. You can also orchestrate Airbyte syncs with [Airflow](https://docs.airbyte.com/operator-guides/using-the-airflow-airbyte-operator), [Prefect](https://docs.airbyte.com/operator-guides/using-prefect-task), [Dagster](https://docs.airbyte.com/operator-guides/using-dagster-integration), or [Kestra](https://docs.airbyte.com/operator-guides/using-kestra-plugin/).

![Airbyte OSS Connections UI](https://user-images.githubusercontent.com/2302748/205949986-5207ca24-f1f0-41b1-97e1-a0745a0de55a.png)

Explore our [demo app](https://demo.airbyte.io/).

## Quick start

⚠️ This Quickstart method is still under active development. This tool is intended to get Airbyte running as quickly as possible with no additional configuration necessary.
Additional configuration options may be added in the future, however, if you need additional configuration options now, use the
docker compose solution by following the instructions for the `run_ab_platform.sh` script [here](/deploying-airbyte/docker-compose).
⚠️

### Run Airbyte locally

You can run Airbyte locally with `abctl`. Mac users can install `abctl` with Brew:

```bash
brew tap airbytehq/tap
brew install abctl
```


## Setup & launch Airbyte

- Install `Docker Desktop`  \(see [instructions](https://docs.docker.com/desktop/install/mac-install/)\).
- After `Docker Desktop` is installed, you must enable `Kubernetes` \(see [instructions](https://docs.docker.com/desktop/kubernetes/)\).
- For users that cannot install `abctl` with `brew` you download the latest version of `abctl` from the [releases page](https://github.com/airbytehq/abctl/releases)
- Run the following command:

```bash
./abctl local install
```

- Your browser should open to the Airbyte Application, if it does not visit [http://localhost](http://localhost)
- You will be asked for a username and password. By default, that's username `airbyte` and password `password`. You can set these values through command line flags or environment variables. For example, to set the username and password to `foo` and `bar` respectively, you can run the following command:

```bash
./abctl local install --username foo --password bar

# Or as Environment Variables
ABCTL_LOCAL_INSTALL_PASSWORD=foo
ABCTL_LOCAL_INSTALL_USERNAME=bar
```

Follow web app UI instructions to set up a source, destination and connection to replicate data. Connections support the most popular sync modes: full refresh, incremental and change data capture for databases.

Read the [Airbyte docs](https://docs.airbyte.com).

### Manage Airbyte configurations with code

You can also programmatically manage sources, destinations, and connections with YAML files, [Octavia CLI](https://github.com/airbytehq/airbyte/tree/master/octavia-cli), and API.

### Deploy Airbyte to production

Deployment options: [Docker](https://docs.airbyte.com/deploying-airbyte/local-deployment), [AWS EC2](https://docs.airbyte.com/deploying-airbyte/on-aws-ec2), [Azure](https://docs.airbyte.com/deploying-airbyte/on-azure-vm-cloud-shell), [GCP](https://docs.airbyte.com/deploying-airbyte/on-gcp-compute-engine), [Kubernetes](https://docs.airbyte.com/deploying-airbyte/on-kubernetes-via-helm), [Restack](https://docs.airbyte.com/deploying-airbyte/on-restack), [Plural](https://docs.airbyte.com/deploying-airbyte/on-plural), [Oracle Cloud](https://docs.airbyte.com/deploying-airbyte/on-oci-vm), [Digital Ocean](https://docs.airbyte.com/deploying-airbyte/on-digitalocean-droplet)...

### Use Airbyte Cloud

Airbyte Cloud is the fastest and most reliable way to run Airbyte. You can get started with free credits in minutes.

Sign up for [Airbyte Cloud](https://cloud.airbyte.io/signup).

## Contributing

Get started by checking Github issues and creating a Pull Request. An easy way to start contributing is to update an existing connector or create a new connector using the low-code and Python CDKs. You can find the code for existing connectors in the [connectors](https://github.com/airbytehq/airbyte/tree/master/airbyte-integrations/connectors) directory. The Airbyte platform is written in Java, and the frontend in React. You can also contribute to our docs and tutorials. Advanced Airbyte users can apply to the [Maintainer program](https://airbyte.com/maintainer-program) and [Writer Program](https://airbyte.com/write-for-the-community).

Read the [Contributing guide](https://docs.airbyte.com/contributing-to-airbyte/).

## Reporting vulnerabilities

⚠️ Please do not file GitHub issues or post on our public forum for security vulnerabilities as they are public! ⚠️

Airbyte takes security issues very seriously. If you have any concerns about Airbyte or believe you have uncovered a vulnerability, please get in touch via the e-mail address security@airbyte.io. In the message, try to provide a description of the issue and ideally a way of reproducing it. The security team will get back to you as soon as possible.

Note that this security address should be used only for undisclosed vulnerabilities. Dealing with fixed issues or general questions on how to use the security features should be handled regularly via the user and the dev lists. Please report any security problems to us before disclosing it publicly.

## License

See the [LICENSE](docs/project-overview/licenses/) file for licensing information, and our [FAQ](docs/project-overview/licenses/license-faq.md) for any questions you may have on that topic.

## Resources

- [Weekly office hours](https://airbyte.io/weekly-office-hours/) for live informal sessions with the Airbyte team
- [Slack](https://slack.airbyte.io) for quick discussion with the Community and Airbyte team
- [Discourse](https://discuss.airbyte.io/) for deeper conversations about features, connectors, and problems
- [GitHub](https://github.com/airbytehq/airbyte) for code, issues and pull requests
- [Youtube](https://www.youtube.com/c/AirbyteHQ) for videos on data engineering
- [Newsletter](https://airbyte.com/newsletter) for product updates and data news
- [Blog](https://airbyte.com/blog) for data insigts articles, tutorials and updates
- [Docs](https://docs.airbyte.com/) for Airbyte features
- [Roadmap](https://go.airbyte.com/roadmap) for planned features
