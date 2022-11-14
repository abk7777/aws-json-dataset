# aws-json-dataset

## Table of Contents
- [aws-json-dataset](#aws-json-dataset)
  - [Table of Contents](#table-of-contents)
  - [Table of Contents](#table-of-contents-1)
  - [Project Structure](#project-structure)
  - [Description](#description)
  - [Quickstart](#quickstart)
  - [Installation](#installation)
    - [Prerequisites](#prerequisites)
    - [Creating a Python Virtual Environment](#creating-a-python-virtual-environment)
    - [Notebook Setup](#notebook-setup)
    - [Environment Variables](#environment-variables)
    - [AWS Credentials](#aws-credentials)
  - [AWS Deployment](#aws-deployment)
  - [Makefile Usage](#makefile-usage)
  - [Testing](#testing)
    - [Unit Tests](#unit-tests)
  - [Troubleshooting](#troubleshooting)
  - [References & Links](#references--links)
  - [Authors](#authors)
  - [Project Structure](#project-structure-1)
  - [Description](#description-1)
  - [Quickstart](#quickstart-1)
  - [Installation](#installation-1)
    - [Prerequisites](#prerequisites-1)
    - [Creating a Python Virtual Environment](#creating-a-python-virtual-environment-1)
    - [Notebook Setup](#notebook-setup-1)
    - [Environment Variables](#environment-variables-1)
    - [AWS Credentials](#aws-credentials-1)
  - [AWS Deployment](#aws-deployment-1)
  - [Makefile Usage](#makefile-usage-1)
  - [Testing](#testing-1)
    - [Unit Tests](#unit-tests-1)
  - [Troubleshooting](#troubleshooting-1)
  - [References & Links](#references--links-1)
  - [Authors](#authors-1)

## Table of Contents
- [aws-json-dataset](#aws-json-dataset)
  - [Table of Contents](#table-of-contents)
  - [Table of Contents](#table-of-contents-1)
  - [Project Structure](#project-structure)
  - [Description](#description)
  - [Quickstart](#quickstart)
  - [Installation](#installation)
    - [Prerequisites](#prerequisites)
    - [Creating a Python Virtual Environment](#creating-a-python-virtual-environment)
    - [Notebook Setup](#notebook-setup)
    - [Environment Variables](#environment-variables)
    - [AWS Credentials](#aws-credentials)
  - [AWS Deployment](#aws-deployment)
  - [Makefile Usage](#makefile-usage)
  - [Testing](#testing)
    - [Unit Tests](#unit-tests)
  - [Troubleshooting](#troubleshooting)
  - [References & Links](#references--links)
  - [Authors](#authors)
  - [Project Structure](#project-structure-1)
  - [Description](#description-1)
  - [Quickstart](#quickstart-1)
  - [Installation](#installation-1)
    - [Prerequisites](#prerequisites-1)
    - [Creating a Python Virtual Environment](#creating-a-python-virtual-environment-1)
    - [Notebook Setup](#notebook-setup-1)
    - [Environment Variables](#environment-variables-1)
    - [AWS Credentials](#aws-credentials-1)
  - [AWS Deployment](#aws-deployment-1)
  - [Makefile Usage](#makefile-usage-1)
  - [Testing](#testing-1)
    - [Unit Tests](#unit-tests-1)
  - [Troubleshooting](#troubleshooting-1)
  - [References & Links](#references--links-1)
  - [Authors](#authors-1)

<small><i><a href='http://ecotrust-canada.github.io/markdown-toc/'>Table of contents generated with markdown-toc</a></i></small>


## Project Structure


## Description

## Quickstart

## Installation
Follow the steps to set the deployment environment.

### Prerequisites

### Creating a Python Virtual Environment
When developing locally, create a Python virtual environment to manage dependencies:
```bash
python3 -m venv .venv-dev
source .venv-dev/bin/activate
pip install -U pip
pip install -r requirements-dev.txt
```

### Notebook Setup
To use the virtual environment inside Jupyter Notebook, first activate the virtual environment, then create a kernel for it.
```bash
# Install ipykernal and dot-env
pip install ipykernel python-dotenv jupyterthemes

# Add the kernel
python3 -m ipykernel install --user --name=<environment name>

# Delete the kernel
jupyter kernelspec uninstall <environment name>
```

### Environment Variables

Sensitive environment variables containing secrets like passwords and API keys must be exported to the environment first.

Create a `.env` file in the project root.
```bash

```

***Important:*** *Always use a `.env` file or AWS SSM Parameter Store or Secrets Manager for sensitive variables like credentials and API keys. Never hard-code them, including when developing. AWS will quarantine an account if any credentials get accidentally exposed and this will cause problems.*

***Make sure that `.env` is listed in `.gitignore`***

### AWS Credentials
Valid AWS credentials must be available to AWS CLI and SAM CLI. The easiest way to do this is running `aws configure`, or by adding them to `~/.aws/credentials` and exporting the `AWS_PROFILE` variable to the environment.

For more information visit the documentation page:
[Configuration and credential file settings](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html)

## AWS Deployment
Once an AWS profile is configured and environment variables are exported, the application can be deployed using `make`.
```bash
make deploy
```

## Makefile Usage
```bash
# Deploy all layers
make deploy

# Delete all layers (data in S3 must be deleted manually first)
make delete

# Deploy only one layer
make emr.deploy

# Delete only one layer
make emr.delete
```

## Testing
### Unit Tests
Create a Python virtual environment to manage test dependencies.

```bash
python3 -m venv .venv-test
source .venv-test/bin/activate
pip install -U pip
pip install -r requirements-tests.txt
```
Run tests with the following command.
```bash
coverage run -m pytest
```

## Troubleshooting
* Check your AWS credentials in `~/.aws/credentials`
* Check that the environment variables are available to the services that need them
* Check that the correct environment or interpreter is being used for Python

## References & Links

## Authors
**Primary Contact:** Gregory Lindsey (@abk7777)

    - [Unit Tests](#unit-tests)
  - [Troubleshooting](#troubleshooting)
  - [References & Links](#references--links)
  - [Authors](#authors)

<small><i><a href='http://ecotrust-canada.github.io/markdown-toc/'>Table of contents generated with markdown-toc</a></i></small>


## Project Structure


## Description

## Quickstart

## Installation
Follow the steps to set the deployment environment.

### Prerequisites

### Creating a Python Virtual Environment
When developing locally, create a Python virtual environment to manage dependencies:
```bash
python3 -m venv .venv-dev
source .venv-dev/bin/activate
pip install -U pip
pip install -r requirements-dev.txt
```

### Notebook Setup
To use the virtual environment inside Jupyter Notebook, first activate the virtual environment, then create a kernel for it.
```bash
# Install ipykernal and dot-env
pip install ipykernel python-dotenv jupyterthemes

# Add the kernel
python3 -m ipykernel install --user --name=<environment name>

# Delete the kernel
jupyter kernelspec uninstall <environment name>
```

### Environment Variables

Sensitive environment variables containing secrets like passwords and API keys must be exported to the environment first.

Create a `.env` file in the project root.
```bash

```

***Important:*** *Always use a `.env` file or AWS SSM Parameter Store or Secrets Manager for sensitive variables like credentials and API keys. Never hard-code them, including when developing. AWS will quarantine an account if any credentials get accidentally exposed and this will cause problems.*

***Make sure that `.env` is listed in `.gitignore`***

### AWS Credentials
Valid AWS credentials must be available to AWS CLI and SAM CLI. The easiest way to do this is running `aws configure`, or by adding them to `~/.aws/credentials` and exporting the `AWS_PROFILE` variable to the environment.

For more information visit the documentation page:
[Configuration and credential file settings](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html)

## AWS Deployment
Once an AWS profile is configured and environment variables are exported, the application can be deployed using `make`.
```bash
make deploy
```

## Makefile Usage
```bash
# Deploy all layers
make deploy

# Delete all layers (data in S3 must be deleted manually first)
make delete

# Deploy only one layer
make emr.deploy

# Delete only one layer
make emr.delete
```

## Testing
### Unit Tests
Create a Python virtual environment to manage test dependencies.

```bash
python3 -m venv .venv-test
source .venv-test/bin/activate
pip install -U pip
pip install -r requirements-tests.txt
```
Run tests with the following command.
```bash
coverage run -m pytest
```

## Troubleshooting
* Check your AWS credentials in `~/.aws/credentials`
* Check that the environment variables are available to the services that need them
* Check that the correct environment or interpreter is being used for Python

## References & Links

## Authors
**Primary Contact:** Gregory Lindsey (@abk7777)
