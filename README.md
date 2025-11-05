# oc_autoworkflow
**oc_autoworkflow** is an automatised version of the OpenCitations workflow for the complementation of OpenCitations databases, prepared as the practical part of a Master's thesis in Digital Humanities and Digital Knowledge at Alma Mater Studiorum - Universita' di Bologna in Academic Year 2025/26.

## Prerequisites
### To install:
Python 3.12.4<
[Luigi](https://github.com/spotify/luigi)
[Pandas](https://github.com/pandas-dev/pandas)
[PYYaml](https://github.com/yaml/pyyaml)
[ruamel.yaml](https://github.com/commx/ruamel-yaml)
[oc_ocdm](https://github.com/opencitations/oc_ocdm)
[oc_validator](https://github.com/opencitations/oc_validator)
[virtuoso_utilities](https://github.com/opencitations/virtuoso_utilities)
[oc_index](https://github.com/opencitations/index)
[oc_meta dependencies(redis, SPARQLWrapper, tqdm, yaml, pebble, time_agnostic_library)]()

[OpenLink Virtuoso](https://github.com/openlink/virtuoso-opensource) installed

[Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running

### Cloned repositories:
[oc_meta](https://github.com/opencitations/oc_meta) code stored locally
[oc_index](https://github.com/opencitations/index) code stored locally
[virtuoso_utilities](https://github.com/opencitations/virtuoso_utilities) stored locally

## Installation
**oc_autoworkflow** is a stand-alone script called with a command-line interface.
It's recommended, but not necessary, to keep the three OpenCitations repositories mentioned in the previous sections in parallel folders, eg.
> opencitations
> opencitations/oc_autoworkflow
> opencitations/oc_meta
> opencitations/index
> opencitations/virtuoso_utilities

## Usage

First, configure the script by editing the *config.yaml* file.
If you are following the recommended repository structure from the previous section, the *DIRECTORIES* subsection does not need to be changed.

Before running **oc_autoworkflow**, *Luigi*'s central scheduler needs to be turned on with a command-line interface using the following command:
`luigid`
The CLI window used to run this command needs to be open during the whole runtime of **oc_autoworkflow**.

Afterwards, run the script using:
`python workflow.py`
Most of the "tasks" that combined create this workflow utilise pre-existing OpenCitations tools and scripts, that print information regarding their runtime in console.
You can also open *Luigi*'s web interface at *localhost:8082* for more information regarding the tasks running as part of the script.

The script can also be used with a local scheduler, by using:
`python workflow.py --local-scheduler`
Keep in mind that without central scheduler, Luigi's web application is not available.