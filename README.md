# All of Us Curation NLP

[![Circle CI](https://circleci.com/gh/all-of-us/curation-nlp/tree/master.svg?style=shield)](https://circleci.com/gh/all-of-us/curation-nlp)

## Purpose of this document

Describes the All of Us NLP deliverables associated with data ingestion and quality control, intended to support 
alpha release requirements. This document is version controlled; you should read the version that lives in the branch 
or tag you need. The specification document should always be consistent with the implemented curation processes. 

## Directory Overview

*   `src` 
    *   Source code in Java
    *   `main` - Scripts for setup, maintenance, deployment, etc.
    *   `test` - Unit tests.
*   `docker`
    *   Dockerfile with all tools necessary for running the package
*   `config`
    *   Cloud Build configuration.

## Developer setup

Please reference [this guide](https://docs.google.com/document/d/186jPUIcerFsg833OKZnAyESdVy0lXQoMFiGD9r-084M/edit)
for development setup.

## Usage

Ensure the required environment variables are set as indicated in the developer guide.\
The following command can be used to build maven for different profiles
* `mvn clean install -U -P {profile}` where profile can be `direct`, `spark`, `flink` and `dataflow`\

To deploy to Google Dataflow, use the following command
* <code>java -cp \
  target/curation-nlp-bundled-dataflow-1.2-SNAPSHOT.jar \
  org.allofus.curation.pipeline.CurationNLPMain \
  --runner=DataflowRunner \
  --gcpTempLocation={bucket}/gcp_tmp \
  --stagingLocation={bucket}/staging \
  --tempLocation={bucket}/tmp \
  --resourcesDir={bucket}/resources \
  --input={bucket}/input --output={bucket}/output \
  --inputType=jsonl --outputType=jsonl \
  --project={project} --region={region} \
  --subnetwork={subnet} \
  --usePublicIps=false \
  --maxNumWorkers=5 --numberOfWorkerHarnessThreads=2 \
  --workerMachineType=n1-highmem-4 --diskSizeGb=50 \
  --experiments=use_runner_v2 \
  --pipeline={pipeline} --maxClampThreads=4 \
  --maxOutputPartitionSeconds=60 --maxOutputBatchSize=100 \
  [--streaming --enableStreamingEngine]</code>

## Authentication Details

All actors calling APIs in production will use [service accounts](https://cloud.google.com/compute/docs/access/service-accounts).
