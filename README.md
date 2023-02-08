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

## Authentication Details

All actors calling APIs in production will use [service accounts](https://cloud.google.com/compute/docs/access/service-accounts).
