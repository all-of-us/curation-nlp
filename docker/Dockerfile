FROM ubuntu:18.04
LABEL org.opencontainers.image.authors="np2689@cumc.columbia.edu"
LABEL org.opencontainers.image.url="https://github.com/all-of-us/curation-nlp"
LABEL org.opencontainers.image.documentation="https://github.com/all-of-us/curation-nlp"
LABEL org.opencontainers.image.licenses="MIT"
LABEL org.opencontainers.image.title="All of Us Curation NLP development image"
LABEL org.opencontainers.image.description="Base development container image used by the All of Us Curation NLP team"

### DESCRIPTION

# provide a universal base image upon which all subsequent development images can be derived

### CONTAINER BUILD

## OS INIT
# run the base container init first, as it is highly unlikely to change.
# this allows us to modify subsequent steps without initating a full image rebuild
RUN apt update \
    && apt upgrade -y \
    && apt install -y \
      curl \
      git \
      wget \
      openjdk-8-jdk \
      maven \
      python3.7-dev \
      python3.7-venv \
      python3-pip \
      python3-wheel \
    && update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.7 1 \
    && update-alternatives --install /usr/bin/python python /usr/bin/python3.7 1

## BUILD ARGS

# this will be the name of our in-container user
ARG CURATION_USER="curation_nlp"
ARG CURATION_HOME="/home/${CURATION_USER}"

# gsdk args
ARG GSDK_VERSION="360.0.0"
ARG GSDK_CHECKSUM="6192cb2791f592da6d372888dbd7ee81d3d91f255d844ab5fc518fc97e453648"
ARG GSDK_INSTALL_PATH="${CURATION_HOME}/google-cloud-sdk"
ARG GSDK_TAR_FILE="google-cloud-sdk-${GSDK_VERSION}-linux-x86_64.tar.gz"
ARG GSDK_DOWNLOAD="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/${GSDK_TAR_FILE}"

# download gsdk and verify checksum
RUN wget --quiet "${GSDK_DOWNLOAD}" \
    && if ! $(echo "${GSDK_CHECKSUM}" "${GSDK_TAR_FILE}" | sha256sum --check --status); \
        then echo "GSDK tar integrity check failure, please update build arg with correct checksum and/or version.";\
        exit 1; \
    fi;

# install gsdk,
RUN mkdir -p "${GSDK_INSTALL_PATH}" \
    && tar -xzf "${GSDK_TAR_FILE}" -C "${CURATION_HOME}" \
    && rm "${GSDK_TAR_FILE}" \
    && cd "${GSDK_INSTALL_PATH}" \
    && ./install.sh --quiet

# Use bash and update path
SHELL ["/bin/bash", "-c"]
RUN echo "source ${GSDK_INSTALL_PATH}/path.bash.inc" | tee --append "${CURATION_HOME}/.bashrc" "${CURATION_HOME}/.profile" \
    && echo "source ${GSDK_INSTALL_PATH}/completion.bash.inc" | tee --append "${CURATION_HOME}/.bashrc" "${CURATION_HOME}/.profile" \
    && source "${CURATION_HOME}/.bashrc" \
    && gcloud components update
