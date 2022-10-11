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

# Update and upgrade
RUN apt update \
    && apt upgrade -y

# Install basic tools
RUN apt install -y \
      curl \
      git \
      wget

# Install python 3.7
RUN apt install -y \
      python3.7-dev \
      python3.7-venv \
      python3-pip \
      python3-wheel \
    && update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.7 1 \
    && update-alternatives --install /usr/bin/python python /usr/bin/python3.7 1

# Install Java 8
RUN apt install -y \
      openjdk-8-jdk \
      openjdk-8-jre

# Install gcloud dependencies
RUN apt install -y \
      apt-transport-https \
      ca-certificates \
      gnupg

# Install maven
ENV JAVA_VER 8
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
ENV MAVEN_VERSION 3.8.6

# Maven args
ARG MAVEN_TAR_FILE="apache-maven-${MAVEN_VERSION}-bin.tar.gz"
ARG MAVEN_TAR_DOWNLOAD="https://dlcdn.apache.org/maven/maven-3/${MAVEN_VERSION}/binaries/${MAVEN_TAR_FILE}"
ARG MAVEN_CHECK_URL="https://downloads.apache.org/maven/maven-3/${MAVEN_VERSION}/binaries"
ARG MAVEN_CHECKSUM="${MAVEN_TAR_FILE}.sha512"
ARG MAVEN_CHECKSUM_DOWNLOAD="${MAVEN_CHECK_URL}/${MAVEN_CHECKSUM}"
ARG MAVEN_SIGNATURE="${MAVEN_TAR_FILE}.asc"
ARG MAVEN_SIGNATURE_DOWNLOAD="${MAVEN_CHECK_URL}/${MAVEN_SIGNATURE}"
ARG MAVEN_INSTALL_PATH="/usr/share/maven"
ARG MAVEN_KEYS_DOWNLOAD="https://downloads.apache.org/maven/KEYS"

# Download maven
RUN mkdir -p "${MAVEN_INSTALL_PATH}" \
  && wget -q "${MAVEN_TAR_DOWNLOAD}" \
  && wget -q "${MAVEN_CHECKSUM_DOWNLOAD}" \
  && wget -q "${MAVEN_SIGNATURE_DOWNLOAD}" \
  && wget -q "${MAVEN_KEYS_DOWNLOAD}"

# Verify maven
RUN $(echo $(cat "${MAVEN_CHECKSUM}") "${MAVEN_TAR_FILE}" | sha512sum --check --status) \
  && gpg --import KEYS \
  && gpg --verify "${MAVEN_SIGNATURE}" "${MAVEN_TAR_FILE}"

# Install maven
RUN tar -xzf "${MAVEN_TAR_FILE}" -C "${MAVEN_INSTALL_PATH}" --strip-components=1 \
  && rm "${MAVEN_TAR_FILE}" \
  && rm "${MAVEN_CHECKSUM}" \
  && rm "${MAVEN_SIGNATURE}" \
  && ls "${MAVEN_INSTALL_PATH}" \
  && ln -s "${MAVEN_INSTALL_PATH}/bin/mvn" /usr/bin/mvn

ENV MAVEN_HOME "${MAVEN_INSTALL_PATH}"

# Naming in-container user
ARG CURATION_USER="curation_nlp"
ARG CURATION_HOME="/home/${CURATION_USER}"

# gsdk args
ARG GSDK_VERSION="405.0.0"
ARG GSDK_CHECKSUM="ce70e0f2783bf3970acb60e943c9510d43c12a7f28c97aeb9dd52ff4a5fe62bf"
ARG GSDK_INSTALL_PATH="${CURATION_HOME}/google-cloud-sdk"
ARG GSDK_TAR_FILE="google-cloud-cli-${GSDK_VERSION}-linux-x86_64.tar.gz"
ARG GSDK_DOWNLOAD="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/${GSDK_TAR_FILE}"

# Download gsdk
RUN wget -q "${GSDK_DOWNLOAD}"

# Verify gsdk
RUN $(echo "${GSDK_CHECKSUM}" "${GSDK_TAR_FILE}" | sha256sum --check --status) \
    && mkdir -p "${GSDK_INSTALL_PATH}"

# Install gsdk
RUN tar -xzf "${GSDK_TAR_FILE}" -C "${CURATION_HOME}" \
    && rm "${GSDK_TAR_FILE}" \
    && cd "${GSDK_INSTALL_PATH}" \
    && ./install.sh -q

# Use bash and update path
SHELL ["/bin/bash", "-c"]
RUN echo "source ${GSDK_INSTALL_PATH}/path.bash.inc" | tee --append "${CURATION_HOME}/.bashrc" "${CURATION_HOME}/.profile" \
    && echo "source ${GSDK_INSTALL_PATH}/completion.bash.inc" | tee --append "${CURATION_HOME}/.bashrc" "${CURATION_HOME}/.profile" \
    && source "${CURATION_HOME}/.bashrc" \
    && gcloud components update

ENV PATH "${PATH}:${GSDK_INSTALL_PATH}/bin"
