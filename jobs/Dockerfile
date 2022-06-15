FROM --platform=linux/amd64 debian:buster-slim

# Source: https://github.com/phusion/baseimage-docker/issues/58
ENV DEBIAN_FRONTEND noninteractive

## Required for GCloud CLI install
RUN apt-get update \
  && apt-get -y install apt-utils \
  && apt-get -y upgrade \
  && apt-get -y install \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg

## GSutil/GCloud CLI
# Source: https://cloud.google.com/storage/docs/gsutil_install#deb
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" \
  | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg \
  | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - \
    && apt-get update -y \
    && apt-get install google-cloud-cli -y

# equivalent to: gcloud components update
RUN apt-get update \
  && apt-get --only-upgrade install \
    google-cloud-sdk \
    google-cloud-sdk-anthos-auth \
    google-cloud-sdk-app-engine-go \
    google-cloud-sdk-app-engine-grpc \
    google-cloud-sdk-app-engine-java \
    google-cloud-sdk-app-engine-python \
    google-cloud-sdk-app-engine-python-extras \
    google-cloud-sdk-bigtable-emulator \
    google-cloud-sdk-cbt \
    google-cloud-sdk-cloud-build-local \
    google-cloud-sdk-cloud-run-proxy \
    google-cloud-sdk-config-connector \
    google-cloud-sdk-datalab \
    google-cloud-sdk-datastore-emulator \
    google-cloud-sdk-firestore-emulator \
    google-cloud-sdk-gke-gcloud-auth-plugin \
    google-cloud-sdk-kpt \
    google-cloud-sdk-kubectl-oidc \
    google-cloud-sdk-local-extract \
    google-cloud-sdk-minikube \
    google-cloud-sdk-nomos \
    google-cloud-sdk-pubsub-emulator \
    google-cloud-sdk-skaffold \
    google-cloud-sdk-spanner-emulator \
    google-cloud-sdk-terraform-tools \
    kubectl
 
## Install Python3 and clean-up APT for cleaner/smaller images
RUN apt-get update \
  && apt-get -y upgrade \
  && apt-get -y install \
    python3 \
    python3-pip \
  && apt-get -y autoclean

## Install Python3 minimum libraries
RUN python3 -m pip install -U pip \
  && python3 -m pip install -U \
    keyring \
    keyrings.google-artifactregistry-auth \
    twine \
    virtualenv

## Custom code for pushing packages onto Artifact Registry
COPY cmd.sh .

RUN chmod 755 cmd.sh

CMD [ "./cmd.sh" ]
