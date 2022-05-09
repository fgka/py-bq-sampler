# Customized version of Sendmail PubSub

This documentation relies on [Sending emails natively from Google Cloud Pub/Sub events](https://medium.com/google-cloud/sending-emails-natively-from-google-cloud-pub-sub-events-f8260ffa6a89).
With code hosted on github [GoogleCloudPlatform/cloud-pubsub-sendmail](https://github.com/GoogleCloudPlatform/cloud-pubsub-sendmail).


## Create infrastructure

### Definitions

```bash
export EMAIL_FUNCTION_NAME="${FUNCTION_NAME}-error-sendmail"
export EMAIL_FUNCTION_SA="${EMAIL_FUNCTION_NAME}-sa"
```

### Service Account

```bash
gcloud iam service-accounts create "${EMAIL_FUNCTION_SA}" \
  --project="${PROJECT_ID}" \
  --description="Service Account used by CloudFunction ${EMAIL_FUNCTION_NAME} to error emails"
```

## Define e-mail variables

```bash
export MAIL_TO="YOUR_EMAIL_ADDRESS"
export MAIL_FROM="${MAIL_TO}"
export MAIL_SERVER="smtp-relay.gmail.com:587"
export MAIL_SUBJECT="[ERROR] Function ${FUNCTION_NAME} error"
export MAIL_LOCAL_HOST="pubsub-sendmail-nat.example.com"
export MAIL_DEBUG="TRUE"
export MAIL_FORCE_TLS="FALSE"
```

### Deploy the function

```bash
REPO_DIR=`mktemp -d`
echo "Code being pulled into ${REPO_DIR}"
pushd ${REPO_DIR}
git clone https://github.com/GoogleCloudPlatform/cloud-pubsub-sendmail.git
pushd cloud-pubsub-sendmail

echo "Patching requirements.txt"
cat > requirements.txt << __END__
Flask>=2.1.1
google-cloud-error-reporting>=1.5.2
__END__

unset ENV_VARS
set -a ENV_VARS
ENV_VARS+=("MAIL_FROM=${MAIL_FROM}")
ENV_VARS+=("MAIL_TO=${MAIL_TO}")
ENV_VARS+=("MAIL_SERVER=${MAIL_SERVER}")
ENV_VARS+=("MAIL_SUBJECT=${MAIL_SUBJECT}")
ENV_VARS+=("MAIL_LOCAL_HOST=${MAIL_LOCAL_HOST}")
ENV_VARS+=("MAIL_DEBUG=${MAIL_DEBUG}")
ENV_VARS+=("MAIL_FORCE_TLS=${MAIL_FORCE_TLS}")
ENV_VARS_COMMA=$(local IFS=","; echo "${ENV_VARS[*]}")

echo "Deploying ${EMAIL_FUNCTION_NAME}"
gcloud functions deploy ${EMAIL_FUNCTION_NAME} \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --runtime="python39" \
  --entry-point="pubsub_sendmail" \
  --service-account="${EMAIL_FUNCTION_SA}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --trigger-topic="${PUBSUB_ERROR_TOPIC}" \
  --set-env-vars="${ENV_VARS_COMMA}"

popd
popd
```

### Test the function

```bash
gcloud pubsub topics publish ${PUBSUB_ERROR_TOPIC} \
  --project="${PROJECT_ID}" \
  --message "This is a test"
  
echo "You should have received an email at ${MAIL_TO}"
```