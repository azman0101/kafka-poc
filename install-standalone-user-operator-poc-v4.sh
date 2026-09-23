#!/usr/bin/env bash
set -Eeuo pipefail

###############################################################################
# Standalone Strimzi User Operator PoC
#
# Purpose
#   Prove that the Strimzi User Operator can manage KafkaUser/ACL state against
#   a Kafka cluster that is NOT managed by the Strimzi Cluster Operator.
#
# Components
#   - Apache Kafka 4.3.1, deployed by HelmForge Kafka chart 1.3.14
#   - cert-manager, used as the PoC CA / certificate issuer
#   - Strimzi User Operator 1.2.0 ONLY
#
# Kafka:
#   - KRaft
#   - one broker/controller
#   - TLS + mTLS on the client listener
#   - TLS + mTLS on the KRaft controller listener
#   - StandardAuthorizer
#
# Test identity:
#   catalog-product-updated.cart.xxxxxx.io
#
# The external client certificate has:
#   CN=catalog-product-updated.cart.xxxxxx.io
#
# No Strimzi Cluster Operator is installed.
###############################################################################

NAMESPACE="${NAMESPACE:-kafka-security}"

STRIMZI_VERSION="${STRIMZI_VERSION:-1.2.0}"
KAFKA_CHART_VERSION="${KAFKA_CHART_VERSION:-1.3.14}"
KAFKA_IMAGE_TAG="${KAFKA_IMAGE_TAG:-4.3.1}"
CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.21.2}"

KAFKA_RELEASE="${KAFKA_RELEASE:-kafka}"
KAFKA_FULLNAME="${KAFKA_FULLNAME:-kafka}"

KAFKA_USER="${KAFKA_USER:-catalog-product-updated.cart.xxxxxx.io}"
KAFKA_TOPIC="${KAFKA_TOPIC:-poc-user-operator-topic}"
KAFKA_GROUP="${KAFKA_GROUP:-poc-user-operator-group}"

KAFKA_CLIENT_PORT="${KAFKA_CLIENT_PORT:-9092}"
KAFKA_CONTROLLER_PORT="${KAFKA_CONTROLLER_PORT:-9093}"

KAFKA_SERVICE="${KAFKA_SERVICE:-${KAFKA_FULLNAME}}"
KAFKA_HEADLESS_SERVICE="${KAFKA_HEADLESS_SERVICE:-${KAFKA_FULLNAME}-headless}"
KAFKA_POD="${KAFKA_POD:-${KAFKA_FULLNAME}-0}"

ROOT_CA_CERTIFICATE="${ROOT_CA_CERTIFICATE:-kafka-root-ca}"
ROOT_CA_SECRET="${ROOT_CA_SECRET:-kafka-root-ca}"
KAFKA_CA_ISSUER="${KAFKA_CA_ISSUER:-kafka-ca}"

BROKER_CERTIFICATE="${BROKER_CERTIFICATE:-kafka-broker}"
BROKER_CERT_SECRET="${BROKER_CERT_SECRET:-kafka-broker-tls}"

UO_CERTIFICATE="${UO_CERTIFICATE:-strimzi-user-operator}"
UO_CERT_SECRET="${UO_CERT_SECRET:-strimzi-user-operator-certs}"

# Secrets consumed by the standalone User Operator.
CLIENTS_CA_CERT_SECRET="${CLIENTS_CA_CERT_SECRET:-kafka-clients-ca-cert}"
CLIENTS_CA_KEY_SECRET="${CLIENTS_CA_KEY_SECRET:-kafka-clients-ca}"
CLUSTER_CA_CERT_SECRET="${CLUSTER_CA_CERT_SECRET:-kafka-cluster-ca-cert}"

USER_CERTIFICATE="${USER_CERTIFICATE:-${KAFKA_USER}}"
USER_CERT_SECRET="${USER_CERT_SECRET:-${KAFKA_USER}-tls}"

PASSWORD="${POC_KEYSTORE_PASSWORD:-poc-changeit}"
# kubectl uses the current context or the KUBECONFIG environment variable.
TMP_ROOT="${TMP_ROOT:-$(mktemp -d)}"
trap 'rm -rf "$TMP_ROOT"' EXIT

log() {
  printf '\n[%s] %s\n' "$(date +'%H:%M:%S')" "$*"
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Missing command: $1"
}

wait_rollout() {
  kubectl -n "$NAMESPACE" rollout status "deployment/$1" --timeout=10m
}

wait_cert() {
  kubectl -n "$NAMESPACE" wait \
    --for=condition=Ready \
    "certificate/$1" \
    --timeout=5m
}

require_cluster_access() {
  kubectl version --request-timeout=15s >/dev/null
}

ensure_no_cluster_operator() {
  if kubectl get deployments.apps -A --no-headers 2>/dev/null \
      | awk '$2=="strimzi-cluster-operator" {found=1} END {exit !found}'; then
    die "A Strimzi Cluster Operator deployment already exists."
  fi
}

install_cert_manager() {
  log "Installing cert-manager ${CERT_MANAGER_VERSION}"

  helm upgrade --install cert-manager \
    oci://quay.io/jetstack/charts/cert-manager \
    --version "$CERT_MANAGER_VERSION" \
    --namespace cert-manager \
    --create-namespace \
    --set crds.enabled=true \
    --wait \
    --timeout=15m

  kubectl -n cert-manager rollout status deployment/cert-manager --timeout=5m
  kubectl -n cert-manager rollout status deployment/cert-manager-webhook --timeout=5m
  kubectl -n cert-manager rollout status deployment/cert-manager-cainjector --timeout=5m
}

create_ca() {
  log "Creating PoC root CA and CA Issuer"

  kubectl apply -f - <<YAML
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: poc-selfsigned
  namespace: ${NAMESPACE}
spec:
  selfSigned: {}
---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: ${ROOT_CA_CERTIFICATE}
  namespace: ${NAMESPACE}
spec:
  secretName: ${ROOT_CA_SECRET}
  isCA: true
  commonName: Kafka-PoC-Root-CA
  duration: 8760h
  renewBefore: 720h
  privateKey:
    algorithm: RSA
    size: 3072
  issuerRef:
    name: poc-selfsigned
    kind: Issuer
---
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: ${KAFKA_CA_ISSUER}
  namespace: ${NAMESPACE}
spec:
  ca:
    secretName: ${ROOT_CA_SECRET}
YAML

  wait_cert "$ROOT_CA_CERTIFICATE"
}

create_broker_certificate() {
  log "Creating Kafka broker/controller certificate"

  kubectl apply -f - <<YAML
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: ${BROKER_CERTIFICATE}
  namespace: ${NAMESPACE}
spec:
  secretName: ${BROKER_CERT_SECRET}
  commonName: kafka-broker
  duration: 8760h
  renewBefore: 720h
  dnsNames:
    - ${KAFKA_SERVICE}
    - ${KAFKA_SERVICE}.${NAMESPACE}
    - ${KAFKA_SERVICE}.${NAMESPACE}.svc
    - ${KAFKA_SERVICE}.${NAMESPACE}.svc.cluster.local
    - ${KAFKA_POD}
    - ${KAFKA_POD}.${KAFKA_HEADLESS_SERVICE}
    - ${KAFKA_POD}.${KAFKA_HEADLESS_SERVICE}.${NAMESPACE}
    - ${KAFKA_POD}.${KAFKA_HEADLESS_SERVICE}.${NAMESPACE}.svc
    - ${KAFKA_POD}.${KAFKA_HEADLESS_SERVICE}.${NAMESPACE}.svc.cluster.local
  usages:
    - server auth
    - client auth
  privateKey:
    algorithm: RSA
    size: 2048
  issuerRef:
    name: ${KAFKA_CA_ISSUER}
    kind: Issuer
YAML

  wait_cert "$BROKER_CERTIFICATE"
}

create_uo_certificate() {
  log "Creating certificate for the standalone User Operator Kafka Admin client"

  kubectl apply -f - <<YAML
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: ${UO_CERTIFICATE}
  namespace: ${NAMESPACE}
spec:
  secretName: ${UO_CERT_SECRET}-certmanager
  commonName: strimzi-user-operator
  duration: 8760h
  renewBefore: 720h
  usages:
    - client auth
  privateKey:
    algorithm: RSA
    size: 2048
  issuerRef:
    name: ${KAFKA_CA_ISSUER}
    kind: Issuer
YAML

  wait_cert "$UO_CERTIFICATE"
}

create_external_user_certificate() {
  log "Creating externally managed KafkaUser certificate"

  kubectl apply -f - <<YAML
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: ${USER_CERTIFICATE}
  namespace: ${NAMESPACE}
spec:
  secretName: ${USER_CERT_SECRET}
  commonName: ${KAFKA_USER}
  duration: 8760h
  renewBefore: 720h
  usages:
    - client auth
  privateKey:
    algorithm: RSA
    size: 2048
  issuerRef:
    name: ${KAFKA_CA_ISSUER}
    kind: Issuer
YAML

  wait_cert "$USER_CERTIFICATE"
}

make_pkcs12() {
  local crt="$1"
  local key="$2"
  local ca="$3"
  local output="$4"
  local alias="$5"

  local password_file="${output}.password"
  printf '%s' "$PASSWORD" > "$password_file"

  openssl pkcs12 -export \
    -out "$output" \
    -inkey "$key" \
    -in "$crt" \
    -certfile "$ca" \
    -name "$alias" \
    -passout "file:${password_file}"

  rm -f "$password_file"
}

make_truststore() {
  local ca="$1"
  local output="$2"

  # Build a Java PKCS#12 truststore containing a real
  # TrustedCertificateEntry. Using "openssl pkcs12 -export -nokeys"
  # creates a certificate bag that can result in an empty Java trust-anchor
  # set and the error:
  #   InvalidAlgorithmParameterException:
  #   the trustAnchors parameter must be non-empty
  rm -f "$output"

  keytool -importcert \
    -noprompt \
    -alias kafka-ca \
    -file "$ca" \
    -keystore "$output" \
    -storetype PKCS12 \
    -storepass "$PASSWORD"

  keytool -list \
    -keystore "$output" \
    -storetype PKCS12 \
    -storepass "$PASSWORD" \
    -alias kafka-ca >/dev/null
}

extract_secret_files() {
  local secret="$1"
  local prefix="$2"

  kubectl -n "$NAMESPACE" get secret "$secret" \
    -o jsonpath='{.data.tls\.crt}' \
    | base64 -d > "${prefix}.crt"

  kubectl -n "$NAMESPACE" get secret "$secret" \
    -o jsonpath='{.data.tls\.key}' \
    | base64 -d > "${prefix}.key"

  kubectl -n "$NAMESPACE" get secret "$ROOT_CA_SECRET" \
    -o jsonpath='{.data.tls\.crt}' \
    | base64 -d > "${prefix}.ca.crt"
}

create_broker_pkcs12_secret() {
  log "Creating broker PKCS#12 keystore/truststore"

  local d="${TMP_ROOT}/broker"
  mkdir -p "$d"

  extract_secret_files "$BROKER_CERT_SECRET" "$d/broker"

  make_pkcs12 \
    "$d/broker.crt" \
    "$d/broker.key" \
    "$d/broker.ca.crt" \
    "$d/broker.p12" \
    kafka-broker

  make_truststore \
    "$d/broker.ca.crt" \
    "$d/truststore.p12"

  kubectl -n "$NAMESPACE" delete secret "${BROKER_CERT_SECRET}-pkcs12" \
    --ignore-not-found >/dev/null

  kubectl -n "$NAMESPACE" create secret generic "${BROKER_CERT_SECRET}-pkcs12" \
    --from-file=broker.p12="$d/broker.p12" \
    --from-file=truststore.p12="$d/truststore.p12" \
    --from-file=ca.crt="$d/broker.ca.crt" \
    --from-literal=password="$PASSWORD"
}

create_strimzi_ca_secrets() {
  log "Creating CA Secrets expected by standalone User Operator"

  local d="${TMP_ROOT}/ca"
  mkdir -p "$d"

  kubectl -n "$NAMESPACE" get secret "$ROOT_CA_SECRET" \
    -o jsonpath='{.data.tls\.crt}' \
    | base64 -d > "$d/ca.crt"

  kubectl -n "$NAMESPACE" get secret "$ROOT_CA_SECRET" \
    -o jsonpath='{.data.tls\.key}' \
    | base64 -d > "$d/ca.key"

  make_truststore "$d/ca.crt" "$d/ca.p12"

  kubectl -n "$NAMESPACE" delete secret \
    "$CLIENTS_CA_CERT_SECRET" \
    "$CLIENTS_CA_KEY_SECRET" \
    "$CLUSTER_CA_CERT_SECRET" \
    --ignore-not-found >/dev/null

  # STRIMZI_CA_CERT_NAME -> ca.crt
  kubectl -n "$NAMESPACE" create secret generic "$CLIENTS_CA_CERT_SECRET" \
    --from-file=ca.crt="$d/ca.crt"

  # STRIMZI_CA_KEY_NAME -> ca.key
  kubectl -n "$NAMESPACE" create secret generic "$CLIENTS_CA_KEY_SECRET" \
    --from-file=ca.key="$d/ca.key"

  # STRIMZI_CLUSTER_CA_CERT_SECRET_NAME -> ca.crt
  kubectl -n "$NAMESPACE" create secret generic "$CLUSTER_CA_CERT_SECRET" \
    --from-file=ca.crt="$d/ca.crt" \
    --from-file=ca.p12="$d/ca.p12" \
    --from-literal=ca.password="$PASSWORD"
}

create_uo_admin_secret() {
  log "Creating User Operator Kafka Admin TLS Secret"

  local d="${TMP_ROOT}/uo"
  mkdir -p "$d"

  extract_secret_files "${UO_CERT_SECRET}-certmanager" "$d/uo"

  # Strimzi 1.2.0 uses PemAuthIdentity which expects PKCS#8 private key and PEM certificate:
  # entity-operator.key and entity-operator.crt
  openssl pkcs8 -topk8 -nocrypt -in "$d/uo.key" -out "$d/entity-operator.key"
  cp "$d/uo.crt" "$d/entity-operator.crt"

  make_pkcs12 \
    "$d/uo.crt" \
    "$d/uo.key" \
    "$d/uo.ca.crt" \
    "$d/entity-operator.p12" \
    strimzi-user-operator

  printf '%s' "$PASSWORD" > "$d/entity-operator.password"

  make_truststore "$d/uo.ca.crt" "$d/truststore.p12"

  kubectl -n "$NAMESPACE" delete secret "$UO_CERT_SECRET" \
    --ignore-not-found >/dev/null

  kubectl -n "$NAMESPACE" create secret generic "$UO_CERT_SECRET" \
    --from-file=entity-operator.key="$d/entity-operator.key" \
    --from-file=entity-operator.crt="$d/entity-operator.crt" \
    --from-file=entity-operator.p12="$d/entity-operator.p12" \
    --from-file=entity-operator.password="$d/entity-operator.password" \
    --from-file=truststore.p12="$d/truststore.p12"
}

create_external_user_client_secret() {
  log "Creating client material for ${KAFKA_USER}"

  local d="${TMP_ROOT}/user"
  mkdir -p "$d"

  extract_secret_files "$USER_CERT_SECRET" "$d/user"

  make_pkcs12 \
    "$d/user.crt" \
    "$d/user.key" \
    "$d/user.ca.crt" \
    "$d/user.p12" \
    "$KAFKA_USER"

  make_truststore "$d/user.ca.crt" "$d/truststore.p12"

  printf '%s' "$PASSWORD" > "$d/password"

  kubectl -n "$NAMESPACE" delete secret "${USER_CERT_SECRET}-client" \
    --ignore-not-found >/dev/null

  kubectl -n "$NAMESPACE" create secret generic "${USER_CERT_SECRET}-client" \
    --from-file=user.p12="$d/user.p12" \
    --from-file=truststore.p12="$d/truststore.p12" \
    --from-file=password="$d/password"
}

patch_kafka_readiness_probe() {
  log "Replacing HelmForge plaintext readiness probe with a TLS+mTLS Kafka API probe"

  # The Kafka client listener advertises the Service DNS (kafka.kafka-security.svc.cluster.local:9092).
  # Allow traffic to unready pods so the readiness probe can connect via the advertised listener.
  kubectl -n "$NAMESPACE" patch svc "$KAFKA_SERVICE" \
    -p '{"spec":{"publishNotReadyAddresses":true}}'

  local container_name
  container_name="$(
    kubectl -n "$NAMESPACE" get statefulset "$KAFKA_FULLNAME" \
      -o jsonpath='{.spec.template.spec.containers[0].name}'
  )"

  [[ -n "$container_name" ]] || die "Unable to determine Kafka container name"

  cat > "${TMP_ROOT}/kafka-readiness-patch.yaml" <<YAML
spec:
  template:
    spec:
      containers:
        - name: ${container_name}
          readinessProbe:
            exec:
              command:
                - /bin/sh
                - -ec
                - |
                  PASSWORD="\$(cat /opt/kafka/tls/password)"
                  PROPERTIES=/tmp/kafka-readiness.properties

                  cat > "\${PROPERTIES}" <<EOF
                  security.protocol=SSL
                  ssl.keystore.type=PKCS12
                  ssl.keystore.location=/opt/kafka/tls/broker.p12
                  ssl.keystore.password=\${PASSWORD}
                  ssl.truststore.type=PKCS12
                  ssl.truststore.location=/opt/kafka/tls/truststore.p12
                  ssl.truststore.password=\${PASSWORD}
                  ssl.endpoint.identification.algorithm=
                  EOF

                  /opt/kafka/bin/kafka-broker-api-versions.sh \
                    --bootstrap-server localhost:${KAFKA_CLIENT_PORT} \
                    --command-config "\${PROPERTIES}" \
                    >/dev/null 2>&1
            initialDelaySeconds: 10
            timeoutSeconds: 10
            periodSeconds: 10
            failureThreshold: 6
            successThreshold: 1
YAML

  kubectl -n "$NAMESPACE" patch statefulset "$KAFKA_FULLNAME" \
    --type=strategic \
    --patch-file "${TMP_ROOT}/kafka-readiness-patch.yaml"

  # Delete initial unready pod to trigger immediate recreation with the new probe
  kubectl -n "$NAMESPACE" delete pod "$KAFKA_POD" --ignore-not-found >/dev/null 2>&1 || true

  log "Waiting for Kafka StatefulSet rollout after readiness probe patch"
  kubectl -n "$NAMESPACE" rollout status \
    "statefulset/${KAFKA_FULLNAME}" \
    --timeout=10m
}

install_kafka() {
  log "Installing Apache Kafka ${KAFKA_IMAGE_TAG} with HelmForge ${KAFKA_CHART_VERSION}"

  helm repo add helmforge https://repo.helmforge.dev >/dev/null 2>&1 || true
  helm repo update helmforge >/dev/null

  cat > "${TMP_ROOT}/kafka-values.yaml" <<YAML
architecture: single-broker

fullnameOverride: ${KAFKA_FULLNAME}

image:
  repository: docker.io/apache/kafka
  tag: "${KAFKA_IMAGE_TAG}"
  pullPolicy: IfNotPresent

singleBroker:
  persistence:
    enabled: false
  resources:
    requests:
      cpu: 250m
      memory: 512Mi
    limits:
      cpu: "1"
      memory: 1Gi

config:
  autoCreateTopicsEnabled: false
  numPartitions: 1
  logRetentionHours: 24

  singleBroker: |
    # HelmForge already generates the complete KRaft topology for single-broker:
    # node.id, process.roles, controller.quorum.bootstrap.servers,
    # listeners and advertised.listeners.
    # Only security-related settings are added here.
    listener.security.protocol.map=CLIENT:SSL,CONTROLLER:SSL
    inter.broker.listener.name=CLIENT

    # Broker/controller TLS
    ssl.keystore.type=PKCS12
    ssl.keystore.location=/opt/kafka/tls/broker.p12
    ssl.keystore.password=${PASSWORD}
    ssl.truststore.type=PKCS12
    ssl.truststore.location=/opt/kafka/tls/truststore.p12
    ssl.truststore.password=${PASSWORD}

    # Require mTLS on Kafka client and KRaft controller listeners.
    listener.name.client.ssl.client.auth=required
    listener.name.controller.ssl.client.auth=required

    # Kafka authorization
    authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
    allow.everyone.if.no.acl.found=false

    # The same broker certificate is used for broker/controller identity.
    # It has CN=kafka-broker and is therefore a Kafka super user.
    super.users=User:CN=kafka-broker;User:CN=strimzi-user-operator

extraVolumes:
  - name: kafka-tls
    secret:
      secretName: ${BROKER_CERT_SECRET}-pkcs12

extraVolumeMounts:
  - name: kafka-tls
    mountPath: /opt/kafka/tls
    readOnly: true
YAML

  # The chart's built-in readiness probe is not TLS-aware and invokes
  # kafka-broker-api-versions.sh directly against the mTLS listener.
  # Do not use Helm --wait here: patch the StatefulSet probe first.
  helm upgrade --install "$KAFKA_RELEASE" \
    helmforge/kafka \
    --version "$KAFKA_CHART_VERSION" \
    --namespace "$NAMESPACE" \
    --values "${TMP_ROOT}/kafka-values.yaml" \
    --timeout=15m

  patch_kafka_readiness_probe
}

install_user_operator() {
  log "Installing ONLY the Strimzi User Operator ${STRIMZI_VERSION}"

  local archive="${TMP_ROOT}/strimzi.tar.gz"
  local source="${TMP_ROOT}/strimzi"

  curl -fsSL \
    "https://github.com/strimzi/strimzi-kafka-operator/archive/refs/tags/${STRIMZI_VERSION}.tar.gz" \
    -o "$archive"

  mkdir -p "$source"
  tar -xzf "$archive" -C "$source"

  local root
  root="$(find "$source" -mindepth 1 -maxdepth 1 -type d -name "strimzi-kafka-operator-${STRIMZI_VERSION}" | head -n1)"
  [[ -n "$root" ]] || die "Unable to locate Strimzi source tree"

  local uo_dir="${root}/install/user-operator"
  [[ -d "$uo_dir" ]] || die "Missing ${uo_dir}"

  # Replace the namespace placeholder used by the official manifests.
  find "$uo_dir" -type f -name '*.yaml' -print0 |
    xargs -0 sed -i "s/namespace: myproject/namespace: ${NAMESPACE}/g"

  kubectl apply -n "$NAMESPACE" -f "$uo_dir"

  # The 1.2.0 deployment already has the required security context,
  # projected Kubernetes API token and probes. Only Kafka-specific
  # configuration is changed here.
  kubectl -n "$NAMESPACE" set env deployment/strimzi-user-operator \
    STRIMZI_KAFKA_BOOTSTRAP_SERVERS="${KAFKA_SERVICE}.${NAMESPACE}.svc.cluster.local:${KAFKA_CLIENT_PORT}" \
    STRIMZI_LABELS="strimzi.io/cluster=${KAFKA_RELEASE}" \
    STRIMZI_CA_CERT_NAME="${CLIENTS_CA_CERT_SECRET}" \
    STRIMZI_CA_KEY_NAME="${CLIENTS_CA_KEY_SECRET}" \
    STRIMZI_CLUSTER_CA_CERT_SECRET_NAME="${CLUSTER_CA_CERT_SECRET}" \
    STRIMZI_EO_KEY_SECRET_NAME="${UO_CERT_SECRET}" \
    STRIMZI_ACLS_ADMIN_API_SUPPORTED="true" \
    STRIMZI_FULL_RECONCILIATION_INTERVAL_MS="30000" \
    STRIMZI_LOG_LEVEL="INFO"

  wait_rollout strimzi-user-operator
}

create_topic_as_uo_superuser() {
  log "Creating ${KAFKA_TOPIC} through Kafka Admin API"

  cat <<YAML | kubectl apply -n "$NAMESPACE" -f -
apiVersion: v1
kind: Pod
metadata:
  name: poc-kafka-admin
spec:
  restartPolicy: Never
  containers:
    - name: kafka-admin
      image: docker.io/apache/kafka:${KAFKA_IMAGE_TAG}
      command:
        - /bin/bash
        - -ec
        - |
          PASSWORD="\$(cat /tls/entity-operator.password)"

          cat >/tmp/admin.properties <<EOF
          security.protocol=SSL
          ssl.keystore.type=PKCS12
          ssl.keystore.location=/tls/entity-operator.p12
          ssl.keystore.password=\${PASSWORD}
          ssl.truststore.type=PKCS12
          ssl.truststore.location=/tls/truststore.p12
          ssl.truststore.password=\${PASSWORD}
          ssl.endpoint.identification.algorithm=HTTPS
          EOF

          /opt/kafka/bin/kafka-topics.sh \
            --bootstrap-server ${KAFKA_SERVICE}.${NAMESPACE}.svc.cluster.local:${KAFKA_CLIENT_PORT} \
            --command-config /tmp/admin.properties \
            --create --if-not-exists \
            --topic ${KAFKA_TOPIC} \
            --partitions 1 \
            --replication-factor 1
      volumeMounts:
        - name: uo
          mountPath: /tls
          readOnly: true
  volumes:
    - name: uo
      secret:
        secretName: ${UO_CERT_SECRET}
YAML

  kubectl -n "$NAMESPACE" wait \
    --for=jsonpath='{.status.phase}'=Succeeded \
    pod/poc-kafka-admin \
    --timeout=5m
}

create_kafka_user() {
  log "Creating KafkaUser ${KAFKA_USER}"

  kubectl apply -n "$NAMESPACE" -f - <<YAML
apiVersion: kafka.strimzi.io/v1
kind: KafkaUser
metadata:
  name: ${KAFKA_USER}
  labels:
    strimzi.io/cluster: ${KAFKA_RELEASE}
spec:
  authentication:
    type: tls-external
  authorization:
    type: simple
    acls:
      - resource:
          type: topic
          name: ${KAFKA_TOPIC}
          patternType: literal
        operations:
          - Read
          - Write
          - Describe
      - resource:
          type: group
          name: ${KAFKA_GROUP}
          patternType: literal
        operations:
          - Read
          - Describe
YAML

  kubectl -n "$NAMESPACE" wait \
    --for=condition=Ready \
    "kafkauser/${KAFKA_USER}" \
    --timeout=10m
}

verify_external_tls_user() {
  log "Verifying tls-external did not create a Strimzi credential Secret"

  if kubectl -n "$NAMESPACE" get secret "$KAFKA_USER" >/dev/null 2>&1; then
    die "Secret/${KAFKA_USER} exists; tls-external should not create user credentials."
  fi

  log "Secret/${KAFKA_USER} is absent as expected"
}

test_authorized_user() {
  log "Testing authenticated + authorized external user"

  kubectl delete pod -n "$NAMESPACE" poc-authorized-client \
    --ignore-not-found >/dev/null 2>&1 || true

  cat <<YAML | kubectl apply -n "$NAMESPACE" -f -
apiVersion: v1
kind: Pod
metadata:
  name: poc-authorized-client
spec:
  restartPolicy: Never
  containers:
    - name: kafka-client
      image: docker.io/apache/kafka:${KAFKA_IMAGE_TAG}
      command:
        - /bin/bash
        - -ec
        - |
          PASSWORD="\$(cat /tls/password)"

          cat >/tmp/client.properties <<EOF
          security.protocol=SSL
          ssl.keystore.type=PKCS12
          ssl.keystore.location=/tls/user.p12
          ssl.keystore.password=\${PASSWORD}
          ssl.truststore.type=PKCS12
          ssl.truststore.location=/tls/truststore.p12
          ssl.truststore.password=\${PASSWORD}
          ssl.endpoint.identification.algorithm=HTTPS
          EOF

          MESSAGE="authorized-${KAFKA_USER}"

          echo "\${MESSAGE}" |
            /opt/kafka/bin/kafka-console-producer.sh \
              --bootstrap-server ${KAFKA_SERVICE}.${NAMESPACE}.svc.cluster.local:${KAFKA_CLIENT_PORT} \
              --topic ${KAFKA_TOPIC} \
              --producer.config /tmp/client.properties

          /opt/kafka/bin/kafka-console-consumer.sh \
            --bootstrap-server ${KAFKA_SERVICE}.${NAMESPACE}.svc.cluster.local:${KAFKA_CLIENT_PORT} \
            --topic ${KAFKA_TOPIC} \
            --group ${KAFKA_GROUP} \
            --from-beginning \
            --consumer.config /tmp/client.properties \
            --max-messages 1 \
            --timeout-ms 20000 |
            grep -F "\${MESSAGE}"
      volumeMounts:
        - name: client
          mountPath: /tls
          readOnly: true
  volumes:
    - name: client
      secret:
        secretName: ${USER_CERT_SECRET}-client
YAML

  kubectl -n "$NAMESPACE" wait \
    --for=jsonpath='{.status.phase}'=Succeeded \
    pod/poc-authorized-client \
    --timeout=5m
}

show_state() {
  echo
  log "Kafka"
  kubectl -n "$NAMESPACE" get pod "$KAFKA_POD" -o wide

  echo
  log "User Operator"
  kubectl -n "$NAMESPACE" get deployment strimzi-user-operator

  echo
  log "KafkaUser"
  kubectl -n "$NAMESPACE" get kafkauser "$KAFKA_USER"

  echo
  log "Certificates"
  kubectl -n "$NAMESPACE" get certificates

  echo
  log "Services"
  kubectl -n "$NAMESPACE" get svc
}

main() {
  for cmd in kubectl helm curl tar openssl keytool base64 sed find awk grep; do
    need_cmd "$cmd"
  done

  require_cluster_access
  ensure_no_cluster_operator

  kubectl create namespace "$NAMESPACE" \
    --dry-run=client -o yaml |
    kubectl apply -f -

  install_cert_manager

  create_ca
  create_broker_certificate
  create_uo_certificate
  create_external_user_certificate

  create_broker_pkcs12_secret
  create_strimzi_ca_secrets
  create_uo_admin_secret
  create_external_user_client_secret

  install_kafka
  install_user_operator

  create_topic_as_uo_superuser

  create_kafka_user
  verify_external_tls_user
  test_authorized_user

  show_state

  log "SUCCESS: standalone Strimzi User Operator PoC completed"
}

main "$@"
