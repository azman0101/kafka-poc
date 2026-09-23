# PoC : Strimzi User Operator en mode Standalone sur Apache Kafka (KRaft)

Ce document décrit l'architecture, le fonctionnement détaillé, les mécanismes d'authentification mTLS/ACL et les correctifs techniques du script [`install-standalone-user-operator-poc-v4.sh`](./install-standalone-user-operator-poc-v4.sh).

---

## 1. Objectif du PoC

Démontrer que le **Strimzi User Operator** peut fonctionner de manière **100 % autonome** pour gérer les ressources Kubernetes `KafkaUser` et provisionner les ACLs Kafka sans dépendre du **Strimzi Cluster Operator**, sur un cluster **Apache Kafka 4.3.1 KRaft** standard déployé via le chart Helm tiers [HelmForge](https://repo.helmforge.dev).

### Cas d'usage cible :
- Authentification client en **mTLS externe** (`type: tls-external`) : les certificats clients sont émis par une PKI externe (ici gérée via [cert-manager](https://cert-manager.io/)).
- Autorisation gérée de façon déclarative via la CRD `KafkaUser` (type `simple` mappé sur le `StandardAuthorizer` de Kafka).
- Aucun secret de clé privée n'est généré par Strimzi pour l'utilisateur (`tls-external` préserve la séparation des responsabilités).

---

## 2. Architecture Globale

```mermaid
flowchart TD
    subgraph K8S["Kubernetes Namespace : kafka-security"]
        subgraph PKI["PKI & Cert-Manager"]
            CM["cert-manager"] --> CA["Kafka Root CA (Issuer: kafka-ca)"]
            CA --> CERT_BROKER["Cert Broker (kafka-broker-tls)"]
            CA --> CERT_UO["Cert User Operator (strimzi-user-operator-certs)"]
            CA --> CERT_USER["Cert Client Externe (catalog-product-updated...)"]
            CA --> CERT_UNAUTH["Cert Client Non Autorisé (unauthorized.cart...)"]
        end

        subgraph KAFKA_CLUSTER["Cluster Apache Kafka 4.3.1 (KRaft)"]
            SVC["Service ClusterIP : kafka\n(publishNotReadyAddresses: true)"]
            KAFKA_POD["Pod : kafka-0\n(Broker + Controller KRaft)\nAuth: StandardAuthorizer"]
            SVC --> KAFKA_POD
        end

        subgraph STRIMZI["Strimzi Standalone"]
            UO["Deployment : strimzi-user-operator (1.2.0)\n(Watcher CRD KafkaUser)"]
        end

        subgraph CLIENTS["Validation Pods"]
            ADMIN_POD["Pod : poc-kafka-admin\n(Création Topic via Superuser)"]
            UNAUTH_POD["Pod : poc-unauthorized-client\n(Test Négatif : Rejet ACL)"]
            CLIENT_POD["Pod : poc-authorized-client\n(Test Positif : Pub/Sub mTLS)"]
        end

        UO -- "Gère les ACLs (Admin API TLS)" --> SVC
        ADMIN_POD -- "Superuser Admin API" --> SVC
        UNAUTH_POD -- "Rejeté par ACLs (TopicAuthorizationException)" --> SVC
        CLIENT_POD -- "Pub/Sub mTLS autorisé par ACLs" --> SVC
        UO -- "Watch & Status" --> KU["CRD KafkaUser\n(catalog-product-updated...)"]
    end
```

---

## 3. Composants et Spécifications

| Composant | Version | Rôle |
| :--- | :--- | :--- |
| **Kubernetes local** | v1.35.8 (Docker Desktop / KinD) | Environnement d'exécution |
| **cert-manager** | v1.21.2 | Autorité de certification (CA racine) et émission des certificats X.509 |
| **Apache Kafka** | 4.3.1 (Image Docker officielle) | Moteur d'événements en mode KRaft (single-broker / combined broker+controller) |
| **HelmForge Chart** | 1.3.14 (`helmforge/kafka`) | Déploiement Helm standardisé du StatefulSet Kafka |
| **Strimzi User Operator** | 1.2.0 | Opérateur autonome réconciliant les `KafkaUser` avec les ACLs Kafka |

---

## 4. Déroulement du Script Étape par Étape

Le script [`install-standalone-user-operator-poc-v4.sh`](./install-standalone-user-operator-poc-v4.sh) orchestre les phases suivantes :

### Étape 1 : Pré-requis et validation
- Vérifie la présence des utilitaires CLI locaux : `kubectl`, `helm`, `curl`, `tar`, `openssl`, `keytool`, `base64`, `sed`, `find`, `awk`, `grep`.
- Valide l'accès au cluster Kubernetes courant.
- Vérifie qu'aucun Strimzi Cluster Operator n'est présent (garantie du mode standalone).

### Étape 2 : Installation de cert-manager
- Déploie le chart cert-manager `v1.21.2` dans le namespace `cert-manager` avec `--set crds.enabled=true`.
- Attend le déploiement complet des 3 composants : `cert-manager`, `webhook`, et `cainjector`.

### Étape 3 : Création de la PKI (Root CA & Certificats)
- Crée un Issuer auto-signé `poc-selfsigned`.
- Génère un certificat CA racine `kafka-root-ca` (durée 1 an, RSA 3072 bits).
- Configure un Issuer Kubernetes `kafka-ca` basé sur cette autorité racine.
- Émet 4 certificats signés par cette CA :
  1. `kafka-broker` : avec SANs DNS complets (`kafka`, `kafka-0`, services headless et FQDNs).
  2. `strimzi-user-operator` : certificat d'identité du client Admin de l'opérateur.
  3. `catalog-product-updated.cart.xxxxxx.io` : certificat du client applicatif autorisé.
  4. `unauthorized.cart.xxxxxx.io` : certificat client valide mais volontairement dépourvu de permissions ACL.

### Étape 4 : Préparation des Secrets TLS (PKCS#12, PKCS#8 et PEM)
- **Secret Broker (`kafka-broker-tls-pkcs12`)** :
  - Génère un Keystore PKCS#12 (`broker.p12`) et un Truststore Java (`truststore.p12`) contenant la CA.
- **Secrets CA Strimzi** :
  - `kafka-clients-ca-cert` (`ca.crt`) et `kafka-clients-ca` (`ca.key`).
  - `kafka-cluster-ca-cert` (`ca.crt`, `ca.p12`, mot de passe).
- **Secret Admin User Operator (`strimzi-user-operator-certs`)** :
  - Convertit la clé privée en **PKCS#8** (`entity-operator.key`).
  - Extrait le certificat PEM (`entity-operator.crt`).
  - Génère le PKCS#12 (`entity-operator.p12`) et le Truststore pour le pod de test admin.
- **Secrets Clients Applicatifs (`<user>-tls-client`)** :
  - Keystores PKCS#12 contenant la clé et le certificat pour le client autorisé et pour le client non autorisé.

### Étape 5 : Déploiement d'Apache Kafka avec HelmForge
- Configure le chart `helmforge/kafka` en mode `single-broker` (KRaft).
- Active les protocoles sécurisés :
  - `listener.security.protocol.map=CLIENT:SSL,CONTROLLER:SSL`
  - `listener.name.client.ssl.client.auth=required` (mTLS forcé)
  - `authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer`
  - `allow.everyone.if.no.acl.found=false` (accès refusé par défaut)
  - Superutilisateurs : `User:CN=kafka-broker;User:CN=strimzi-user-operator`
- Monte les volumes TLS dans `/opt/kafka/tls`.
- Déploie via `helm upgrade --install kafka helmforge/kafka`.

### Étape 6 : Patch de la sonde de disponibilité (Readiness Probe)
- Active `publishNotReadyAddresses: true` sur le service `kafka`.
- Remplace la sonde plaintext d'origine par un script appelant `kafka-broker-api-versions.sh` avec configuration SSL/mTLS.
- Supprime le pod initial `kafka-0` pour forcer sa recréation immédiate avec la nouvelle sonde.
- Attend que le StatefulSet soit déclaré `1/1 Ready`.

### Étape 7 : Déploiement du Strimzi User Operator (Standalone)
- Télécharge l'archive officielle Strimzi `1.2.0`.
- Extrait les manifests du répertoire `install/user-operator/` :
  - ServiceAccount, Role, RoleBinding
  - CRD `kafkausers.kafka.strimzi.io` (v1)
  - Deployment `strimzi-user-operator`
- Adapte le namespace (`kafka-security`) et injecte les variables d'environnement cibles :
  - `STRIMZI_KAFKA_BOOTSTRAP_SERVERS=kafka.kafka-security.svc.cluster.local:9092`
  - `STRIMZI_LABELS=strimzi.io/cluster=kafka`
  - `STRIMZI_EO_KEY_SECRET_NAME=strimzi-user-operator-certs`
  - `STRIMZI_CLUSTER_CA_CERT_SECRET_NAME=kafka-cluster-ca-cert`
  - `STRIMZI_ACLS_ADMIN_API_SUPPORTED=true`
- Attend que le pod de l'opérateur soit `1/1 Ready`.

### Étape 8 : Création du Topic
- Lance un pod éphémère `poc-kafka-admin` utilisant l'identité superuser du User Operator (`strimzi-user-operator`).
- Exécute `kafka-topics.sh` pour créer le topic `poc-user-operator-topic`.

### Étape 9 : Déclaration du KafkaUser et Réconciliation ACL
- Applique la ressource `KafkaUser` :
  ```yaml
  apiVersion: kafka.strimzi.io/v1
  kind: KafkaUser
  metadata:
    name: catalog-product-updated.cart.xxxxxx.io
    labels:
      strimzi.io/cluster: kafka
  spec:
    authentication:
      type: tls-external
    authorization:
      type: simple
      acls:
        - resource:
            type: topic
            name: poc-user-operator-topic
            patternType: literal
          operations: [Read, Write, Describe]
        - resource:
            type: group
            name: poc-user-operator-group
            patternType: literal
          operations: [Read, Describe]
  ```
- Attend que le User Operator réconcilie l'utilisateur (`status.conditions[?(@.type=="Ready")].status == "True"`).
- Vérifie qu'aucun secret de clé n'a été créé par Strimzi (comportement attendu de `tls-external`).

### Étape 10 : Test Négatif (Vérification du rejet d'un client non autorisé)
- Déploie le pod `poc-unauthorized-client` avec le certificat de `unauthorized.cart.xxxxxx.io` (client mTLS valide mais sans `KafkaUser` ni ACLs associées).
- Vérifie que la tentative de publication via `kafka-console-producer.sh` est **bloquée** avec l'exception :
  `org.apache.kafka.common.errors.TopicAuthorizationException: Not authorized to access topics: [poc-user-operator-topic]`.
- Vérifie que la tentative de consommation via `kafka-console-consumer.sh` est également **bloquée** avec `TopicAuthorizationException`.
- Le pod ne réussit (`Succeeded`) que si les deux opérations sont strictement rejetées par le `StandardAuthorizer` de Kafka.

### Étape 11 : Test Positif End-to-End (Validation du flux autorisé)
- Déploie le pod `poc-authorized-client` avec le certificat du client autorisé `catalog-product-updated.cart.xxxxxx.io`.
- Publie un message `authorized-catalog-product-updated.cart.xxxxxx.io` via `kafka-console-producer.sh`.
- Consomme le message via `kafka-console-consumer.sh` avec `--max-messages 1`.
- Valide la correspondance exacte du message et affiche l'état global des ressources.

---

## 5. Problèmes Résolus et Décisions d'Ingénierie

Lors des tests sur cluster local (Docker Desktop / Kubernetes), 6 blocages majeurs ont été diagnostiqués et corrigés dans le script :

### 5.1. Deadlock du Readiness Probe Kafka (`SSL handshake failed`)
* **Symptôme :** Le broker loggait toutes les 10s :  
  `Failed authentication with /127.0.0.1 (SSL handshake failed)` et le pod restait `0/1 Running`.
* **Causes :**
  1. Le chart HelmForge code en dur un probe plaintext (`localhost:9092`). Sur un listener configuré en `SSL:required`, la connexion est rejetée.
  2. Le StatefulSet Kubernetes n'applique pas automatiquement un nouveau template à un pod déjà unready.
  3. Le client `kafka-broker-api-versions.sh` interroge `localhost:9092`, reçoit l'adresse annoncée `kafka.kafka-security.svc.cluster.local:9092` (Service ClusterIP) et tente de s'y connecter. Par défaut, un Service Kubernetes n'a aucun endpoint tant que le pod n'est pas Ready (`DisconnectException`).
* **Correction :**
  - Application de `publishNotReadyAddresses: true` sur le Service `kafka`.
  - Patch du StatefulSet avec la configuration mTLS complète pour le probe.
  - Suppression automatique du pod `kafka-0` après le patch pour déclencher sa recréation immédiate sous la nouvelle révision.

### 5.2. Format des Clés et Certificats pour Strimzi UO 1.2.0 (`PemAuthIdentity`)
* **Symptôme :** Crash en boucle de l'opérateur :  
  `RuntimeException: The Secret ... is missing the field entity-operator.key`  
  Puis après ajout de la clé :  
  `InvalidKeyException: Unable to decode key ... algid parse error, not a sequence`.
* **Causes :**
  1. Strimzi 1.2.0 a migré son client Admin interne vers `PemAuthIdentity`, qui requiert impérativement les clés `entity-operator.key` et `entity-operator.crt` (et non plus un Keystore PKCS#12).
  2. cert-manager exporte par défaut les clés privées en **PKCS#1** (`BEGIN RSA PRIVATE KEY`), tandis que la JVM de Strimzi attend du **PKCS#8** (`BEGIN PRIVATE KEY`).
* **Correction :**
  - Conversion de la clé via `openssl pkcs8 -topk8 -nocrypt`.
  - Injection conjointe des formats PEM PKCS#8 (`entity-operator.key`, `entity-operator.crt`) et PKCS#12 dans le Secret `strimzi-user-operator-certs`.

### 5.3. Sortie propre du Consumer de test
* **Symptôme :** Log trompeur en fin de test :  
  `ERROR Error processing message, terminating consumer process: org.apache.kafka.common.errors.TimeoutException`.
* **Cause :** `kafka-console-consumer.sh` avec `--timeout-ms 20000` sans limite de messages attend 20 secondes d'inactivité avant de lever une exception pour terminer le processus.
* **Correction :** Ajout de l'option `--max-messages 1` pour forcer une sortie propre (code 0) dès la réception du premier message validé.

### 5.4. Isolation de la mise à jour des dépôts Helm
* **Symptôme :** Échec immédiat de `install_kafka` sur `helm repo update` en cas de dépôt tiers invalide sur le poste hôte (ex: bucket GCS avec token OAuth expiré).
* **Correction :** Remplacement de `helm repo update` par `helm repo update helmforge >/dev/null`.

### 5.5. Conteneur résiduel Docker Desktop (`kind-registry-mirror`)
* **Symptôme :** Docker Desktop n'arrivait pas à démarrer ou voir le cluster Kubernetes local (`kubernetes failed to start`).
* **Cause :** Un ancien conteneur `kind-registry-mirror` datant d'une version précédente n'avait pas de healthcheck configuré. Docker Desktop 4.92+ exécute `docker inspect -f '{{.State.Health.Status}}'` qui renvoyait une erreur et bloquait le cluster.
* **Correction :** Suppression du conteneur orphelin (`docker rm -f kind-registry-mirror`), automatiquement recréé avec l'image `v0.0.4` disposant d'un healthcheck conforme.

### 5.6. Idempotence et persistance des topics (`poc-kafka-admin` orphelin)
* **Symptôme :** Lors d'une ré-exécution du script sans suppression préalable du namespace, les pods de test échouaient avec :  
  `UnknownTopicOrPartitionException: This server does not host this topic-partition`  
  `TimeoutException: Topic poc-user-operator-topic not present in metadata after 60000 ms`.
* **Cause :** Dans la configuration PoC par défaut, le broker Kafka utilise un stockage temporaire (`emptyDir`). Lors d'un redémarrage ou d'une recréation du broker `kafka-0`, les topics précédents disparaissent. Or, la fonction `create_topic_as_uo_superuser` exécutait `kubectl apply` sur le pod `poc-kafka-admin` sans le supprimer au préalable. Comme un pod Kubernetes est immuable et qu'il était déjà à l'état `Completed`, Kubernetes ne le ré-exécutait pas et `kubectl wait` passait immédiatement sans recréer le topic.
* **Correction :** Ajout de `kubectl delete pod -n "$NAMESPACE" poc-kafka-admin --ignore-not-found` avant son déploiement pour forcer sa ré-exécution à chaque lancement du script.

---

## 6. Rôle d'OpenSSL et Articulation avec cert-manager

L'usage d'**OpenSSL** dans ce projet constitue la passerelle d'interopérabilité essentielle entre la **PKI native Kubernetes (cert-manager)** et le **runtime Java d'Apache Kafka et Strimzi**.

### Architecture des transformations cryptographiques

```mermaid
flowchart TD
    subgraph CERT_MANAGER["cert-manager (PKI Kubernetes)"]
        CA["Kafka Root CA (Issuer: kafka-ca)"]
        CERT["Secret standard : kubernetes.io/tls\n- tls.crt (Certificat public PEM)\n- tls.key (Clé privée RSA PKCS#1)"]
        CA --> CERT
    end

    subgraph OPENSSL["Opérations OpenSSL (Passerelle)"]
        OP1["1. openssl pkcs12 -export\nAssemblage Cert + Clé + CA\navec Alias & Mot de passe"]
        OP2["2. openssl pkcs8 -topk8 -nocrypt\nConversion de structure ASN.1 :\nPKCS#1 -> PKCS#8"]
    end

    subgraph DEST["Consommateurs (Kafka & Strimzi)"]
        KAFKA["Broker Kafka & Clients CLI\n(broker.p12, user.p12)"]
        UO["Strimzi User Operator 1.2.0\n(entity-operator.key PKCS#8)"]
    end

    CERT -- "tls.crt + tls.key + ca.crt" --> OP1 --> KAFKA
    CERT -- "tls.key (PKCS#1)" --> OP2 --> UO
```

### 1. `openssl pkcs12 -export` : Conversion PEM vers Keystores Java PKCS#12

* **Limitation de cert-manager :**  
  cert-manager génère des Secrets Kubernetes contenant des fichiers texte PEM plats (`tls.crt` et `tls.key`). Il ne produit pas de conteneur d'archive PKCS#12 unifié.
* **Besoin de Kafka :**  
  Le broker Apache Kafka et ses clients Java attendent un Keystore sécurisé par mot de passe contenant simultanément la clé privée, le certificat signé et la chaîne de certification sous un alias explicite (`kafka-broker`, `strimzi-user-operator`, etc.).
* **Rôle d'OpenSSL :**  
  La commande suivante fusionne ces éléments dans un fichier binaire `.p12` :
  ```bash
  openssl pkcs12 -export \
    -out "$output" \
    -inkey "$key" \
    -in "$crt" \
    -certfile "$ca" \
    -name "$alias" \
    -passout "file:${password_file}"
  ```

### 2. `openssl pkcs8 -topk8 -nocrypt` : Incompatibilité ASN.1 (PKCS#1 vs PKCS#8)

* **Spécificité de cert-manager :**  
  cert-manager émet par défaut les clés RSA sous la norme historique **PKCS#1** :
  ```text
  -----BEGIN RSA PRIVATE KEY-----
  MIIEowIBAAKCAQ...
  -----END RSA PRIVATE KEY-----
  ```
* **Exigence de Strimzi User Operator 1.2.0 :**  
  Strimzi 1.2.0 charge les certificats via sa classe `PemAuthIdentity`, qui décode la clé privée au moyen de la classe Java standard `PKCS8EncodedKeySpec`. La JVM attend rigoureusement le standard universel **PKCS#8** (`AlgorithmIdentifier` inclus) :
  ```text
  -----BEGIN PRIVATE KEY-----
  MIIEvQIBADANBgk...
  -----END PRIVATE KEY-----
  ```
  L'injection directe d'une clé PKCS#1 générée par cert-manager provoque un crash immédiat de la JVM :  
  `java.security.InvalidKeyException: Unable to decode key: java.io.IOException: algid parse error, not a sequence`.
* **Rôle d'OpenSSL :**  
  OpenSSL réécrit l'enveloppe ASN.1 sans modifier les composantes cryptographiques de la clé :
  ```bash
  openssl pkcs8 -topk8 -nocrypt -in "$d/uo.key" -out "$d/entity-operator.key"
  ```

### 3. Pourquoi `keytool` pour le Truststore (et non OpenSSL) ?

Pour le magasin d'ancres de confiance (`truststore.p12`), la commande `openssl pkcs12 -export -nokeys` génère un *Certificate Bag* standard qui n'est pas reconnu par le provider JSSE de Java comme une collection de `TrustedCertificateEntry`. Cela provoquait à l'exécution :  
`InvalidAlgorithmParameterException: the trustAnchors parameter must be non-empty`.

Le script utilise donc `keytool -importcert -storetype PKCS12` pour garantir une compatibilité native et irréprochable avec le runtime Java.

---

## 7. Guide d'Exécution

### Pré-requis
- Docker Desktop en cours d'exécution avec Kubernetes activé (ou cluster KinD local).
- Outils CLI installés sur la machine hôte : `kubectl`, `helm`, `openssl`, `keytool`.

### Lancement du PoC

```bash
chmod +x install-standalone-user-operator-poc-v4.sh
./install-standalone-user-operator-poc-v4.sh
```

### Paramètres configurables (Variables d'environnement)

Le script respecte les variables d'environnement suivantes si elles sont définies avant l'exécution :

```bash
# Exemple de personnalisation :
NAMESPACE="mon-namespace" \
KAFKA_USER="mon-service.mondomaine.io" \
KAFKA_TOPIC="mon-topic-securise" \
./install-standalone-user-operator-poc-v4.sh
```

| Variable | Défaut | Description |
| :--- | :--- | :--- |
| `NAMESPACE` | `kafka-security` | Namespace Kubernetes cible |
| `STRIMZI_VERSION` | `1.2.0` | Version de Strimzi User Operator |
| `KAFKA_CHART_VERSION` | `1.3.14` | Version du chart HelmForge Kafka |
| `KAFKA_IMAGE_TAG` | `4.3.1` | Version de l'image Apache Kafka |
| `CERT_MANAGER_VERSION` | `v1.21.2` | Version du chart cert-manager |
| `KAFKA_USER` | `catalog-product-updated.cart.xxxxxx.io` | Nom d'utilisateur et CN du certificat mTLS |
| `KAFKA_TOPIC` | `poc-user-operator-topic` | Topic de validation |
| `KAFKA_GROUP` | `poc-user-operator-group` | Consumer group de validation |

---

## 8. Commandes de Diagnostic et Vérification

Vérifier l'état des pods :
```bash
kubectl -n kafka-security get pods -o wide
```

Vérifier la réconciliation de l'utilisateur KafkaUser :
```bash
kubectl -n kafka-security get kafkauser catalog-product-updated.cart.xxxxxx.io -o yaml
```

Consulter les logs du Strimzi User Operator :
```bash
kubectl -n kafka-security logs deployment/strimzi-user-operator -f
```

Consulter les logs du broker Kafka :
```bash
kubectl -n kafka-security logs kafka-0 -c kafka -f
```

Consulter les logs du test négatif (rejet de l'utilisateur non autorisé) :
```bash
kubectl -n kafka-security logs pod/poc-unauthorized-client
```

Consulter les logs du test positif (succès de l'utilisateur autorisé) :
```bash
kubectl -n kafka-security logs pod/poc-authorized-client
```

Vérifier la validité des certificats cert-manager :
```bash
kubectl -n kafka-security get certificates
```

### Nettoyage de l'environnement

Pour désinstaller entièrement les composants créés par le PoC :

```bash
kubectl delete namespace kafka-security
kubectl delete crd kafkausers.kafka.strimzi.io
```
