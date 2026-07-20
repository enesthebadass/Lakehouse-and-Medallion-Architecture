# Deployment ve Policy Blueprint

## 1. Amaç ve Durum

Bu doküman lakehouse bileşenlerinin Kubernetes/OpenShift üzerinde nasıl ayrılacağını,
hangi ekibin hangi sorumluluğu taşıyacağını ve OPA policy-as-code kontrollerinin nerede
uygulanacağını tanımlar.

Mevcut durum:

| Kabiliyet | Durum |
|---|---|
| Docker Compose demo | Implemented/Local |
| OPA offline admission testleri | Implemented/PoC |
| Kubernetes manifest güvenlik baseline'ı | Implemented/Reference |
| Kubernetes veya OpenShift cluster rollout | Designed/External |
| Gatekeeper admission deployment | Designed/External |

Kubernetes/OpenShift seçimi platform mimari kuruluna bağlıdır. Bu repo ürün ve policy
sözleşmesini sağlar; kurumun destek modeli, operator catalog'u, storage/network ve DR
kabiliyeti görülmeden tek bir dağıtımı zorunlu ilan etmez.

## 2. Hedef Namespace Topolojisi

| Namespace | Bileşenler | Birincil sahip |
|---|---|---|
| `lakehouse-orchestration` | Airflow | Data Platform |
| `lakehouse-processing` | Spark ve dbt job'ları | Data Engineering |
| `lakehouse-streaming` | Kafka, Kafka Connect, Debezium | Streaming Platform |
| `lakehouse-serving` | Trino | Data Platform / Analytics Platform |
| `lakehouse-governance` | OpenMetadata ve Ranger | Data Governance / Security |
| `lakehouse-observability` | metrics, logs, alerting | SRE |

Namespace bir mutlak güvenlik sınırı değildir. RBAC, NetworkPolicy, workload identity,
Pod Security ve runtime policy birlikte uygulanır. Her namespace `restricted` Pod
Security standardı ve default-deny ingress/egress ile başlar.

## 3. Deployment Sorumlulukları

### Airflow

- Platform ekibi chart/operator, scheduler HA, database, ingress ve upgrade'i yönetir.
- Data Engineering DAG, connection contract, retry/SLA ve pipeline ownership taşır.
- DAG'ler image içine kopyalanmaz; onaylı GitSync/artifact promotion yolu kullanılır.
- Worker/job service account'ları göreve özgüdür; scheduler cluster-admin olmaz.

### Spark

- Spark Operator veya kurum enterprise Spark servisi platform kararıdır.
- Driver yalnız gerekli SparkApplication kaynaklarına namespace seviyesinde erişir.
- Executor'lar object storage'a statik access key ile değil workload identity ile bağlanır.
- Job image'ları digest ile sabitlenir; driver/executor kaynak limitleri ayrı belirlenir.

### Kafka ve Debezium

- Tercih kurum managed Kafka'sı veya onaylı operatördür; tek pod broker production değildir.
- TLS, ACL, topic ownership, replication, rack/AZ dağılımı ve lag alert'i zorunludur.
- Debezium connector secret'ları Kafka Connect manifestine düz metin yazılmaz.
- Schema/event contract değişikliği source owner ve consumer onayı gerektirir.

### Trino

- Coordinator ve worker deployment'ları ayrılır; coordinator business workload taşımaz.
- TLS ve kurum authentication provider'ı zorunludur.
- Ranger plugin query-time authorization, masking, row filter ve access audit uygular.
- Resource groups, query limits ve autoscaling davranışı yük testiyle ayarlanır.

### OpenMetadata

- Uygulama stateless replica olarak; PostgreSQL ve search dış HA servisler olarak çalışır.
- SSO, bot/service account, backup/restore ve search reindex runbook'u gerekir.
- Ingestion job'ları ayrı service account ve network policy kullanır.
- OpenMetadata classification tutar; Trino sorgusunu kendisi engellemez.

### Ranger

- Security Platform Ranger Admin, database, LDAP sync, TLS ve audit store'u işletir.
- Data Security policy modelini; data owner erişim onayını yönetir.
- Trino plugin fail-closed ve Ranger kesinti senaryosu test edilmeden production açılmaz.
- Allowed/denied access audit SIEM'e aktarılır.

Makinece okunabilir tam sorumluluk listesi `platform/deployment-responsibilities.yaml`
dosyasındadır.

## 4. OPA'nın Üç Ayrı Rolü

### CI Policy Kontrolü

Pull request içindeki manifestler cluster'a ulaşmadan `opa test` ve policy evaluation
ile kontrol edilir. Bu hızlı geri bildirimdir fakat cluster admission yerine geçmez.

### Kubernetes Admission

Production hedefi OPA Gatekeeper'dır. Kubernetes API create/update isteğini Gatekeeper'a
gönderir; policy ihlali varsa resource oluşmadan reddedilir. Rollout sırası:

1. Existing resource audit
2. `dryrun`
3. `warn`
4. Seçilmiş non-production namespace'te `deny`
5. Exception kayıtları kapatıldıktan sonra production `deny`

Gatekeeper policy availability kritik olduğundan replica, disruption budget, audit,
monitoring ve webhook failure policy kurum standardıyla test edilir.

### API ve Servis Authorization

Bir uygulama iş kuralı için OPA'yı sidecar veya merkezi PDP olarak çağırabilir. Kararı
uygulayan API gateway/Envoy/uygulama PEP'tir. Bu repo şu anda API authorization
uygulamaz; yalnız deployment policy PoC'si içerir.

OPA, Trino veri erişiminde Ranger'ın yerine otomatik olarak geçmez. Ranger data-plane
enforcement; OPA platform/admission policy motorudur.

## 5. Uygulanan Admission Kuralları

`platform/policies/kubernetes/admission.rego` şu ihlalleri reddeder:

- Kurum registry'si dışındaki image
- SHA-256 digest ile sabitlenmemiş image veya `latest` kullanımı
- CPU/memory request veya limit eksikliği
- Root veya privileged container
- Privilege escalation, yazılabilir root filesystem veya düşürülmemiş capability
- `RuntimeDefault` seccomp eksikliği
- Default service account veya kontrolsüz token automount
- `hostPath` volume
- Parola/token/secret/key değerinin düz environment value olarak verilmesi
- Owner, environment veya application label eksikliği

On altı pozitif/negatif OPA unit testi gerçek OPA 1.17 motorunda çalışır. Kubernetes örnek
manifestleri ayrıca Python consistency testlerinden geçer.

## 6. IAM, LDAP ve RBAC

Kimliğin system of record'u enterprise IAM'dir. LDAP/AD grup isimleri Kubernetes
RoleBinding, Ranger role ve OpenMetadata team'lerine eşlenir. Bireysel kullanıcıya
doğrudan production RoleBinding verilmez.

Temel ilkeler:

- ClusterRoleBinding yerine mümkün olduğunda namespace RoleBinding
- Application ekiplerine cluster-admin verilmemesi
- İnsan ve service account kimliklerinin ayrılması
- Service account interaktif login'in kapalı olması
- Workload identity ve kısa ömürlü credential
- Joiner/mover/leaver otomasyonu ve periyodik erişim gözden geçirme
- Harici kontrollü break-glass hesabı ve SIEM alarmı

Eşleme taslağı `platform/iam-group-mapping.yaml` dosyasındadır. Gerçek AD grup isimleri
IAM ekibi onayıyla değiştirilir.

## 7. TLS ve Network Segmentation

- North-south trafik kurum ingress/API gateway üzerinden TLS ile gelir.
- Trino, OpenMetadata, Ranger, Airflow ve Kafka istemci bağlantıları TLS kullanır.
- Ranger plugin-to-admin ve kritik service-to-service bağlantılarda mTLS değerlendirilir.
- Default-deny NetworkPolicy üzerine yalnız açıkça gereken akışlar eklenir.
- Serving, governance, streaming ve operations namespace'leri ayrı network zone kabul edilir.
- Object storage, Oracle/Kafka ve kurumsal servis egress'i sabit CIDR/FQDN policy ile sınırlandırılır.
- CNI NetworkPolicy uygulamıyorsa manifest varlığı güvenlik kanıtı sayılmaz.

Örnek Trino ingress policy yalnız `platform.bank/trino-client=approved` namespace'lerinden
8443 portuna erişim verir. DNS, metastore, object storage, Ranger ve telemetry egress
kuralları gerçek kurum endpoint'leri belli olduğunda environment overlay'e eklenir.

## 8. Secret Yönetimi

Kubernetes Secret tek başına secret manager değildir. Production yaklaşımı:

1. Secret system of record kurum Vault/HSM/secret manager'ıdır.
2. CSI Secret Store veya onaylı External Secrets operatörüyle workload'a ulaştırılır.
3. etcd encryption-at-rest ve Secret RBAC least privilege açılır.
4. Git'te yalnız secret reference bulunur; değer veya base64 secret bulunmaz.
5. Rotation, revocation, expiry ve audit sahibi tanımlanır.
6. Mümkün olan yerde statik parola yerine workload identity kullanılır.

## 9. OpenShift Notları

OpenShift seçilirse aynı Kubernetes manifest sözleşmesi korunur; ayrıca:

- `restricted-v2` SCC uyumluluğu doğrulanır.
- Image'ların arbitrary non-root UID ile çalışabilmesi kontrol edilir.
- Route, internal registry, OperatorHub ve SecurityContextConstraints kurum standardına bağlanır.
- Custom SCC veya `anyuid` istisnası varsayılan çözüm yapılmaz.

## 10. Production Promotion Kapıları

1. Policy ve manifest statik testleri
2. Image build, SBOM, vulnerability ve signature doğrulaması
3. Non-production admission `dryrun/warn`
4. Namespace RBAC ve NetworkPolicy connectivity testleri
5. Secret rotation ve certificate renewal testi
6. HA, disruption, backup/restore ve failover testi
7. Security/platform/data owner onayı
8. GitOps promotion ve post-deployment smoke test

Bu adımların tamamlanması platform ve security ekiplerine bağlıdır. Offline OPA testinin
geçmesi tek başına production sertifikasyonu değildir.
