# Lakehouse Operasyon Runbook'u

## 1. Kapsam ve Durum

Bu runbook lokal PoC'nin izlenmesi, hata ayıklaması, kontrollü recovery'si ve üretim
öncesi eksiklerinin görünür tutulması içindir.

| Alan | Durum |
|---|---|
| Prometheus metric toplama | Implemented/PoC |
| Grafana dashboard provisioning | Implemented/PoC |
| Prometheus alert rules | Implemented/PoC |
| Alertmanager | Implemented/PoC, yalnız null receiver |
| Production notification route | Designed/External |
| Otomatik backup/restore | Open |
| Banka onaylı RPO/RTO | External |
| HA Prometheus/Grafana | External |

Lokal baseline tek node ve tek instance bileşenlerden oluşur. Production availability
ve disaster recovery kanıtı değildir.

## 2. Observability Stack'i Başlatma

Normal demo observability servislerini otomatik başlatmaz. Mevcut lakehouse çalışırken:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.observability.yml \
  --profile observability \
  up -d --build
```

Arayüzler:

| Arayüz | URL | Lokal kullanıcı |
|---|---|---|
| Grafana | <http://localhost:3000> | `admin` / `.env` içindeki parola |
| Prometheus | <http://localhost:9090> | authentication yok, yalnız lokal |
| Alertmanager | <http://localhost:9093> | authentication yok, yalnız lokal |
| Pipeline exporter | <http://localhost:9108/metrics> | authentication yok, yalnız lokal |

Grafana'da **Lakehouse > Lakehouse Pipeline Operations** dashboard'u otomatik gelir.
Production'da bu portlar public açılmaz; ingress, TLS, IAM ve network policy arkasında
çalışır.

## 3. Metric Contract

Makine tarafından doğrulanan contract `observability/metric-contract.yaml` içindedir.

| İhtiyaç | Metric | Lokal kaynak |
|---|---|---|
| Kafka lag | `lakehouse_kafka_consumer_lag_total` | committed offset ve high watermark |
| Connector status | `lakehouse_cdc_connector_up` | Kafka Connect REST |
| Airflow failure | `lakehouse_airflow_dag_latest_state` | Airflow metadata DB read-only poll |
| Spark duration | `lakehouse_spark_task_latest_duration_seconds` | SparkSubmitOperator task süresi |
| Trino query | `trino_execution_name_QueryManager_*` | Trino `/metrics` OpenMetrics |
| Reconciliation | `lakehouse_reconciliation_failures` | latest audit Delta batch |
| Gold quality | `lakehouse_gold_table_grain_valid` | fact grain kontrolü |
| Dashboard freshness | `lakehouse_gold_last_load_timestamp_seconds` | dbt load zamanı |

Lokal exporter Airflow metadata database'ini okur. Production'da Airflow'un resmi
StatsD veya OpenTelemetry metric yolu tercih edilmeli; metadata DB polling yalnız PoC
adapter'ıdır. Spark süresi Airflow operator overhead'ini içerir. Production Spark
History Server ve native Spark metric sink ayrıca kurulmalıdır.

## 4. Alert Öncelikleri

- `critical`: veri kaybı, pipeline durması, reconciliation veya Gold grain ihlali
  ihtimali. On-call ve data owner bilgilendirilir.
- `warning`: gecikme, yavaşlama, stale data veya telemetry kaybı. Mesai içinde
  inceleme başlatılır; SLO riski varsa critical'a yükseltilir.
- Lokal eşikler başlangıç değeridir. Production eşikleri peak rate, hacim testi ve
  business freshness gereksinimine göre onaylanır.

Alertmanager lokal ortamda bildirim göndermez. Production receiver; bankanın SIEM,
e-posta, ITSM veya on-call sistemiyle security onayından sonra yapılandırılır.

## 5. Incident Response

1. Alert'in başlangıç zamanını, affected service ve dataset'i kaydet.
2. Dashboard ile ilk etki alanını belirle: source, CDC, Bronze, Silver, Gold veya query.
3. Veri kaybını büyütebilecek otomatik aksiyonu durdur; raw log, Kafka offset ve
   Bronze objelerini silme.
4. Connector/task/query loglarını aynı zaman aralığında topla.
5. Son başarılı Kafka coordinate, reconciliation batch ve Airflow run ID'yi kaydet.
6. Recovery öncesinde replay penceresinin Kafka ve source log retention içinde
   kaldığını doğrula.
7. Düzeltmeden sonra source -> Bronze -> Raw Vault -> Gold reconciliation çalıştır.
8. Incident kaydına root cause, etki, recovery süresi ve kalıcı aksiyonu ekle.

### Metrics Collector Failure

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.observability.yml \
  logs --tail=200 pipeline-metrics-exporter
```

`lakehouse_observability_collector_up{collector="..."}` hangi bağımlılığın
başarısız olduğunu gösterir. Exporter hatası pipeline'ı durdurmaz; sadece görünürlüğü
azaltır. Önce ilgili endpoint/DB erişimini kontrol et, secret değerlerini loglama.

### CDC Connector or Kafka Lag

```bash
curl -fsS http://localhost:8083/connectors/core-banking-postgres-cdc/status

docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --group bronze-cdc-writer-v1 \
  --describe

docker compose logs --tail=200 debezium-connect bronze-cdc-writer
```

- Connector `FAILED` ise task trace, source DB erişimi ve replication slot kontrol edilir.
- Lag artarken connector sağlıklıysa Bronze writer hatası, MinIO erişimi ve throughput
  kontrol edilir.
- Offset reset, replication slot silme veya topic silme change approval olmadan yapılmaz.
- Replay gerekiyorsa `cdc/bronze/README.md` prosedürü izlenir.

### Airflow or Spark Failure

```bash
docker compose logs --tail=200 airflow-scheduler spark-master spark-worker
docker compose exec airflow-webserver airflow dags list-runs \
  --dag-id cdc_raw_vault_incremental --limit 5
```

Önce başarısız task belirlenir. Aynı run içinde yalnız idempotent olduğu kanıtlanmış
task yeniden çalıştırılır. `validate` veya `reconcile` başarısızsa downstream publish
devam ettirilmez. Spark OOM durumunda input hacmi, partition sayısı ve en büyük
transaction ölçülmeden yalnız memory artırılmaz.

### Trino Query Failure

```bash
docker compose logs --tail=200 trino
docker compose exec trino trino --execute \
  "SELECT state, count(*) FROM system.runtime.queries GROUP BY 1"
```

Catalog/metastore erişimi, S3 endpoint, authorization ve query memory limitleri
kontrol edilir. Production query history için Trino event listener ile kalıcı audit
store gerekir; coordinator memory history tek başına yeterli değildir.

### Reconciliation or Gold Quality Failure

```bash
docker compose exec trino trino --execute \
  "SELECT * FROM lakehouse.audit.cdc_raw_vault_reconciliation ORDER BY checked_at DESC LIMIT 30"

docker compose --profile tools run --rm dbt test
```

- Hangi source object ve check type'ın bozulduğu belirlenir.
- Bronze raw event değiştirilmez veya overwrite edilmez.
- Eksik event varsa source retention ve Kafka'dan kontrollü replay yapılır.
- Mapping hatasıysa versioned code fix ve yeni batch uygulanır.
- Reconciliation geçmeden Power BI refresh veya Gold publish onaylanmaz.

## 6. Backup ve Restore Matrisi

| Varlık | Lokal durum | Production gereksinimi | Restore sırası |
|---|---|---|---|
| Source Oracle/PostgreSQL | Demo volume | DBA backup/PITR, source sistem sorumluluğu | 1 |
| Kafka data/offsets | Tek volume, backup yok | Replication, backup/replay ve tested retention | 2 |
| Object storage Bronze/Delta | Tek MinIO volume | Versioning/object lock kararı, replicated backup | 3 |
| Hive Metastore DB | Tek PostgreSQL volume | Encrypted scheduled backup ve PITR | 4 |
| Airflow metadata DB | Tek PostgreSQL volume | Encrypted backup; DAG code Git'ten restore | 5 |
| Ranger/OpenMetadata DB | Optional local volume | Encrypted backup ve application-aware restore | 6 |
| Prometheus TSDB | 7 günlük lokal volume | HA veya remote-write retention kararı | 7 |
| Grafana | Provisioned files + volume | Git as source of truth, secrets external | 8 |
| Power BI artifact | Repo dışında | Report Server catalog backup ve versioning | 9 |

Restore testi backup alınmış sayılmanın parçasıdır. Sadece volume bulunduğunu görmek
backup kanıtı değildir. Production restore drill; checksum, row count, Delta log,
Kafka coordinate ve dashboard total kontrollerini içermelidir.

## 7. RPO ve RTO

Henüz banka tarafından onaylanmış RPO/RTO yoktur. Lokal planlama varsayımları:

- ingestion RPO: Kafka'nın 7 günlük lokal retention penceresi içinde best effort replay;
- lokal platform RTO hedefi: manuel restore için 4 saat;
- dashboard freshness hedefi: manuel pipeline için 24 saat.

Bunlar SLA değildir. Production değerleri dataset criticality, mevzuat, source redo
retention, peak event rate, HA altyapısı, backup teknolojisi ve destek saatleriyle
belirlenmelidir. Her domain aynı RPO/RTO'ya zorlanmamalıdır.

## 8. Restore Sırası

1. Network, IAM, secret manager ve certificate bağımlılıklarını doğrula.
2. Source sistem ve log retention erişimini doğrula.
3. Kafka broker, internal topics, connector offsets ve schema history'yi restore et.
4. Object storage ve Delta `_delta_log` bütünlüğünü doğrula.
5. Hive Metastore'u restore et ve table location'ları doğrula.
6. Airflow metadata'yı restore et; DAG kodunu Git'ten deploy et.
7. Trino catalog ve authorization'ı aç; read-only smoke query çalıştır.
8. Reconciliation ve dbt testleri geçtikten sonra Gold publish et.
9. Power BI refresh'i en son aç ve KPI total'lerini backend sorgularıyla karşılaştır.

## 9. End-to-End Test

Mevcut datayı değiştirmeyen kontrol:

```bash
./operations/end_to_end_smoke_test.sh --read-only
```

Yeni source transaction batch'i üreten, incremental DAG'ı, dbt'yi ve Power BI export'u
çalıştıran tam test:

```bash
./operations/end_to_end_smoke_test.sh --exercise-change
```

Sonuç `tests/results/day19-e2e.md` dosyasına yazılır. Test şu zinciri doğrular:

```text
source -> Debezium -> Kafka -> immutable Bronze -> CDC Raw Vault
       -> Trino/dbt Gold -> Power BI CSV contract
```

Bu test Power BI Report Server'daki gerçek `.pbix`, ODBC driver veya scheduled refresh'i
doğrulamaz; o adımlar şirket ortamında external kabul testidir.

## 10. Açık Production Eksikleri

- HA Kafka, object storage, metastore, Airflow, Trino ve monitoring deployment'ı
- Merkezi log toplama, trace correlation ve SIEM entegrasyonu
- Airflow StatsD/OpenTelemetry ve native Spark metric sink
- Debezium/Kafka JMX exporter ve stable custom metric tags
- Trino event listener ile kalıcı query audit history
- Alertmanager production receiver, escalation ve on-call ownership
- Encrypted backup, immutable backup, restore drill ve DR environment
- Banka onaylı dataset-level SLO, RPO, RTO ve capacity thresholds
