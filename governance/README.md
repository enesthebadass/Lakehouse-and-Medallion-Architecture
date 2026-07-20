# OpenMetadata Local Governance Runbook

Bu dizin OpenMetadata deployment, ingestion ve governance blueprint dosyalarını
içerir. OpenMetadata normal lakehouse başlangıcına dahil değildir; kaynak tüketimini
kontrol etmek için ayrı Compose overlay ve `governance` profili kullanılır.

## Bileşenler

- `docker-compose.openmetadata.yml`: OpenMetadata 1.13.0, PostgreSQL ve Elasticsearch
- `ingestion/source-postgres.yaml`: sentetik `mms`, `krd`, `prm` kaynak metadata'sı
- `ingestion/trino.yaml`: `cdc_raw_vault` ve `gold_dbt` teknik metadata'sı
- `ingestion/dbt.yaml`: dbt descriptions, tests, tags ve model lineage
- `catalog/governance-blueprint.yaml`: domain, data product, glossary ve classification taslağı
- `catalog/bootstrap_catalog.py`: blueprint'i API'ye idempotent uygulayan araç

OpenMetadata burada Hive Metastore'un yerini almaz. Hive Metastore Trino'nun Delta
tablolarını çözmesini sağlar; OpenMetadata insanların aradığı governance catalog'udur.

## Ön Koşullar

- Ana lakehouse servisleri çalışıyor olmalıdır.
- Docker Desktop'a OpenMetadata için ek CPU ve RAM ayrılmalıdır.
- `lakehouse.gold_dbt` modelleri üretilmiş olmalıdır.
- Yerel profil basic authentication ve varsayılan quickstart JWT anahtarlarını kullanır;
  yalnızca geliştirici bilgisayarı içindir.

## 1. dbt Artifact'lerini Üret

```bash
docker compose run --rm dbt run
docker compose run --rm dbt test
docker compose run --rm dbt docs generate
```

Son komut `dbt/target/manifest.json`, `catalog.json` ve `run_results.json` dosyalarını
hazırlar. Database metadata ingestion dbt ingestion'dan önce çalışmalıdır.

## 2. OpenMetadata'yı Başlat

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.openmetadata.yml \
  --profile governance \
  up -d \
  openmetadata-postgresql \
  openmetadata-elasticsearch \
  openmetadata-migrate \
  openmetadata-server
```

Durumu kontrol et:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.openmetadata.yml \
  --profile governance \
  ps

curl -fsS http://localhost:8586/healthcheck
```

Arayüz: <http://localhost:8585>

İlk local giriş:

- Kullanıcı: `admin@open-metadata.org`
- Parola: `admin`

Giriş yaptıktan sonra `Settings > Bots > ingestion-bot` ekranından JWT token üret veya
mevcut token'ı kopyala. Token'ı YAML dosyasına yazma ve Git'e ekleme:

```bash
export OPENMETADATA_JWT_TOKEN='<ingestion-bot-jwt>'
```

Production'da varsayılan admin hesabı ve quickstart JWT anahtarları kaldırılmalıdır.

## 3. Metadata Ingestion Çalıştır

Önce operasyonel PostgreSQL ve Trino metadata'sını, sonra dbt artifact'lerini yükle:

```bash
COMPOSE='docker compose -f docker-compose.yml -f docker-compose.openmetadata.yml'

$COMPOSE --profile governance --profile governance-tools run --rm \
  openmetadata-ingestion ingest \
  -c /opt/openmetadata/ingestion/source-postgres.yaml

$COMPOSE --profile governance --profile governance-tools run --rm \
  openmetadata-ingestion ingest \
  -c /opt/openmetadata/ingestion/trino.yaml

$COMPOSE --profile governance --profile governance-tools run --rm \
  openmetadata-ingestion ingest \
  -c /opt/openmetadata/ingestion/dbt.yaml
```

Komutların tekrar çalıştırılması aynı service ve entity'leri günceller; yeni kopyalar
oluşturmamalıdır. Her run sonunda `Status: Success` ve sıfır failure beklenir.

PostgreSQL ingestion sırasında `pg_stat_statements` extension'ı yoksa query usage için
uyarı görülebilir. Bu uyarı schema, table, column ve comment ingestion'ını etkilemez.

## 4. Governance Blueprint'i Uygula

Metadata ingestion tamamlandıktan sonra version-controlled blueprint'i uygula:

```bash
$COMPOSE --profile governance --profile governance-tools run --rm \
  openmetadata-governance-bootstrap
```

Araç classification/tag, domain, glossary/term, data product ve asset ilişkilerini
doğru bağımlılık sırasıyla oluşturur. Tekrar çalıştırılabilir; mevcut nesneleri
çoğaltmaz ve kolonlara yalnızca eksik tag'leri ekler.

Blueprint'teki owner/steward adları kurumsal rol taslağıdır. Kurum IAM grupları belli
olmadan kişisel kullanıcı veya sahte e-posta oluşturulmaz; atamalar daha sonra onaylı
IAM provisioning entegrasyonuyla tamamlanır.

Blueprint bütünlük testi:

```bash
.venv/bin/python tests/test_governance_blueprint.py
```

## 5. Arayüzde Doğrula

1. `synthetic_core_banking` service altında `mms`, `krd`, `prm` şemalarını aç.
2. Kaynak SQL comment'lerinin tablo ve kolon açıklamalarına geldiğini kontrol et.
3. `lakehouse_trino` altında `cdc_raw_vault` ve `gold_dbt` şemalarını aç.
4. `dim_customer_current` üzerinde dbt model, description ve test sekmelerini kontrol et.
5. `fct_loans_current` lineage ekranında Raw Vault kaynaklarını kontrol et.
6. `customers.national_id` kolonunda `SensitiveIdentifier` ve `Restricted` tag'lerini aç.
7. `Customer360` ve `LoanPortfolio` çıktılarının doğru domain'e bağlı olduğunu kontrol et.

## Doğrulanmış Yerel Sonuç

2026-07-20 yerel çalıştırmasında üç ingestion workflow'u da `%100 Success` ve sıfır
error ile tamamlandı. Catalog'da 13 PostgreSQL kaynak tablosu, 22 Trino tablosu, 7 dbt
modeli, 145 test case ve 25 tablo lineage ilişkisi görüldü. Governance bootstrap; 4
classification, 13 tag, 3 domain, 2 data product, 1 glossary ve 4 terim oluşturdu.
İkinci bootstrap çalıştırması aynı sonuçla tamamlanarak idempotency doğrulandı.

## Durdurma

Ana lakehouse servislerine dokunmadan governance servislerini durdur:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.openmetadata.yml \
  --profile governance \
  stop \
  openmetadata-server \
  openmetadata-elasticsearch \
  openmetadata-postgresql
```

Metadata PostgreSQL ve Elasticsearch named volume'larında korunur. `down -v` metadata'yı
kalıcı olarak siler ve normal durdurma prosedürü değildir.

## Production Sınırı

Local profile'daki varsayılan admin/JWT anahtarları, sabit database kullanıcısı, HTTP
ve tek-node Elasticsearch
production için kabul edilemez. Production deployment; kurum SSO/IAM, TLS, secret
manager, external HA PostgreSQL, HA Elasticsearch/OpenSearch, backup/restore,
monitoring, network policy ve kaynak limitleri tamamlandıktan sonra yapılmalıdır.
