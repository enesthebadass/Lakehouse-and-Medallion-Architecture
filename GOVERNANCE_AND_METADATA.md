# Governance ve Metadata Mimarisi

## 1. Amaç

Bu doküman lakehouse platformunda metadata, governance, teknik catalog, lineage ve
erişim politikasının hangi bileşende tutulacağını tanımlar. Amaç tek bir araca bütün
sorumlulukları yüklemek değil, karar kaynağı ile runtime enforcement noktasını ayırmaktır.

## 2. Sorumluluk Sınırları

| Bileşen | Sorumluluk | Sorumlu Olmadığı Konu |
|---|---|---|
| Hive Metastore | Delta tablo location ve teknik şema çözümleme | Business glossary, ownership |
| OpenMetadata | Aranabilir catalog, domain, glossary, classification, lineage | Trino sorgusunu runtime'da engelleme |
| dbt | Gold model tanımı, test, description ve SQL lineage | Merkezi kullanıcı yetkilendirmesi |
| OpenLineage | Job/run/dataset lineage event standardı | Catalog UI veya access enforcement |
| Apache Ranger | Trino üzerinde table/column access, masking, row filter | Business glossary yönetimi |
| OPA | Kubernetes/API/platform policy-as-code | Varsayılan data catalog |
| Metaworks | Kurumun mevcut governance kabiliyetleri | Bu aşamada bilinmeyen kabiliyetler varsayılmaz |

OpenMetadata bir Hive Metastore alternatifi değildir. Ranger da OpenMetadata alternatifi
değildir. OpenMetadata politikaya girdi sağlayan context'i tutar; Ranger sorgu anında
kararı uygular.

## 3. Yerel Deployment Kararı

Yerel deployment `docker-compose.openmetadata.yml` overlay dosyasıyla isteğe bağlıdır.
OpenMetadata 1.13.0, kendi PostgreSQL metadata store'u ve Elasticsearch search index'i
ile çalışır. Ana Airflow ikinci kez kurulmaz; ingestion workflow'ları OpenMetadata CLI
image'ından harici olarak çalıştırılır.

Bu kararın nedenleri:

- Normal CDC demosunun başlangıç süresini ve RAM kullanımını artırmamak
- Aynı platformda iki ayrı Airflow instance'ı işletmemek
- Ingestion YAML'larını Git ile versiyonlamak
- Gelecekte aynı workflow'ları kurum Airflow/Kubernetes job'larına taşıyabilmek

Yerel profile basic authentication ve quickstart JWT anahtarları konmuştur. Bu sadece
localhost deneyi içindir; production kimlik doğrulama tasarımı değildir.

## 4. Metadata Ingestion Akışı

```text
Synthetic PostgreSQL comments/schema
              | source-postgres.yaml
              v
        OpenMetadata Catalog

Hive Metastore -> Trino schemas/tables
              | trino.yaml
              v
        OpenMetadata Catalog

dbt manifest + catalog + run_results
              | dbt.yaml
              v
descriptions + tests + model/source lineage
```

Çalıştırma sırası önemlidir:

1. PostgreSQL metadata ingestion
2. Trino database metadata ingestion
3. dbt artifact ingestion
4. Governance blueprint uygulaması

dbt lineage, hedef tablolar catalog'da bulunmadan çalıştırılırsa entity eşleştirmesi
eksik kalabilir. Bu nedenle database metadata her zaman önce yüklenir.

### Yerel Entegrasyon Kanıtı

2026-07-20 tarihinde OpenMetadata 1.13.0 local profile üzerinde şu sonuçlar doğrulandı:

- PostgreSQL source ingestion: 18 record, 0 error, `%100 Success`
- Trino ingestion: 26 record, 0 error, `%100 Success`
- dbt ingestion: 363 dbt record ve 603 OpenMetadata record, 0 error, `%100 Success`
- Catalog kapsamı: 13 source tablo, 22 Trino tablo, 7 dbt model, 145 test case
- Lineage: 25 table-to-table ilişki
- Governance: 4 classification, 13 tag, 3 domain, 2 data product, 1 glossary, 4 term

`customers.national_id` gibi kolonlarda PII/sensitivity tag'leri; Gold çıktılarında
domain ve data product ilişkileri OpenMetadata API üzerinden doğrulandı. Bootstrap art
arda iki kez çalıştırılarak nesne çoğaltmadığı gösterildi.

## 5. Domain ve Data Product Modeli

İlk domain taslağı:

| Domain | Kaynak | Business owner rolü | Steward rolü |
|---|---|---|---|
| Customer | `mms` | Customer Data Owner | Customer Data Steward |
| Lending | `krd` | Lending Data Owner | Lending Data Steward |
| Reference Data | `prm` | Enterprise Data Owner | Reference Data Steward |

İlk data product taslağı:

| Data product | Domain | Çıkışlar |
|---|---|---|
| Customer 360 | Customer | `dim_customer_current` |
| Loan Portfolio | Lending | `fct_loans_current`, `agg_customer_loan_portfolio` |

Owner hesabı karar yetkisini; steward hesabı tanım, kalite ve sınıflandırma bakımını;
Data Engineering ise teknik pipeline ve SLA sorumluluğunu temsil eder. Bu roller aynı
kişiye otomatik olarak verilmez.

## 6. Classification Taxonomy

`governance/catalog/governance-blueprint.yaml` dört ayrı classification tanımlar:

- `SourceDomain`: MMS, KRD ve PRM sentetik kaynak sınırları
- `DataSensitivity`: Public, Internal, Confidential, Restricted
- `PersonalData`: DirectIdentifier, SensitiveIdentifier, Contact, QuasiIdentifier
- `FinancialData`: MonetaryAmount, CreditExposure

Bir kolon birden fazla tag alabilir. Örneğin `national_id` hem
`PersonalData.SensitiveIdentifier` hem `DataSensitivity.Restricted` olur.

Tag yalnızca metadata'dır. Restricted tag'i tek başına sorguyu engellemez. Gün 16'da
Ranger politikası bu tag kararlarını column masking ve access policy'ye çevirecektir.

## 7. Business Glossary

İlk terimler:

- Customer
- Loan Application
- Active Loan
- Principal Exposure

Glossary terimi teknik kolon adından bağımsız, iş tarafından onaylanmış anlamdır.
Örneğin principal exposure farklı tablolarda farklı kolonlarla temsil edilebilir ama
tek kurumsal tanıma bağlanır. Değişiklikler steward önerisi ve owner onayıyla yapılmalıdır.

## 8. Lineage Stratejisi

### Bugün Çalışan Kısım

- Kafka topic/partition/offset ile Bronze object arasında audit bağı vardır.
- Raw Vault satırları source position ve Bronze lineage alanlarını taşır.
- dbt manifest'i Raw Vault source -> Gold model bağımlılıklarını verir.
- OpenMetadata dbt ingestion bu model/test lineage'ını catalog'a taşır.

Production-shaped PoC hedefinde lineage yalnız dataset-to-dataset okundan ibaret
olmayacaktır. Batch manifest URI, topic/partition low-high offset, schema contract
version, Airflow attempt, reconciliation evidence ve aktif Gold release kimliği aynı
run zincirine bağlanacaktır. Bu alanlar henüz runtime OpenLineage event'i olarak
uygulanmış değildir; `Planned` durumundadır.

### Airflow ve Spark Runtime Lineage Hedefi

Airflow REST metadata ingestion DAG ve task yapısını getirir; tek başına dataset lineage
üretmez. Runtime lineage için OpenLineage event'leri gerekir.

Hedef uygulama sırası:

1. Airflow image'ına uyumlu `apache-airflow-providers-openlineage` sürümünü sabitle.
2. Namespace'i OpenMetadata pipeline service adıyla aynı yap.
3. HTTP transport'u `/api/v1/openlineage/lineage` endpoint'ine bağla.
4. Bot JWT'yi environment yerine secret manager'dan al.
5. Spark agent'i `spark.extraListeners` ile etkinleştir.
6. S3A path'lerini OpenMetadata table FQN'lerine eşleyen dataset naming standardını test et.
7. `source -> Bronze -> Raw Vault -> Gold` grafiğini bir kontrollü run ile doğrula.

Mevcut SparkSubmit task'larında OpenLineage agent etkin değildir. Bu açıkça tasarım
durumudur; dbt lineage'ının varlığı Spark runtime lineage'ı varmış gibi sunulmaz.

## 9. Metaworks ile Birlikte Çalışma

Metaworks doğrudan kaldırılmaz. Önce aşağıdaki capability mapping kurum ekibiyle
doldurulur:

| Capability | OpenMetadata referansı | Metaworks durumu | System of record |
|---|---|---|---|
| Technical discovery | Trino/PostgreSQL connector | Doğrulanacak | Beklemede |
| Business glossary | Native glossary | Doğrulanacak | Beklemede |
| Ownership/stewardship | Team/user/domain | Doğrulanacak | Beklemede |
| Classification | Classification/tag | Doğrulanacak | Beklemede |
| Lineage | dbt/OpenLineage | Doğrulanacak | Beklemede |
| Approval workflow | Governance workflow | Doğrulanacak | Beklemede |
| Policy enforcement | Ranger'a context sağlar | Doğrulanacak | Ranger/Trino |
| Metadata API/export | REST API | Doğrulanacak | Beklemede |

Karar kuralları:

1. Aynı glossary, owner veya classification iki sistemde manuel yönetilmez.
2. Kurum Metaworks'ü zorunlu system of record seçerse OpenMetadata referans
   implementasyon ve metadata exchange kaynağı olarak kalır.
3. OpenMetadata seçilirse Metaworks tüketici veya geçiş sistemi olur.
4. Çift yönlü senkronizasyonda field-level ownership ve conflict policy olmadan
   entegrasyon açılmaz.

## 10. Production Öncesi Zorunlu Kontroller

- Kurum OIDC/SAML/LDAP entegrasyonu ve self-signup kapatma
- TLS ve internal service encryption
- External HA PostgreSQL ve Elasticsearch/OpenSearch
- Secret manager ve anahtar rotasyonu
- Metadata database backup/restore testi
- Search reindex ve upgrade runbook'u
- Network policy ve egress sınırları
- Resource request/limit ve capacity testi
- Audit log retention ve SIEM aktarımı
- Governance role separation ve approval workflow
- Metaworks system-of-record kararı

Local Compose bu maddelerin kanıtı değildir; yalnızca entegrasyon ve governance model
doğrulamasıdır.
