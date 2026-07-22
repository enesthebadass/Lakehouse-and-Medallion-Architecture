# Oracle CDC Üretim Hazırlık Rehberi

## 1. Karar Özeti

Bu proje için önerilen Oracle CDC yolu:

```text
Oracle redo/archive logs
        |
        v
Oracle LogMiner
        |
        v
Debezium Oracle Connector + Kafka Connect
        |
        v
Kafka table topics
        |
        v
Immutable Bronze -> CDC Raw Vault -> dbt Gold
```

Seçilen yaklaşım **Debezium Oracle Connector + LogMiner**'dır. Debezium açık kaynaklı
ve Apache License 2.0 lisanslıdır. LogMiner Oracle Database'in bir parçasıdır. Oracle'ın
2026 Database Licensing Information dokümanı LogMiner'ı tüm Database offering'leri
için kullanılabilir gösterir. Ancak aynı doküman, **GoldenGate Supplemental Logging**
açıkken GoldenGate lisansı gerektiğini ayrıca belirtir.

Bu nedenle:

- XStream seçilmedi; XStream Oracle GoldenGate'in ticari bir bileşenidir.
- GoldenGate bu blueprint'in zorunlu bir parçası değildir.
- `database.connection.adapter=logminer` kullanılacaktır.
- DBA, satın alma/lisans yönetimi ve hukuk ekipleri bankanın gerçek Oracle sözleşmesini
  kontrol etmeden production kurulumu yapılmayacaktır.
- Bu belge lisans veya hukuk görüşü değildir.

Resmi referanslar:

- [Oracle Database Licensing Information](https://docs.oracle.com/cd/G47991_01/dblic/database-licensing-information-user-manual.pdf)
- [Debezium Oracle Connector](https://debezium.io/documentation/reference/stable/connectors/oracle.html)
- [Debezium Apache 2.0 License](https://github.com/debezium/debezium)

## 2. Şu Anda Pipeline Ne Yapıyor?

Lokal çalışan yol şöyledir:

```text
Sentetik workload -> PostgreSQL mms/krd/prm -> PostgreSQL WAL
 -> Debezium PostgreSQL -> Kafka -> MinIO Bronze
 -> Spark CDC Raw Vault -> Trino/dbt Gold
```

Bu yol bize şu ortak CDC davranışlarını kanıtlar:

- `insert`, `update` ve `delete` event'lerini yakalama;
- Debezium `before`, `after`, `op`, `source` ve `transaction` envelope'u;
- Kafka topic, partition ve offset ile ordering/audit;
- immutable ve tekrar oynatılabilir Bronze;
- idempotent Hub, Link ve Satellite yükleme;
- delete işlemini fiziksel silmek yerine tarihçe olarak koruma.

Ancak PostgreSQL pilotu aşağıdakileri **kanıtlamaz**:

- Oracle redo/archive log davranışı ve LogMiner performansı;
- Oracle RAC, Data Guard, CDB/PDB ve failover davranışı;
- Oracle NUMBER, DATE, TIMESTAMP, LOB, XML veya özel tip dönüşümleri;
- bankanın gerçek `MMS`, `KRD`, `PRM` tablo ve kolonları;
- gereken Oracle grant'lerinin güvenlik ekiplerince kabul edileceği;
- Oracle lisans sözleşmesinin production kullanımına izin verdiği.

Dolayısıyla bugün Oracle kurmak yerine aynı downstream contract'ı koruyacak bir
**Oracle source adapter blueprint'i** hazırlıyoruz. Gerçek Oracle bağlantısı banka
ortamındaki non-production Oracle üzerinde ayrıca doğrulanacaktır.

## 3. Ücret ve Lisans Ayrımı

| Parça | Bu projedeki karar | Lisans notu |
|---|---|---|
| Debezium | Kullan | Apache License 2.0 |
| Apache Kafka / Kafka Connect | Kullan | Açık kaynak |
| Oracle LogMiner | Kullanım adayı | Oracle Database özelliği; gerçek sözleşme doğrulanmalı |
| Oracle XStream | Kullanma | GoldenGate ticari bileşeni |
| Oracle GoldenGate | Zorunlu değil | Ticari ürün, ayrıca onay gerekir |
| OpenLogReplicator | Şimdilik kullanma | Açık kaynak, fakat Debezium adapter'ı incubating ve redo dosyalarına doğrudan erişim ister |
| Oracle JDBC driver | Gerekli | Debezium dağıtımına lisans nedeniyle gömülmez; kurumun onaylı artifact deposundan sağlanmalı |

Önemli ayrım: normal/table-level supplemental logging, CDC'nin doğru `before/after`
bilgisi için gereken bir database ayarıdır. Oracle dokümanında özel olarak adı geçen
**GoldenGate Supplemental Logging** ile aynı şey olduğu varsayılmamalıdır. DBA ve
lisans ekibi etkin ayarları birlikte teyit etmelidir.

## 4. PostgreSQL ve Oracle Event Contract Eşlemesi

Debezium iki connector'da da temel envelope'u korur. Adapter'a özgü alanlar aşağıdaki
şekilde ele alınır:

| Anlam | PostgreSQL | Oracle | Canonical kullanım |
|---|---|---|---|
| Önceki satır | `value.before` | `value.before` | Raw olarak koru |
| Yeni satır | `value.after` | `value.after` | Raw olarak koru |
| Operasyon | `r/c/u/d` | `r/c/u/d` | Aynı semantik |
| Şema | `value.source.schema` | `value.source.schema` | Bronze metadata'da lowercase |
| Tablo | `value.source.table` | `value.source.table` | Bronze metadata'da lowercase |
| Kaynak zaman | `value.source.ts_ms` | `value.source.ts_ms` | UTC event timestamp |
| Log pozisyonu | `lsn` | `scn`, `commit_scn`, `rs_id`, `ssn` | Raw sakla, Kafka koordinatıyla birlikte kullan |
| Transaction | transaction metadata | transaction metadata | Raw sakla ve audit'e taşı |
| Snapshot | `source.snapshot` | `source.snapshot` | `op=r` ile birlikte sakla |

Silver staging, adapter'a özgü pozisyon alanlarını kaybetmeden ortak bir sıralama
sözleşmesi üretmelidir:

```text
canonical_source_position = {
  adapter,
  transaction_position,   # PostgreSQL LSN veya Oracle commit_scn/scn
  intra_transaction_order,# Oracle rs_id/ssn veya adapter eşdeğeri
  kafka_topic,
  kafka_partition,
  kafka_offset
}
```

Bu yapı tek bir sayıya zorlanmaz; Oracle transaction sırası ile Kafka teslim sırası
aynı kavram değildir. Raw alanlar audit için korunur, canonical alan downstream
deterministik sıralama ve adapter contract testleri için kullanılır.

Oracle identifier'ları çoğunlukla büyük harfle gelir. Bronze writer artık `MMS` ve
`mms` gibi şema adlarını canonical object path için `mms` biçimine getirir, ancak
orijinal Debezium envelope'u değiştirmez. Böylece Bronze gerçek kaynak kaydı olarak
kalır.

Oracle kolonlarının adları ve tipleri bilinmediği için `CUSTOMER_NO -> customer_no`
gibi bir dönüşüm varsayılmamıştır. Data dictionary geldikten sonra version-controlled
source-to-canonical mapping üretilecek ve Silver staging katmanında uygulanacaktır.
Bugünkü Raw Vault script'i gerçek Oracle event'lerine doğrudan bağlanmaya hazır olarak
sunulmamalıdır.

## 5. Connector Şablonu

Referans configuration:

[`cdc/connectors/core-banking-oracle-logminer.template.json`](cdc/connectors/core-banking-oracle-logminer.template.json)

Bu şablon doğrudan deploy edilemez. Özellikle aşağıdaki placeholder'lar çözülmelidir:

- Oracle host, service/CDB adı ve varsa PDB adı;
- Secret Manager üzerinden connector kullanıcı/parolası;
- gerçek `table.include.list`;
- Oracle sürümüne uygun JDBC driver;
- TLS/TCPS URL, truststore veya Oracle Wallet;
- LOB ve mining strategy kararı;
- Kafka replication factor, retention ve HA ayarları.

`MMS,KRD,PRM` yalnızca bilinen şema isimleridir. Bütün şemaları otomatik capture etmek
yasaktır; source owner tarafından onaylanmış tablo allowlist'i kullanılmalıdır.

## 6. Initial Snapshot'tan Streaming'e Geçiş

Önerilen ilk yük akışı:

1. Source owner tablo allowlist'ini ve business key'leri onaylar.
2. DBA Oracle topology, ARCHIVELOG, supplemental logging, UNDO ve archive retention
   durumunu doğrular.
3. Connector, Kafka ve schema-history topic'leri production replication ile açılır.
4. Connector başlangıç SCN'ini alır ve kısa süreli gerekli lock'ları kullanır.
5. `snapshot.mode=initial` ile mevcut satırlar `op=r` event'leri olarak Kafka'ya yazılır.
6. Source ve snapshot row count/business key reconciliation çalışır.
7. Connector kaydettiği SCN'den LogMiner streaming'e devam eder.
8. Snapshot sırasında gelen değişikliklerin kaybolmadığı ve duplicate'lerin downstream
   idempotency tarafından güvenle işlendiği test edilir.
9. Kafka -> Bronze -> Raw Vault reconciliation tamamlanmadan production kabulü verilmez.
10. Raw Vault batch'i için topic/partition high offset manifest'i dondurulur; aynı
    batch'in bütün task'ları bu manifest'i kullanır.
11. Reconciliation canlı source durumuyla değil, snapshot/stream boundary'sinin SCN ve
    Kafka offset kanıtlarıyla yapılır.

Snapshot süresi `UNDO_RETENTION` değerini aşarsa `ORA-01555` riski vardır. Archive log
retention yalnız normal gecikmeyi değil şu pencereyi karşılamalıdır:

```text
max(ölçülen snapshot süresi, beklenen connector kesintisi, recovery/replay penceresi)
+ operasyon güvenlik payı
```

Bu değer tahminle değil, non-production hacim testiyle belirlenmelidir.

## 7. Oracle DBA Checklist

### Topology

- [ ] Oracle version ve edition kaydedildi.
- [ ] CDB/PDB veya non-CDB yapısı belirlendi.
- [ ] RAC instance/redo thread yapısı belirlendi.
- [ ] Data Guard/standby ve planned failover davranışı belirlendi.
- [ ] Listener service name, DNS ve TCPS endpoint doğrulandı.

### Logging ve Retention

- [ ] Database `ARCHIVELOG` modunda.
- [ ] Archive destination kapasitesi ve alarmı mevcut.
- [ ] Minimum supplemental logging etkin ve kanıtlandı.
- [ ] Her allowlisted tablo için gereken supplemental logging onaylandı.
- [ ] GoldenGate Supplemental Logging yanlışlıkla etkin değil veya lisansı ayrıca onaylı.
- [ ] Redo log switch sıklığı ve peak redo hacmi ölçüldü.
- [ ] Retention, snapshot/outage/recovery penceresini karşılıyor.

### Connector Account

- [ ] Kişisel olmayan dedicated service account oluşturuldu.
- [ ] Grant'ler hedef Oracle sürümüne göre DBA ve security tarafından incelendi.
- [ ] `SELECT ANY TABLE` yerine mümkünse allowlisted tablo grant'leri kullanıldı.
- [ ] LogMiner ve gerekli `V_$` view erişimleri doğrulandı.
- [ ] Flashback/snapshot erişimi sadece gereken kapsamda verildi.
- [ ] Connector flush table ve tablespace davranışı onaylandı.
- [ ] Credential Secret Manager'da, rotation ve audit aktif.

### Capacity ve Reliability

- [ ] En büyük transaction ve en uzun açık transaction ölçüldü.
- [ ] Kafka Connect heap, Oracle PGA/SGA, CPU ve I/O load test edildi.
- [ ] Connector restart ve offset recovery test edildi.
- [ ] RAC/Data Guard failover senaryosu test edildi.
- [ ] Schema history ve offset topic backup/replication politikası var.

Debezium dokümanındaki örnek grant listesi production ortamında körlemesine
çalıştırılmamalıdır. Exact SQL; Oracle sürümü, CDB/PDB yapısı, capture kapsamı ve
kurumun least-privilege standardına göre DBA tarafından üretilmelidir.

## 8. Dış Bağımlılıklar ve Sorumlular

| Ekip | Beklenen çıktı |
|---|---|
| Oracle DBA | Topology, logging, retention, grant, capacity ve failover kanıtı |
| Source owner | Tablo allowlist, business key, delete ve DDL semantiği |
| Security/IAM | Service account, Secret Manager, rotation ve audit onayı |
| Network | Kafka Connect -> Oracle TCPS route, DNS ve firewall |
| Lisans yönetimi | Oracle sözleşmesi ve kullanılmayan GoldenGate/XStream teyidi |
| Data engineering | Connector, canonical mapping, Bronze, Raw Vault ve reconciliation |
| Platform/SRE | Kafka Connect HA, monitoring, backup, recovery ve runbook |

Oracle'ın object storage'a doğrudan bağlanması gerekmez. Sadece Kafka Connect'in
Oracle'a kontrollü ağ erişimi olur. Kafka/Bronze tarafı ayrı network zone'da kalır.

## 9. Gerçek MMS/KRD/PRM Discovery Süreci

Erişim verildiğinde ilk iş veri çekmek değildir. Önce metadata discovery yapılır:

1. Source owner'dan onaylı tablo/kolon inventory alınır.
2. Primary, unique ve foreign key'ler çıkarılır.
3. Tip, precision, scale, nullability, default ve comments kaydedilir.
4. Row count, günlük değişim hacmi ve en büyük transaction ölçülür.
5. PII, bankacılık sırrı, retention ve maskeleme sınıfları atanır.
6. Business key ile teknik surrogate key ayrılır.
7. Oracle-specific ve desteklenmeyen tipler için karar kaydı açılır.
8. Source-to-canonical kolon mapping'i version control'e alınır.
9. Data Vault Hub/Link/Satellite mapping'i source owner ile onaylanır.
10. Reconciliation sorguları ve kabul eşikleri hazırlanır.

Bu çalışma tamamlanmadan `table.include.list` doldurulmaz ve production snapshot
başlatılmaz.

## 10. Production Kabul Testleri

- [ ] Bankanın lisans ekibi LogMiner yaklaşımını onayladı.
- [ ] Non-production Oracle üzerinde connector çalıştı.
- [ ] Snapshot sırasında insert/update/delete uygulanarak gap testi geçti.
- [ ] Duplicate ve connector restart testleri geçti.
- [ ] Uzun transaction, peak redo ve archive pressure testi geçti.
- [ ] DDL/schema evolution senaryoları kontrollü şekilde test edildi.
- [ ] Oracle event fixture'ları canonical CDC schema contract testlerinden geçti.
- [ ] LOB ve Oracle-specific tip kararları doğrulandı.
- [ ] Source -> Kafka -> Bronze -> Raw Vault reconciliation geçti.
- [ ] Reconciliation aynı SCN/commit SCN ve batch manifest boundary'sinde çalıştı.
- [ ] Task aralarında gelen event'in sonraki batch'e kaldığı doğrulandı.
- [ ] Sabit delta batch'in maliyeti büyüyen Bronze history ile doğrusal artmadı.
- [ ] Credential rotation, TLS ve network isolation test edildi.
- [ ] Monitoring, alert, backup/restore ve incident runbook hazır.

Bu kapılar geçilene kadar Oracle yolu `Designed/External` statüsündedir. Lokal
PostgreSQL yolu ise CDC contract ve downstream idempotency için `Implemented/PoC`
statüsündedir.
