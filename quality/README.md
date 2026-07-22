# Data Correctness Contract

`correctness-invariants.yaml`, pipeline retry, replay, failure ve source adapter
değişimlerinde korunması gereken veri garantilerinin makine tarafından okunabilir
sözleşmesidir.

Durumlar:

- `implemented_poc`: Lokal çalışan davranış ve otomatik veya tarihsel kanıt var.
- `partially_implemented`: Garantinin bir bölümü var, belirtilen eksik kapanmadı.
- `planned`: Production-shaped PoC ek fazlarında repository içinde uygulanacak.
- `external`: Yalnız banka/target ortamında doğrulanabilecek kabul kapısı.

Bir invariant ancak `evidence` alanındaki test veya baseline check çalıştığında
`implemented_poc` yapılır. Mimari dokümanda bulunması implementasyon kanıtı değildir.
`required_technical_fields` zorunlu alanları, `technical_field_semantics` ise bu
alanların retry ve source adapter değişimlerinde korunacak kesin anlamını tanımlar.

Faz 0 baseline collector şu yüzeyleri tek JSON raporda toplar:

- source tablo count'ları;
- Kafka topic/partition offset ve consumer lag;
- Bronze, Raw Vault, quarantine, audit ve Gold object/byte envanteri;
- son başarılı Airflow Spark task süreleri;
- Raw Vault ve Gold row count/content fingerprint'leri;
- source event identity uniqueness, Gold grain ve reconciliation sonucu;
- son published control-plane batch, attempt, task manifest evidence, source ledger
  conservation ve point-in-time audit sonucu;
- Git commit ve invariant contract checksum'u.

Çalıştırma:

```bash
python operations/collect_phase0_baseline.py \
  --output tests/results/phase0-baseline.json
```

No-op retry/replay sonrasında business sonucu karşılaştırmak için:

```bash
python operations/collect_phase0_baseline.py \
  --compare-to tests/results/phase0-baseline.json \
  --output tests/results/phase0-after-noop.json
```

Karşılaştırma; runtime timestamp ve Airflow süreleri gibi doğal olarak değişen
metadata'yı dışarıda bırakır. Source count, Bronze inventory ve Raw Vault/Gold content
fingerprint değişirse rapor `FAIL` olur.

Bu collector production monitoring aracı değildir. Faz değişiklikleri öncesi ve
sonrası tekrarlanabilir teknik kanıt üretir.

21 Temmuz 2026 lokal kanıtında ilk baseline ve aynı veriyi yeniden işleyen
`phase0-noop-20260721T120000Z` koşusu PASS oldu. Karşılaştırmada stable business
fingerprint farkı yoktu; ayrıntılı JSON dosyaları `tests/results/` altında lokal
kanıt olarak tutulur ve Git'e eklenmez.

Kayıtlı Faz 0 özeti:

| Ölçüm | Değer |
|---|---:|
| Source satırı | 868 |
| Kafka partition / toplam lag | 13 / 0 |
| Bronze object / byte | 895 / 1.420.151 |
| Bronze medyan / p95 object byte | 1.555 / 1.709 |
| Raw Vault object / byte | 99 / 605.779 |
| Gold object / byte | 437 / 1.017.183 |
| Reconciliation | 29 kontrol / 0 hata |
| No-op dört task toplam süresi | 274,25 saniye |
| No-op stable fingerprint farkı | 0 |

Mevcut recursive tasarım her Raw Vault fazında 895 object ve 1.420.151 logical byte'ı
aday kabul eder; dört faz için proxy 3.580 object ve 5.680.604 byte'dır. Bunlar Spark
physical I/O metriği değildir. Faz 1 ortak high sınırına sabitlenmiş snapshot'ı,
Faz 2 fiziksel `(low, high]` bounded okumayı, Faz 9 ise physical I/O ve
history-scaling bütçesini ekler.

Kayıtlı Faz 1 özeti:

- Ayrı `pipeline-control-postgres` ve Trino `pipeline_control` catalog'u çalışıyor.
- İlk manifest 13 partition ve 895 object için immutable low/high sınırı üretti.
- İlk batch çalışırken 23 yeni Bronze object oluşturuldu; ilk batch değişmeden
  `PUBLISHED` oldu ve bu object'ler ikinci batch aralığına girdi.
- İkinci batch'in yedi değişen partition'ında `low`, ilk batch'in `high` değeriyle
  birebir eşleşti; interval object toplamı 23 oldu.
- İlk batch, ikinci batch ve ikinci batch replay attempt'inde dörder task evidence,
  attempt başına tek manifest checksum ve sıfır failure kaydedildi.
- Replay öncesi/sonrası Raw Vault ve Gold stable fingerprint farkı sıfırdı.
- Final collector dokuz canlı kontrolle PASS oldu.

Bu kanıt immutable batch boundary ve replay idempotency içindir.

Kayıtlı Faz 2 özeti:

- Manifest v2 exact `(low, high]` object key, ETag ve size envanteri taşır.
- Yaklaşık 941 object geçmişinde dört Spark fazının her biri yalnız 23 object,
  40.477 byte ve 23 record işledi; 16 bounded reconciliation kontrolü geçti.
- Aynı manifest replay'inde stable fingerprint farkı sıfırdı.
- Sıfır-object batch dört Spark scriptinde session oluşturmadan tamamlandı.
- Final collector, bounded task evidence dahil 10 canlı kontrolle PASS oldu.

Bu değerler seçilen logical S3 object/byte maliyetidir. Gerçek connector physical I/O,
S3 metadata listing ölçeği ve history-scaling benchmark'ı Faz 9 kapısıdır.

Kayıtlı Faz 3 özeti:

- Manifest v3, beş PostgreSQL transaction için 23 beklenen/23 gözlenen CDC olayını
  ve kaynak LSN sınırını mühürledi.
- Altı point-in-time conservation kuralı ile 16 batch-to-target kuralı, toplam
  22/22 PASS verdi; attempt audit JSON'u URI ve SHA-256 ile kontrol DB'ye kaydedildi.
- Aynı manifest ikinci attempt olarak replay edildi. Replay sürerken gelen dört yeni
  Bronze olayı seçilmedi; Raw Vault ve Gold fingerprint farkı sıfır kaldı.
- `gold_row_lineage` 317 kaynak katkı satırı üretti; duplicate ve bütün Gold business
  key'leri için eksiksizlik testleriyle dbt paketi 169/169 PASS verdi.
- Collector'ın point-in-time audit dahil 11 zorunlu canlı kontrolü geçti.

Release/transformation sürümünün lineage zincirine bağlanması atomik Gold publish
fazında tamamlanacaktır.
