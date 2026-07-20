# Güvenlik ve Veri Erişim Kontrolü

## 1. Karar

Trino sorgu anındaki veri erişim enforcement noktasıdır. Production hedefinde Apache
Ranger; catalog, schema, table ve column yetkilendirmesi, masking, row filtering ve
access audit için kullanılacaktır.

Yerel Docker ortamına tam Ranger kurulmadı. Ranger Admin, policy database, LDAP grup
senkronizasyonu ve audit search store birlikte kurulmadan yalnızca bir Ranger container'ı
çalıştırmak production'a yakın bir kanıt oluşturmaz. Bunun yerine:

- Trino file-based access control ile çalışan davranış PoC'si eklendi.
- Aynı kararlar `security/access-matrix.yaml` içinde kaynak kontrollü tutuldu.
- Ranger 2.5+ config ve policy şablonları `security/ranger/` altında hazırlandı.
- Ranger durumu `designed_external`, local file policy durumu `implemented_poc` olarak işaretlendi.

## 2. Authentication ve Authorization Farkı

Authentication kullanıcının kim olduğunu kanıtlar. Authorization bu kimliğin hangi
veriye hangi işlemle erişebileceğine karar verir. Yalnızca Ranger policy yazmak yeterli
değildir; kullanıcı Trino'ya istediği username'i söyleyebiliyorsa policy aşılabilir.

Yerel PoC password authentication kullanır. Production sırası:

1. TLS ile istemci bağlantısını şifrele.
2. LDAP/AD, OAuth2 veya kurum standardıyla kullanıcıyı doğrula.
3. User/group mapping ile kurumsal grupları Trino kimliğine dönüştür.
4. Ranger plugin ile authorization kararını uygula.
5. Kararı bağımsız audit store'a gönder.

## 3. Role Matrix Özeti

| Rol | Silver | Gold | Audit | Temel sınır |
|---|---|---|---|---|
| platform_admin | admin | admin | admin | İnsan hesabı, break-glass kontrollü |
| data_engineer | read/write | read/write | read | Pipeline işletimi |
| transformation_service | read | read/write | none | Yalnızca dbt service account |
| metadata_service | connector read | connector read | none | Yalnızca ingestion service account |
| analyst | none | read | none | BI için minimize edilmiş Gold |
| branch_analyst | none | filtered read | none | Kurumsal branch attribute ile scope |
| auditor | masked read | read | read | PII varsayılan maskeli |
| bi_service | none | read | none | Power BI read-only service account |

Tam matris `security/access-matrix.yaml` dosyasındadır. Varsayılan karar deny'dır ve
service account'lar insan kullanıcı hesabı olarak kullanılmaz.

Trino file policy'de tablo görünürlüğü bir tablo privilege'ına bağlı olduğundan
OpenMetadata connector hesabı kaynak ve Gold tablolarda teknik olarak `SELECT` alır.
Bu hesap interaktif kullanıma kapatılır, network policy ile yalnız ingestion job'undan
erişir ve query audit'e tabidir; `metadata only` şeklinde daha güçlü bir iddia yapılmaz.

## 4. Masking

`sat_customer_details` içindeki doğrudan kimlik alanları Gold katmanına taşınmaz.
Auditor'a kontrollü Silver erişimi gerektiğinde:

- `national_id`, `tax_id`: yalnızca son dört karakter görünür.
- `first_name`, `last_name`: yalnızca ilk karakter görünür.
- `date_of_birth`: gün ve ay `01-01` olacak şekilde genellenir.

Masking sonucu kolonun veri tipi değişmemelidir. Yerel test gerçek Trino sorgusunda
maskeli ve açık değeri karşılaştırır. Masking, encryption veya tokenization yerine
geçmez; export, cache, log ve downstream kopyalar ayrıca kontrol edilir.

## 5. Row Filtering

Branch analyst PoC kullanıcısı için:

- `dim_customer_current.home_branch_code = 'BR001'`
- `fct_loans_current.branch_code = 'BR001'`

Production'da sabit `BR001` kullanılmaz. Kullanıcının branch attribute'u IAM/Ranger
context'inden gelir. Aggregate tablolar branch scope taşımıyorsa branch analyst'e açılmaz;
aksi halde filtrelenmiş fact üzerinde kapatılan veri aggregate üzerinden sızabilir.

## 6. OpenMetadata ile İlişki

OpenMetadata `Restricted`, `SensitiveIdentifier`, `Contact` ve `CreditExposure` gibi
classification bilgisini tutar. Ranger sorguyu engeller veya dönüştürür.

İlk rollout'ta tag-to-policy akışı kontrollü ve onaylıdır:

1. Steward classification önerir.
2. Data owner ve security ekibi etkiyi onaylar.
3. Policy değişikliği test ortamında uygulanır.
4. Allow, deny, mask ve row-filter regression testleri geçer.
5. Değişiklik production'a promotion ile alınır.

OpenMetadata tag silindiğinde Ranger policy'nin otomatik silinmesi varsayılan davranış
olmaz. Otomasyon için reconciliation, dry-run ve conflict policy gerekir.

## 7. Audit Modeli

Governance audit ve query-access audit farklı kayıtlardır:

- Governance audit: tag, glossary, owner veya domain değişikliğini kim yaptı?
- Query-access audit: hangi kullanıcı hangi tablo/kolona hangi kararla erişti?

Ranger audit event'lerinde en az event time, query ID, user/groups, client address,
resource, columns, access type, policy ID ve allow/deny kararı bulunmalıdır. Allowed ve
denied event'ler toplanmalı, ayrı audit store'da immutable retention uygulanmalı ve
SIEM'e aktarılmalıdır. Kesin retention süresi kurum regülasyon/records ekibi tarafından
onaylanmadan burada sayı olarak uydurulmaz.

## 8. Production Bağımlılıkları

- Ranger Admin HA ve desteklenen Ranger database
- Apache Ranger 2.5 veya üzeri Trino service definition
- Enterprise LDAP/AD group sync ve joiner/mover/leaver süreci
- Trino TLS, kurum authentication provider'ı ve internal communication security
- Ranger Admin bağlantısında TLS/mTLS ve secret manager
- Solr, Elasticsearch/OpenSearch, S3 veya kurum onaylı audit store
- SIEM forwarding, alert ve retention politikası
- Policy owner, approver, emergency access ve break-glass prosedürü
- Performance/load testi ve Ranger erişilemezken fail-closed davranış doğrulaması

Bu dış bağımlılıklar tamamlanmadan paket production security sertifikasyonu değildir.
