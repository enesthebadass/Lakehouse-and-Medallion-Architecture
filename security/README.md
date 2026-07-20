# Local Access-Control Runbook

Bu dizin iki farklı kanıt taşır:

- `trino/`: Trino file-based access control ile çalışan yerel davranış PoC'si
- `ranger/`: Apache Ranger 2.5+ için production deployment ve policy sözleşmesi

Yerel file policy Ranger değildir. Aynı deny, mask ve row-filter davranışını hızlı ve
tekrarlanabilir biçimde doğrular. Production enforcement hedefi Ranger olarak kalır.

## Yerel PoC'yi Başlat

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.access-control.yml \
  up -d --force-recreate trino

.venv/bin/python security/verify_access_control.py

docker compose \
  -f docker-compose.yml \
  -f docker-compose.access-control.yml \
  run --rm trino-init
```

Beklenen test çıktısı:

```text
Access-control PoC passed: auth, deny, mask, row filter
```

Test şu kontrolleri yapar:

1. Admin ve analyst doğru parola ile sorgu çalıştırır.
2. Analyst yalnızca Gold okur; CDC Raw Vault sorgusu reddedilir.
3. Auditor `national_id` değerinin yalnızca son dört hanesini görür.
4. Branch analyst `fct_loans_current` içinde yalnızca `BR001` satırlarını görür.
5. Yanlış parola HTTP 401 ile reddedilir.

## Yerel Kullanıcılar

| Kullanıcı | Demo parolası | Rol |
|---|---|---|
| `lakehouse-admin` | `admin-local` | platform admin |
| `data-engineer` | `engineer-local` | data engineer |
| `dbt-local` | `dbt-local` | transformation service |
| `openmetadata-ingestion` | `metadata-local` | metadata service |
| `analyst` | `analyst-local` | Gold analyst |
| `branch-analyst` | `branch-local` | BR001 scoped analyst |
| `auditor` | `auditor-local` | masked Silver/audit reader |
| `bi-service` | `bi-local` | Power BI service account |

Parolalar yalnızca localhost PoC içindir. Dosyada PBKDF2 hash olarak tutulmaları onları
production secret yapmaz. Production'da LDAP/AD/OIDC, TLS ve secret manager zorunludur.

PoC, sertifika yönetimini kapsam dışında tutmak için forwarded HTTPS işaretiyle HTTP
üzerinde çalışır. Bu ayar banka ağına veya paylaşılan ortama taşınmaz.

## Normal Demo Moduna Dön

```bash
docker compose -f docker-compose.yml up -d --force-recreate trino
```

Normal mod authentication kullanmaz ve yalnızca izole local geliştirme içindir.

## Production Ranger Geçişi

1. Ranger Admin HA deployment ve ayrı policy database kurulur.
2. Ranger 2.5+ Trino service definition doğrulanır.
3. LDAP/AD grupları Ranger ve Trino kimlikleriyle eşlenir.
4. Trino coordinator'a `ranger-access-control.properties.example` uyarlanır.
5. Ranger bağlantısı TLS/mTLS ve secret manager ile kurulur.
6. `policy-spec.yaml` politikaları dört-göz onayıyla Ranger'a girilir.
7. Allowed ve denied audit event'leri ayrı audit store'a, oradan SIEM'e gönderilir.
8. Aynı integration testleri kurum test kullanıcılarıyla yeniden çalıştırılır.

OpenMetadata classification tag'leri policy girdisidir; otomatik enforcement değildir.
Tag-to-policy senkronizasyonu ancak owner/security onayı ve conflict kontrolüyle açılır.
