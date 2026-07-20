# Platform Policy Package

Bu dizin hedef Kubernetes/OpenShift runtime sözleşmesini ve OPA policy-as-code
kontrollerini içerir. Buradaki manifestler production deployment'ın tamamı değildir;
platform ekibinin onaylı chart/operator ve kurum servisleriyle tamamlayacağı güvenlik
baseline'ıdır.

## Durumlar

- Docker Compose: `implemented_local`
- OPA offline policy testleri: `implemented_poc`
- Kubernetes/OpenShift rollout: `designed_external`
- Gatekeeper admission deployment: `designed_external`

## Dosyalar

- `deployment-responsibilities.yaml`: component, namespace ve sahiplik sözleşmesi
- `iam-group-mapping.yaml`: kurumsal grup, Kubernetes, Ranger ve OpenMetadata eşlemesi
- `policies/kubernetes/`: Rego admission policy ve unit testleri
- `kubernetes/examples/`: restricted workload, RBAC ve network policy örnekleri

## Yerel Doğrulama

```bash
docker run --rm \
  --volume "$PWD/platform/policies:/policies:ro" \
  openpolicyagent/opa:1.17.0-static \
  test /policies --verbose --fail-on-empty

.venv/bin/python tests/test_platform_blueprint.py
```

On altı OPA testi approved registry, immutable digest, non-root, privilege escalation,
read-only filesystem, resource request/limit, secret reference, non-default service
account, labels, seccomp ve hostPath kurallarını değerlendirir.

## Production Uygulama

OPA'nın Kubernetes admission entegrasyonu için hedef Gatekeeper'dır. Kurum başka bir
admission standardı kullanıyorsa aynı test edilmiş kararlar o motora taşınır. Önce
`dryrun`, sonra `warn`, kontrollü namespace pilotundan sonra `deny` uygulanır.

Gerçek image digest, registry, certificate, secret, storage class, node selector ve
kurum endpoint'leri Git'e açık değer olarak yazılmaz. Bunlar environment overlay,
GitOps ve external secret mekanizmasıyla enjekte edilir.
