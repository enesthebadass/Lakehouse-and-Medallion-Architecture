package lakehouse.kubernetes.admission_test

import rego.v1
import data.lakehouse.kubernetes.admission

compliant_input := {
    "request": {
        "kind": {"kind": "Deployment"},
        "object": {
            "metadata": {
                "name": "metadata-ingestion",
                "labels": {
                    "app.kubernetes.io/name": "metadata-ingestion",
                    "platform.bank/owner": "data-governance",
                    "platform.bank/environment": "production",
                },
            },
            "spec": {
                "template": {
                    "spec": {
                        "serviceAccountName": "openmetadata-ingestion",
                        "automountServiceAccountToken": false,
                        "securityContext": {
                            "runAsNonRoot": true,
                            "seccompProfile": {"type": "RuntimeDefault"},
                        },
                        "containers": [{
                            "name": "ingestion",
                            "image": "registry.bank.internal/lakehouse/openmetadata@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                            "resources": {
                                "requests": {"cpu": "250m", "memory": "512Mi"},
                                "limits": {"cpu": "1", "memory": "1Gi"},
                            },
                            "securityContext": {
                                "allowPrivilegeEscalation": false,
                                "readOnlyRootFilesystem": true,
                                "capabilities": {"drop": ["ALL"]},
                            },
                            "env": [{
                                "name": "OPENMETADATA_JWT_TOKEN",
                                "valueFrom": {"secretKeyRef": {"name": "metadata-token", "key": "token"}},
                            }],
                        }],
                    },
                },
            },
        },
    },
}

input_with_container(patch) := result if {
    base_container := compliant_input.request.object.spec.template.spec.containers[0]
    container := object.union(base_container, patch)
    result := input_with_exact_container(container)
}

input_with_exact_container(container) := result if {
    result := object.union(
        compliant_input,
        {"request": {"object": {"spec": {"template": {"spec": {"containers": [container]}}}}}},
    )
}

test_compliant_workload_is_allowed if {
    count(admission.deny) == 0 with input as compliant_input
}

test_non_workload_resource_is_out_of_scope if {
    service_input := {
        "request": {
            "kind": {"kind": "Service"},
            "object": {"metadata": {"name": "trino"}, "spec": {}},
        },
    }
    count(admission.deny) == 0 with input as service_input
}

test_unapproved_registry_is_denied if {
    candidate := input_with_container({"image": "docker.io/trino:latest"})
    violations := admission.deny with input as candidate
    some message in violations
    contains(message, "approved registry")
}

test_mutable_image_is_denied if {
    candidate := input_with_container({"image": "registry.bank.internal/lakehouse/trino:latest"})
    violations := admission.deny with input as candidate
    some message in violations
    contains(message, "immutable sha256 digest")
}

test_missing_resources_are_denied if {
    base_container := compliant_input.request.object.spec.template.spec.containers[0]
    container := object.remove(base_container, {"resources"})
    candidate := input_with_exact_container(container)
    violations := admission.deny with input as candidate
    some message in violations
    contains(message, "requests and limits")
}

test_root_container_is_denied if {
    violations := admission.deny with input as compliant_input with input.request.object.spec.template.spec.securityContext.runAsNonRoot as false
    some message in violations
    contains(message, "non-root")
}

test_privileged_container_is_denied if {
    candidate := input_with_container({"securityContext": {"privileged": true}})
    violations := admission.deny with input as candidate
    some message in violations
    contains(message, "must not be privileged")
}

test_privilege_escalation_is_denied if {
    candidate := input_with_container({"securityContext": {"allowPrivilegeEscalation": true}})
    violations := admission.deny with input as candidate
    some message in violations
    contains(message, "disable privilege escalation")
}

test_writable_root_filesystem_is_denied if {
    candidate := input_with_container({"securityContext": {"readOnlyRootFilesystem": false}})
    violations := admission.deny with input as candidate
    some message in violations
    contains(message, "read-only root filesystem")
}

test_missing_capability_drop_is_denied if {
    candidate := input_with_container({"securityContext": {"capabilities": {"drop": []}}})
    violations := admission.deny with input as candidate
    some message in violations
    contains(message, "drop all Linux capabilities")
}

test_plaintext_secret_is_denied if {
    plaintext := [{"name": "DATABASE_PASSWORD", "value": "do-not-commit-this"}]
    candidate := input_with_container({"env": plaintext})
    violations := admission.deny with input as candidate
    some message in violations
    contains(message, "secretKeyRef")
}

test_default_service_account_is_denied if {
    violations := admission.deny with input as compliant_input with input.request.object.spec.template.spec.serviceAccountName as "default"
    "workload must use a non-default service account" in violations
}

test_missing_seccomp_is_denied if {
    violations := admission.deny with input as compliant_input with input.request.object.spec.template.spec.securityContext.seccompProfile.type as "Unconfined"
    "pod must use the RuntimeDefault seccomp profile" in violations
}

test_unapproved_token_automount_is_denied if {
    violations := admission.deny with input as compliant_input with input.request.object.spec.template.spec.automountServiceAccountToken as true
    "service account token automount must be disabled unless explicitly approved" in violations
}

test_missing_labels_are_denied if {
    violations := admission.deny with input as compliant_input with input.request.object.metadata.labels as {}
    some message in violations
    contains(message, "missing required labels")
}

test_host_path_is_denied if {
    host_volumes := [{"name": "host", "hostPath": {"path": "/etc"}}]
    violations := admission.deny with input as compliant_input with input.request.object.spec.template.spec.volumes as host_volumes
    some message in violations
    contains(message, "hostPath")
}
