package lakehouse.kubernetes.admission

import rego.v1

approved_registries := {"registry.bank.internal/"}
required_labels := {"app.kubernetes.io/name", "platform.bank/owner", "platform.bank/environment"}

workload_kind if {
    input.request.kind.kind in {"Pod", "Deployment", "StatefulSet", "DaemonSet", "Job", "CronJob"}
}

pod_spec := input.request.object.spec if {
    input.request.kind.kind == "Pod"
}

pod_spec := input.request.object.spec.template.spec if {
    input.request.kind.kind in {"Deployment", "StatefulSet", "DaemonSet", "Job"}
}

pod_spec := input.request.object.spec.jobTemplate.spec.template.spec if {
    input.request.kind.kind == "CronJob"
}

object_metadata := input.request.object.metadata

containers := array.concat(
    array.concat(
        object.get(pod_spec, "initContainers", []),
        object.get(pod_spec, "containers", []),
    ),
    object.get(pod_spec, "ephemeralContainers", []),
)

approved_image(image) if {
    some registry in approved_registries
    startswith(image, registry)
}

sensitive_env_name(name) if {
    regex.match("(?i).*(password|passwd|token|secret|api[_-]?key|private[_-]?key).*", name)
}

resources_complete(container) if {
    resources := object.get(container, "resources", {})
    requests := object.get(resources, "requests", {})
    limits := object.get(resources, "limits", {})
    requests.cpu
    requests.memory
    limits.cpu
    limits.memory
}

deny contains msg if {
    container := containers[_]
    not approved_image(container.image)
    msg := sprintf("container %q must use an approved registry", [container.name])
}

deny contains msg if {
    container := containers[_]
    not contains(container.image, "@sha256:")
    msg := sprintf("container %q image must use an immutable sha256 digest", [container.name])
}

deny contains msg if {
    container := containers[_]
    not resources_complete(container)
    msg := sprintf("container %q must define CPU/memory requests and limits", [container.name])
}

deny contains msg if {
    container := containers[_]
    container_security := object.get(container, "securityContext", {})
    pod_security := object.get(pod_spec, "securityContext", {})
    not object.get(container_security, "runAsNonRoot", object.get(pod_security, "runAsNonRoot", false))
    msg := sprintf("container %q must run as non-root", [container.name])
}

deny contains msg if {
    container := containers[_]
    security := object.get(container, "securityContext", {})
    object.get(security, "allowPrivilegeEscalation", true)
    msg := sprintf("container %q must disable privilege escalation", [container.name])
}

deny contains msg if {
    container := containers[_]
    security := object.get(container, "securityContext", {})
    not object.get(security, "readOnlyRootFilesystem", false)
    msg := sprintf("container %q must use a read-only root filesystem", [container.name])
}

deny contains msg if {
    container := containers[_]
    security := object.get(container, "securityContext", {})
    object.get(security, "privileged", false)
    msg := sprintf("container %q must not be privileged", [container.name])
}

deny contains msg if {
    container := containers[_]
    security := object.get(container, "securityContext", {})
    capabilities := object.get(security, "capabilities", {})
    not "ALL" in object.get(capabilities, "drop", [])
    msg := sprintf("container %q must drop all Linux capabilities", [container.name])
}

deny contains "pod must use the RuntimeDefault seccomp profile" if {
    security := object.get(pod_spec, "securityContext", {})
    seccomp := object.get(security, "seccompProfile", {})
    object.get(seccomp, "type", "") != "RuntimeDefault"
}

deny contains "workload must use a non-default service account" if {
    object.get(pod_spec, "serviceAccountName", "default") == "default"
}

deny contains "service account token automount must be disabled unless explicitly approved" if {
    object.get(pod_spec, "automountServiceAccountToken", true)
    annotations := object.get(object_metadata, "annotations", {})
    object.get(annotations, "platform.bank/kubernetes-api-access", "") != "approved"
}

deny contains msg if {
    volume := object.get(pod_spec, "volumes", [])[_]
    volume.hostPath
    msg := sprintf("hostPath volume %q is forbidden", [volume.name])
}

deny contains msg if {
    container := containers[_]
    env := object.get(container, "env", [])[_]
    sensitive_env_name(env.name)
    env.value
    msg := sprintf("sensitive environment variable %q must use secretKeyRef", [env.name])
}

deny contains msg if {
    workload_kind
    labels := object.get(object_metadata, "labels", {})
    missing := required_labels - {label | labels[label]}
    count(missing) > 0
    msg := sprintf("workload is missing required labels: %v", [missing])
}
