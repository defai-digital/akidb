# Deployment Assets

This directory contains deployment manifests, templates, and scripts.

- `ansible/` - Ansible inventory, playbooks, and systemd templates.
- `compose/` - Docker Compose stacks and monitoring dashboards.
- `docker/` - Dockerfiles for AkiDB services.
- `kubernetes/` - Kubernetes manifests.
- `prometheus/` and `grafana/` - standalone monitoring assets.
- `scripts/` - deployment helper scripts.

Compiled binaries are build artifacts and are not tracked in git. Build them
under `target/` and pass the resulting directory to deployment tooling through
`local_binary_dir` when needed.
