#!/usr/bin/env python3

import os
import re
import json
import time
import secrets
import argparse
import threading
import subprocess

ETCD_IMAGE = "quay.io/coreos/etcd:v3.3.5"

DELETABLE_RESOURCES = [
    "roles.rbac.authorization.k8s.io",
    "rolebindings.rbac.authorization.k8s.io"
]

NEWLINE_SEPARATE_OBJECTS_PATTERN = re.compile(r"\}\n+\{", re.MULTILINE)

GCP_KUBE_CONTEXT_NAME_PATTERN = re.compile(r"gke_([^_]+)_(.+)")

# Resources to delete on reset
DELETABLE_RESOURCES = [
    "replicasets",
    "services",
    "deployments",
    "pods",
    "rc",
    "serviceaccounts",
    "secrets",
    "clusterrole",
    "clusterrolebinding",
    "roles.rbac.authorization.k8s.io",
    "rolebindings.rbac.authorization.k8s.io",
]

class RedactedString(str):
    pass

class ExcThread(threading.Thread):
    def __init__(self, target):
        super().__init__(target=target)
        self.error = None

    def run(self):
        try:
            self._target()
        except Exception as e:
            self.error = e

def join(*targets):
    threads = []

    for target in targets:
        t = ExcThread(target)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()
    for t in threads:
        if t.error is not None:
            raise Exception("Thread error") from t.error

class BaseDriver:
    def clear(self):
        # ignore errors here because most likely no cluster is just deployed
        # yet
        try:
            run("pachctl", "undeploy", "--metadata", "--jupyterhub", stdin="y\n")
        except:
            pass

        run("kubectl", "delete", ",".join(DELETABLE_RESOURCES), "-l", "suite=pachyderm")

    def start(self):
        pass

    def create_manifest(self, include_dash):
        # We use hostpaths for storage. On docker for mac, hostpaths aren't
        # cleared until the VM is restarted -- I think this is the same on
        # minikube, though it's less relevant there because we wipe the
        # minikube VM entirely. Because of this behavior, re-deploying on the
        # same hostpath without a restart will cause us to bring up a new
        # pachyderm cluster with access to the old cluster volume, causing a
        # bad state. This works around the issue by just using a different
        # hostpath on every deployment.
        args = [
            "pachctl", "deploy", "local", "-d", "--dry-run",
            "--create-context", "--no-guaranteed",
            "--host-path", "/var/pachyderm-{}".format(secrets.token_hex(5))
        ]
        if not include_dash:
            args.append("--no-dashboard")
        return capture(*args)

    def sync_images(self, deployments):
        dash_spec = find_in_json(deployments, lambda j: \
            isinstance(j, dict) and j.get("name") == "dash" and j.get("image") is not None)
        grpc_proxy_spec = find_in_json(deployments, lambda j: \
            isinstance(j, dict) and j.get("name") == "grpc-proxy")

        if dash_spec is not None:
            run("docker", "pull", dash_spec["image"])
        if grpc_proxy_spec is not None:
            run("docker", "pull", grpc_proxy_spec["image"])
        run("docker", "pull", ETCD_IMAGE)

        push_images = [ETCD_IMAGE, "pachyderm/pachd:local", "pachyderm/worker:local"]
        if dash_spec is not None:
            push_images.append(dash_spec["image"])
        if grpc_proxy_spec is not None:
            push_images.append(grpc_proxy_spec["image"])

        return push_images

    def update_config(self):
        pass

class DockerDesktopDriver(BaseDriver):
    pass

class MinikubeDriver(BaseDriver):
    def clear(self):
        run("minikube", "delete")

    def start(self):
        run("minikube", "start")

        while run("minikube", "status", raise_on_error=False, capture_output=True).returncode != 0:
            print("Waiting for minikube to come up...")
            time.sleep(1)

    def sync_images(self, deployments):
        for image in super().sync_images(deployments):
            run("./etc/kube/push-to-minikube.sh", image)

    def update_config(self):
        ip = capture("minikube", "ip").strip()
        run("pachctl", "config", "update", "context", f"--pachd-address={ip}:30650")

class GCPDriver(BaseDriver):
    def __init__(self, project_id):
        self.project_id = project_id

    def clear(self):
        super().clear()
        run("kubectl", "delete", "secret", "regcred", raise_on_error=False)

    def create_manifest(self, include_dash):
        args = [
            "pachctl", "deploy", "local", "-d", "--dry-run", "--create-context", "--no-guaranteed",
            "--image-pull-secret", "regcred", "--registry", f"gcr.io/{self.project_id}"
        ]
        if not include_dash:
            args.append("--no-dashboard")
        return capture(*args)

    def sync_images(self, deployments):
        docker_config_path = os.path.expanduser("~/.docker/config.json")
        run("kubectl", "create", "secret", "generic", "regcred",
            f"--from-file=.dockerconfigjson={docker_config_path}",
            "--type=kubernetes.io/dockerconfigjson")

        for image in super().sync_images(deployments):
            if image.startswith("quay.io/"):
                image_url = f"gcr.io/{self.project_id}/{image[8:]}"
            else:
                image_url = f"gcr.io/{self.project_id}/{image}"

            run("docker", "tag", image, image_url)
            run("docker", "push", image_url)

def find_in_json(j, f):
    if f(j):
        return j

    iter = None
    if isinstance(j, dict):
        iter = j.values()
    elif isinstance(j, list):
        iter = j

    if iter is not None:
        for sub_j in iter:
            v = find_in_json(sub_j, f)
            if v is not None:
                return v

def print_status(status):
    print(f"===> {status}")

def run(cmd, *args, raise_on_error=True, stdin=None, capture_output=False, timeout=None):
    print_args = [cmd, *[a if not isinstance(a, RedactedString) else "[redacted]" for a in args]]
    print_status("running: `{}`".format(" ".join(print_args)))

    return subprocess.run([cmd, *args], check=raise_on_error, capture_output=capture_output, input=stdin,
                          encoding="utf8", timeout=timeout)

def capture(cmd, *args):
    return run(cmd, *args, capture_output=True).stdout

def main():
    parser = argparse.ArgumentParser(description="Resets a pachyderm cluster.")
    parser.add_argument("--dash", action="store_true", help="Deploy dash")
    parser.add_argument("--jupyterhub", action="store_true", help="Deploy jupyterhub")
    args = parser.parse_args()

    if "GOPATH" not in os.environ:
        raise Exception("Must set GOPATH")
    if "PACH_CA_CERTS" in os.environ:
        raise Exception("Must unset PACH_CA_CERTS\nRun:\nunset PACH_CA_CERTS")

    kube_context = capture("kubectl", "config", "current-context").strip()
    driver = None

    if kube_context == "minikube":
        print_status("using the minikube driver")
        driver = MinikubeDriver()
    elif kube_context == "docker-desktop":
        print_status("using the docker desktop driver")
        driver = DockerDesktopDriver()
    else:
        match = GCP_KUBE_CONTEXT_NAME_PATTERN.match(kube_context)
        if match is not None:
            print_status("using the GKE driver")
            driver = GCPDriver(match.groups()[0])

    if driver is None:
        raise Exception(f"could not derive driver from context name: {kube_context}")

    driver.clear()

    bin_path = os.path.join(os.environ["GOPATH"], "bin", "pachctl")
    if os.path.exists(bin_path):
        os.remove(bin_path)

    join(
        driver.start,
        lambda: run("make", "install"),
        lambda: run("make", "docker-build"),
    )
    
    version = capture("pachctl", "version", "--client-only").strip()
    print_status(f"deploy pachyderm version v{version}")

    deployments_str = driver.create_manifest(args.dash)
    deployments_json = json.loads("[{}]".format(NEWLINE_SEPARATE_OBJECTS_PATTERN.sub("},{", deployments_str)))
    driver.sync_images(deployments_json)
    run("kubectl", "create", "-f", "-", stdin=deployments_str)
    driver.update_config()

    while run("pachctl", "version", raise_on_error=False, capture_output=True).returncode:
        print_status("waiting for pachyderm to come up...")
        time.sleep(1)

    if args.jupyterhub:
        enterprise_token = capture("aws", "s3", "cp",
                                   "s3://pachyderm-engineering/test_enterprise_activation_code.txt", "-")
        run("pachctl", "enterprise", "activate", RedactedString(enterprise_token))
        run("pachctl", "auth", "activate", stdin="admin\n")
        run("pachctl", "deploy", "jupyterhub")

if __name__ == "__main__":
    main()
