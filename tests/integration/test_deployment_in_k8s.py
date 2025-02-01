import os
from pathlib import Path

import pytest
import yaml

from testcontainers.k3s import K3SContainer
from kubernetes import client, config
from pyhelm3 import Client


HERE = Path(os.path.abspath(os.path.dirname(__file__)))

k3s = K3SContainer("docker.io/rancher/k3s:v1.32.1-k3s1")


@pytest.fixture(scope="module")
def setup_k3s(request):
    k3s.start()

    def remove_container():
        k3s.stop()

    request.addfinalizer(remove_container)


@pytest.mark.asyncio
async def test_deploy_to_k8s_should_create_delta_tables(setup_k3s, tmp_path):
    config.load_kube_config_from_dict(yaml.safe_load(k3s.config_yaml()))
    kubeconfig_path = tmp_path / "kubeconfig.yaml"
    kubeconfig_path.write_text(k3s.config_yaml())

    helm_client = Client(kubeconfig=kubeconfig_path)

    chart = await helm_client.get_chart(HERE / "fixtures")

    # XXX: currently requires the chart dependencies to be installed
    #      before calling it: `helm dep up tests/integration/fixtures`
    revision = await helm_client.install_or_upgrade_release(
        release_name="fixtures",
        chart=chart,
        wait=True,
    )

    print(
        revision.release.name,
        revision.release.namespace,
        revision.revision,
        str(revision.status),
    )

    pod = client.CoreV1Api().list_pod_for_all_namespaces(limit=1)

    (x, y) = k3s.get_logs()

    print(x.decode("utf-8"))
    print(y.decode("utf-8"))

    revision = await helm_client.install_or_upgrade_release(
        release_name="fixtures",
        chart=chart,
        values={
            "stream-processors.enabled": "true",
            "stream-processors.processors.fhir-to-delta.image.tag": "test",
        },
        wait=True,
    )

    print(
        revision.release.name,
        revision.release.namespace,
        revision.revision,
        str(revision.status),
    )

    assert len(pod.items) > 0, "Unable to get running nodes from k3s cluster"
    # install strimzi operator
    # install kafka with tls
    # install minio
    # install this app using the stream-processor helm chart
    # load bundles using this test function
    # check that the delta tables are created (after some time)
