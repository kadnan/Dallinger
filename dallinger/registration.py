"""Register experiments through the Open Science Framework."""

import os
import requests

from psiturk.psiturk_config import PsiturkConfig
config = PsiturkConfig()
config.load_config()

try:
    config.get('OSF', 'osf_access_token')
except Exception:
    pass

root = "https://api.osf.io/v2"


def register(dlgr_id, snapshot=None):
    """Register the experiment."""
    osf_id = create_osf_project(dlgr_id)
    upload_assets(dlgr_id, osf_id)


def create_osf_project(dlgr_id, description=None):
    """Create a project on the OSF."""

    if not description:
        description = "Experiment {} registered by Dallinger.".format(
            dlgr_id
        )

    r = requests.post(
        "{}/nodes/".format(root),
        data={
            "type": "nodes",
            "category": "project",
            "title": "Experiment dlgr-{}".format(dlgr_id[0:8]),
            "description": description,
        },
        headers={
            "Authorization": "Bearer {}".format(personal_access_token)
        }
    )

    osf_id = r.json()['data']['id']

    print("Project registered on OSF at http://osf.io/{}".format(osf_id))

    return osf_id


def upload_assets(dlgr_id, osf_id, provider="osfstorage"):
    """Upload experimental assets to the OSF."""
    root = "https://files.osf.io/v1"
    snapshot_filename = "{}-code.zip".format(dlgr_id)
    snapshot_path = os.path.join("snapshots", snapshot_filename)
    requests.put(
        "{}/resources/{}/providers/{}/".format(
            root,
            osf_id,
            provider,
        ),
        params={
            "kind": "file",
            "name": snapshot_filename,
        },
        headers={
            "Authorization": "Bearer {}".format(personal_access_token),
            "Content-Type": "text/plain",
        },
        data=open(snapshot_path, 'rb'),
    )


if __name__ == "__main__":
    register("test")
