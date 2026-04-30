# Copyright (c) 2021 SUSE LLC
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of version 3 of the GNU General Public License as
# published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.   See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, contact SUSE LLC.
#
# To contact SUSE about this file by physical or electronic mail,
# you may find current contact information at www.suse.com
import warnings
from time import sleep
from datetime import datetime, timedelta, timezone

import pytest

pytest_plugins = [
    "harvester_e2e_tests.fixtures.api_client"
]


@pytest.mark.p0
@pytest.mark.smoke
@pytest.mark.settings
def test_get_all_settings(api_client, expected_settings):
    expected_settings = expected_settings['default']
    code, data = api_client.settings.get()

    available_settings = {m['metadata']['name'] for m in data['items']}

    assert 200 == code, (code, data)
    assert expected_settings <= available_settings, (
        "Some setting missing:\n"
        f"{expected_settings - available_settings}"
    )


@pytest.mark.p0
@pytest.mark.settings
@pytest.mark.skip_if_version("< v1.1.0")
def test_get_all_settings_v110(api_client, expected_settings):
    expected_settings = expected_settings['default'] | expected_settings['1.1.0']
    code, data = api_client.settings.get()

    available_settings = {m['metadata']['name'] for m in data['items']}

    assert 200 == code, (code, data)
    assert expected_settings <= available_settings, (
        "Some setting missing:\n"
        f"{expected_settings - available_settings}"
    )

    removed = expected_settings - available_settings
    added = available_settings - expected_settings

    if removed:
        warnings.warn(UserWarning(f"Few setting(s) been removed: {removed}."))
    if added:
        warnings.warn(UserWarning(f"New setting(s) added: {added}"))


@pytest.mark.p0
@pytest.mark.sanity
@pytest.mark.settings
@pytest.mark.dependency(name="test_update_log_level")
def test_update_log_level(api_client):
    code, data = api_client.settings.get("log-level")
    assert 200 == code, (f"Failed to get log-level setting with error: {code}, {data}")

    original_value = data.get("value", data['default'])
    updates = {"value": "Debug"}
    code, data = api_client.settings.update("log-level", updates)

    assert 200 == code, (f"Failed to update log-level setting with error: {code}, {data}")

    # For teardown
    updates = {"value": original_value}
    api_client.settings.update("log-level", updates)


@pytest.mark.p0
@pytest.mark.sanity
@pytest.mark.settings
@pytest.mark.dependency(depends=["test_update_log_level"])
def test_log_level_persists_after_pod_restart(api_client, wait_timeout):
    """ ref: https://github.com/harvester/tests/issues/1460
        bug: https://github.com/harvester/harvester/issues/6378

    Verifies that the harvester apiserver controller observes and applies
    log-level setting changes (hot-reload), persists the value across pod
    restart, and applies the persisted value when a new pod starts. Does
    not verify downstream effects on log output or other components.

    Steps:
      1. Get current log-level value (and remember it for cleanup)
      2. Update log-level to a different value (Debug/Info)
      3. Confirm hot-reload: tail apiserver pod logs and wait for
         "set log level to <new_value>" emitted by the controller. This
         proves the running controller saw the change and applied it.
      4. Restart all harvester pods (apiserver + webhook share the
         app.kubernetes.io/name=harvester label)
      5. Wait for new pods Ready and the settings API to answer
      6. Verify the API still returns the new value (persistence — the
         specific failure mode of #6378)
      7. Confirm new pods applied the value at startup: tail the new
         apiserver pod logs and wait for the same
         "set log level to <new_value>" line. Wrangler controllers use
         leader election so only the leader emits it; we accept a hit on
         any pod.
      8. Cleanup restores the original API value AND restarts pods again
         so the in-memory log level on the running pods matches the API
         value, leaving the cluster identical to the pre-test state.
    """
    def _harvester_pods(pods_resp):
        return [
            p for p in pods_resp["data"]
            if p["metadata"]["labels"].get("app.kubernetes.io/name") == "harvester"
        ]

    def _is_ready(pod):
        for cond in pod["status"].get("conditions", []):
            if cond["type"] == "Ready":
                return cond["status"] == "True"
        return False

    def _log(msg):
        # Visible with `pytest -s`; useful for diagnosing rollout issues.
        print(f"[log-level-persist] {msg}", flush=True)

    def _wait_for_setlevel_log(expected_value, scope, timeout):
        """Poll all harvester apiserver pod logs until at least one
        contains a line matching `set log level to <expected_value>`
        (case-insensitive — logrus.ParseLevel lowercases the value).

        `scope` controls which lines count:
          - "since=<datetime>": only count lines whose RFC3339 timestamp
             is at or after the given UTC datetime. Used after a runtime
             setting change to ignore historical "set log level to ..."
             lines from previous test runs.
          - "all": count any line. Used after a pod restart, where the
             pod has no historical log content from before the test.
        Returns (pod_name, line) on success; raises AssertionError on
        timeout.
        """
        target = f"set log level to {expected_value}".lower()
        ts_filter = None
        if isinstance(scope, datetime):
            ts_filter = scope.strftime("%Y-%m-%dT%H:%M:%S")
        elif scope != "all":
            raise ValueError(f"unknown scope: {scope!r}")

        deadline = datetime.now() + timedelta(seconds=timeout)
        last_seen_pods = []
        while datetime.now() < deadline:
            code, pods = api_client.get_pods(namespace="harvester-system")
            if 200 != code:
                sleep(2)
                continue
            apiserver_names = [
                p["metadata"]["name"] for p in pods["data"]
                if p["metadata"]["labels"].get("app.kubernetes.io/name") == "harvester"
                and p["metadata"]["labels"].get("app.kubernetes.io/component") == "apiserver"
            ]
            last_seen_pods = apiserver_names
            for name in apiserver_names:
                # /log via the Steve k8s proxy returns text/plain log content.
                url = api_client.get_url(
                    f"k8s/clusters/local/api/v1/namespaces/harvester-system"
                    f"/pods/{name}/log?container=apiserver&tailLines=500"
                )
                resp = api_client.session.get(url)
                if resp.status_code != 200:
                    continue
                for line in resp.text.splitlines():
                    if target not in line.lower():
                        continue
                    if ts_filter is not None:
                        # Lines look like: time="2026-04-30T18:06:08Z" level=info ...
                        # Compare lexically — RFC3339 in UTC sorts correctly.
                        m = line.split('"', 2)
                        line_ts = m[1] if len(m) >= 2 else ""
                        if line_ts < ts_filter:
                            continue
                    return name, line
            sleep(3)
        raise AssertionError(
            f"Did not find 'set log level to {expected_value}' in any "
            f"apiserver pod log within {timeout}s. "
            f"Pods checked: {last_seen_pods}"
        )

    def _restart_harvester_pods(label):
        """Delete all harvester pods, wait for the deployment to roll new
        ones that are all Ready and have UIDs different from the old set,
        then poll the settings API until it answers. Returns the response
        from settings.get("log-level") on success.

        Strict checks defend against the transient state where deletion is
        in progress and the API briefly returns old (Running) + new
        (Pending) pods at the same time.
        """
        code, pods = api_client.get_pods(namespace="harvester-system")
        assert 200 == code, (code, pods)

        harvester_pods = _harvester_pods(pods)
        assert harvester_pods, (
            f"No harvester pods found in harvester-system: {pods['data']}"
        )

        old_uids = {p["metadata"]["uid"] for p in harvester_pods}
        expected_count = len(old_uids)
        _log(
            f"[{label}] restarting {expected_count} harvester pods: "
            f"{[p['metadata']['name'] for p in harvester_pods]}"
        )

        for pod in harvester_pods:
            ns = pod["metadata"]["namespace"]
            name = pod["metadata"]["name"]
            resp = api_client._delete(f"v1/pods/{ns}/{name}")
            assert resp.status_code in (200, 202), (
                f"Failed to delete pod {name}: {resp.status_code}, {resp.text}"
            )
            _log(f"  [{label}] DELETE {name} -> {resp.status_code}")

        _log(f"[{label}] waiting up to {wait_timeout}s for new pods to be Ready")
        last_state = "no successful poll"
        rollout_start = datetime.now()
        endtime = rollout_start + timedelta(seconds=wait_timeout)
        poll_n = 0
        while endtime > datetime.now():
            code, pods = api_client.get_pods(namespace="harvester-system")
            if 200 == code:
                cur = _harvester_pods(pods)
                cur_uids = {p["metadata"]["uid"] for p in cur}
                old_still_present = cur_uids & old_uids
                ready_count = sum(1 for p in cur if _is_ready(p))
                last_state = (
                    f"total={len(cur_uids)} expected={expected_count} "
                    f"ready={ready_count} old_still_present={len(old_still_present)}"
                )
                poll_n += 1
                elapsed = (datetime.now() - rollout_start).total_seconds()
                _log(f"  [{label}] poll #{poll_n} t={elapsed:5.1f}s: {last_state}")
                if (not old_still_present
                        and len(cur_uids) == expected_count
                        and ready_count == len(cur_uids)):
                    _log(f"  [{label}] rollout complete after {elapsed:.1f}s")
                    break
            sleep(5)
        else:
            raise AssertionError(
                f"Harvester pods did not fully roll within {wait_timeout}s. "
                f"Last observed: {last_state}"
            )

        _log(f"[{label}] polling settings API until it responds")
        api_start = datetime.now()
        endtime = api_start + timedelta(seconds=wait_timeout)
        while endtime > datetime.now():
            code, data = api_client.settings.get("log-level")
            if 200 == code:
                elapsed = (datetime.now() - api_start).total_seconds()
                _log(f"  [{label}] settings API responsive after {elapsed:.1f}s")
                return data
            sleep(5)
        raise AssertionError(
            f"Settings API not reachable within {wait_timeout}s after restart"
        )

    code, data = api_client.settings.get("log-level")
    assert 200 == code, (code, data)
    original_value = data.get("value") or data.get("default", "info")
    # log-level accepts info/debug/trace/warn/error per harvester docs;
    # flip between Debug/Info to guarantee a real change.
    new_value = "Debug" if original_value.lower() != "debug" else "Info"
    _log(f"step 1: original log-level={original_value!r}, will set to {new_value!r}")

    # Track whether we actually changed the value, so cleanup knows whether
    # a pod restart is needed to make the restored value take effect.
    value_changed = False
    try:
        # Capture the boundary timestamp BEFORE the update so the log
        # search can ignore any historical "set log level to ..." lines
        # already in the leader pod's log buffer.
        update_ts = datetime.now(timezone.utc)
        code, data = api_client.settings.update("log-level", {"value": new_value})
        assert 200 == code, (
            f"Failed to set log-level to {new_value}: {code}, {data}"
        )
        value_changed = True

        code, data = api_client.settings.get("log-level")
        assert 200 == code and data.get("value") == new_value, (
            f"log-level should be {new_value} before restart, got: {data}"
        )
        _log(f"step 2: confirmed log-level is now {data.get('value')!r}")

        # Step 3: hot-reload check. The setting controller logs
        # "set log level to <value>" (lowercased by logrus.ParseLevel)
        # on every change. Wait for it to confirm the running controller
        # reacted to our update.
        _log("step 3: waiting for hot-reload log line on running pods")
        pod, line = _wait_for_setlevel_log(new_value, scope=update_ts, timeout=60)
        _log(f"  hot-reload confirmed on {pod}: {line[:160]}")

        _log("step 4-5: restart pods and wait for ready")
        data = _restart_harvester_pods("test")

        # Defend against API shape changes: settings.get is expected to
        # return a dict with at least a "value" key. If that contract
        # breaks, fail loudly instead of comparing None to new_value.
        assert isinstance(data, dict) and "value" in data, (
            f"Unexpected settings.get(\"log-level\") response shape: {data!r}"
        )
        _log(f"step 6: post-restart log-level={data['value']!r} (expected {new_value!r})")
        assert data["value"] == new_value, (
            f"log-level reverted after pod restart. "
            f"Expected {new_value}, got {data['value']}. "
            f"Regression from harvester#6378."
        )

        # Step 7: startup check on the new pods. Brand-new pods have no
        # log content from before the test, so scope="all" is safe.
        _log("step 7: waiting for startup log line on new pods")
        pod, line = _wait_for_setlevel_log(new_value, scope="all", timeout=120)
        _log(f"  startup application confirmed on {pod}: {line[:160]}")
        _log("PASS: hot-reload + persistence + startup-apply all verified")
    finally:
        # Restore the previous known-good state: API value AND running
        # pods' in-memory log level. Each step is checked individually so
        # the warning on failure can describe what actually went wrong.
        # No step here may raise — warnings only — to avoid masking the
        # original test assertion (if any).
        if value_changed:
            cleanup_step = "(none attempted)"
            api_restored = False
            try:
                # Step A: revert the API value. Settings updates are
                # synchronous on success, so doing the pod restart right
                # after is safe — no controller reacts to settings changes
                # by mutating cluster state.
                cleanup_step = "settings.update(original_value)"
                code, data = api_client.settings.update(
                    "log-level", {"value": original_value}
                )
                if code != 200:
                    raise RuntimeError(
                        f"settings.update returned {code}: {data}"
                    )
                api_restored = True
                _log(f"cleanup: restored log-level API value to {original_value!r}")

                # Step B: restart pods so the in-memory log level matches
                # the API value we just restored.
                cleanup_step = "_restart_harvester_pods(cleanup)"
                _restart_harvester_pods("cleanup")
                _log("cleanup: pods restarted, in-memory log level back in sync")
            except Exception as e:
                if not api_restored:
                    state = (
                        f"API value still {new_value!r} (restore failed); "
                        f"running pods also have {new_value!r} in memory"
                    )
                else:
                    state = (
                        f"API value is {original_value!r} (restored), but "
                        f"running pods may still have {new_value!r} in "
                        f"memory because the cleanup pod-restart failed"
                    )
                msg = (
                    f"Cleanup failed at {cleanup_step}: {e}. "
                    f"Cluster state: {state}."
                )
                _log(f"cleanup: WARNING — {msg}")
                warnings.warn(UserWarning(msg))


@pytest.mark.p0
@pytest.mark.smoke
@pytest.mark.settings
def test_get_storage_network(api_client):
    code, data = api_client.settings.get("storage-network")
    assert 200 == code, (f"Failed to get storage-network setting with error: {code}, {data}")


@pytest.mark.p0
@pytest.mark.sanity
@pytest.mark.negative
@pytest.mark.settings
class TestUpdateInvalidStorageNetwork:
    invalid_vlan_id = 4095
    invalid_ip_range = "127.0.0.1/24"
    mgmt_network = "mgmt"

    def test_invalid_vlanid(self, api_client):
        spec = api_client.settings.StorageNetworkSpec.enable_with(
            self.invalid_vlan_id, self.mgmt_network, "192.168.1.0/24"
        )
        code, data = api_client.settings.update('storage-network', spec)

        assert 422 == code, (
            f"Storage Network should NOT able to create with VLAN ID: {self.invalid_vlan_id}\n"
            f"API Status({code}): {data}"
        )

    def test_invalid_iprange(self, api_client):
        valid_vlan_id = 1
        spec = api_client.settings.StorageNetworkSpec.enable_with(
            valid_vlan_id, self.mgmt_network, self.invalid_ip_range
        )
        code, data = api_client.settings.update('storage-network', spec)

        assert 422 == code, (
            f"Storage Network should NOT able to create with IP Range: {self.invalid_ip_range}\n"
            f"API Status({code}): {data}"
        )


@pytest.mark.p0
@pytest.mark.sanity
@pytest.mark.negative
@pytest.mark.settings
class TestUpdateInvalidBackupTarget:
    def test_invalid_nfs(self, api_client):
        NFSSpec = api_client.settings.BackupTargetSpec.NFS

        spec = NFSSpec('not_starts_with_nfs://')
        code, data = api_client.settings.update('backup-target', spec)
        assert 422 == code, (
            f"NFS backup-target should check endpoint starting with `nfs://`\n"
            f"API Status({code}): {data}"
        )

        spec = NFSSpec('nfs://:/lack_server')
        code, data = api_client.settings.update('backup-target', spec)
        assert 422 == code, (
            f"NFS backup-target should check endpoint had server path\n"
            f"API Status({code}): {data}"
        )

        spec = NFSSpec('nfs://127.0.0.1:')
        code, data = api_client.settings.update('backup-target', spec)
        assert 422 == code, (
            f"NFS backup-target should check endpoint had mount path\n"
            f"API Status({code}): {data}"
        )

    def test_invalid_S3(self, api_client):
        S3Spec = api_client.settings.BackupTargetSpec.S3

        spec = S3Spec('bogus_bucket', 'bogus_region', 'bogus_key', 'bogus_secret')
        code, data = api_client.settings.update('backup-target', spec)
        assert 422 == code, (
            f"S3 backup-target should check key/secret/bucket/region"
            f"API Status({code}): {data}"
        )

        spec = S3Spec('', '', '', '', endpoint="http://127.0.0.1")
        code, data = api_client.settings.update('backup-target', spec)
        assert 422 == code, (
            f"S3 backup-target should check key/secret/bucket/region"
            f"API Status({code}): {data}"
        )


@pytest.mark.p0
@pytest.mark.sanity
@pytest.mark.settings
class TestUpdateKubeconfigDefaultToken:
    @pytest.mark.skip_if_version(
            "< v1.3.1",
            reason="https://github.com/harvester/harvester/issues/5891 fixed after v1.3.1")
    def test_invalid_kubeconfig_ttl_min(self, api_client):
        KubeconfigTTLMinSpec = api_client.settings.KubeconfigDefaultTokenTTLSpec.TTL
        spec = KubeconfigTTLMinSpec(99999999999999)
        code, data = api_client.settings.update('kubeconfig-default-token-ttl-minutes', spec)
        assert 422 == code, (
            f"Kubeconfig Default Token TTL Minutes should not exceed 100yrs\n"
            f"API Status({code}): {data}"
        )

    @pytest.mark.skip_if_version("< v1.3.1")
    def test_valid_kubeconfig_ttl_min(self, api_client):
        KubeconfigTTLMinSpec = api_client.settings.KubeconfigDefaultTokenTTLSpec.TTL
        spec = KubeconfigTTLMinSpec(172800)
        code, data = api_client.settings.update('kubeconfig-default-token-ttl-minutes', spec)
        assert 200 == code, (
            f"Kubeconfig Default Token TTL Minutes be allowed to be set for 120 days\n"
            f"API Status({code}): {data}"
        )
