from contextlib import contextmanager
import os
import select
import subprocess
import tempfile
import time
from pathlib import Path

from django import forms
from extras.scripts import Script, StringVar, ObjectVar, BooleanVar, FileVar
from tenancy.models import Tenant
from utilities.exceptions import AbortScript
from virtualization.models import Cluster, VirtualMachine


class SyncNetBoxVMsToOpenStack(Script):
    class Meta:
        name = "Sync NetBox VMs to OpenStack"
        description = "Push NetBox virtual machine data into OpenStack instances. Leave Commit unchecked to run in dry-run mode."
        fieldsets = (
            (
                "VPN tunnel",
                (
                    "vpn_profile",
                ),
            ),
            (
                "OpenStack authentication",
                (
                    "auth_url",
                    "username",
                    "password",
                    "user_domain_name",
                    "project_domain_name",
                    "verify",
                ),
            ),
            (
                "NetBox scope",
                (
                    "cluster",
                    "tenant",
                    "name_prefix",
                ),
            ),
            (
                "Sync behavior",
                (
                    "allow_rename",
                    "update_metadata",
                    "sync_power_state",
                ),
            ),
            (
                "Optional instance creation",
                (
                    "allow_create",
                    "image_name",
                    "network_name",
                    "wait_for_active",
                ),
            ),
        )
        commit_default = False

    auth_url = StringVar(description="Keystone authentication URL")
    username = StringVar(description="OpenStack username")
    password = StringVar(
        description="OpenStack password",
        widget=forms.PasswordInput,
    )
    user_domain_name = StringVar(
        required=False,
        default="Default",
        description="User domain name, for example Default",
    )
    project_domain_name = StringVar(
        required=False,
        default="Default",
        description="Project domain name, for example Default",
    )
    verify = BooleanVar(
        required=False,
        default=True,
        description="Validate the OpenStack API TLS certificate",
    )
    vpn_profile = FileVar(
        required=False,
        description="Optional OpenVPN profile (.ovpn) to bring up before syncing; the upload is only kept for this job",
    )

    cluster = ObjectVar(
        model=Cluster,
        description="Only NetBox VMs in this cluster will be synchronized",
    )
    tenant = ObjectVar(
        model=Tenant,
        required=False,
        description="Optional tenant filter for NetBox VMs",
    )
    name_prefix = StringVar(
        required=False,
        description="Optional prefix to limit synchronization to matching NetBox VM names",
    )
    allow_rename = BooleanVar(
        required=False,
        default=True,
        description="Rename the OpenStack instance when the NetBox VM name differs",
    )
    update_metadata = BooleanVar(
        required=False,
        default=True,
        description="Update OpenStack metadata from NetBox VM fields",
    )
    sync_power_state = BooleanVar(
        required=False,
        default=False,
        description="Map NetBox status to OpenStack power state: active -> start, offline -> stop",
    )

    allow_create = BooleanVar(
        required=False,
        default=False,
        description="Create a missing OpenStack instance for a NetBox VM",
    )
    image_name = StringVar(
        required=False,
        description="Required for creation when the VM does not define its own image custom field",
    )
    network_name = StringVar(
        required=False,
        description="Required for creation when the VM does not define its own network custom field",
    )
    wait_for_active = BooleanVar(
        required=False,
        default=True,
        description="Wait for newly created instances to become ACTIVE",
    )

    def run(self, data, commit):
        if commit:
            self.log_info("Running in apply mode: changes will be sent to OpenStack")
        else:
            self.log_warning("Running in dry-run mode: no changes will be sent to OpenStack or saved back to NetBox")

        try:
            import openstack
        except ImportError as exc:
            raise AbortScript("openstacksdk is not installed in the NetBox Python environment") from exc

        with self._openvpn_tunnel(data.get("vpn_profile")):
            cluster = data["cluster"]
            tenant = data.get("tenant")
            name_prefix = (data.get("name_prefix") or "").strip()


            queryset = VirtualMachine.objects.filter(cluster=cluster)
            if tenant is not None:
                queryset = queryset.filter(tenant=tenant)
            if name_prefix:
                queryset = queryset.filter(name__startswith=name_prefix)

            netbox_vms = list(queryset.order_by("name"))
            if not netbox_vms:
                self.log_info("No NetBox VMs matched the selected filters")
                return "No matching NetBox VMs found"

            connection_cache = {}
            created_count = 0
            updated_count = 0
            unchanged_count = 0
            failed_count = 0

            for vm in netbox_vms:
                try:
                    conn = self._get_connection_for_vm(openstack, data, vm, connection_cache)
                    result = self._sync_vm(
                        conn=conn,
                        vm=vm,
                        data=data,
                        commit=commit,
                    )
                    if result == "created":
                        created_count += 1
                    elif result == "updated":
                        updated_count += 1
                    else:
                        unchanged_count += 1
                except Exception as exc:
                    failed_count += 1
                    self.log_failure(f"Failed to sync NetBox VM {vm.name}: {exc}", vm)

            return (
                f"NetBox to OpenStack sync complete: "
                f"created={created_count}, "
                f"updated={updated_count}, "
                f"unchanged={unchanged_count}, "
                f"failed={failed_count}, "
                f"dry_run={'yes' if not commit else 'no'}"
            )

    @contextmanager
    def _openvpn_tunnel(self, uploaded_profile):
        if uploaded_profile is None:
            yield None
            return

        with tempfile.TemporaryDirectory(prefix="netbox-openvpn-") as temp_dir:
            profile_path = Path(temp_dir) / "uploaded-profile.ovpn"
            self._write_uploaded_profile(uploaded_profile, profile_path)

            self.log_info("sleeping for 5 minutes to allow for any transient OpenVPN connectivity issues to resolve before syncing")
            time.sleep(300)

            self.log_info(f"Starting OpenVPN tunnel from uploaded profile {getattr(uploaded_profile, 'name', profile_path.name)}")
            proc = self._start_openvpn(profile_path)
            try:
                yield proc
            finally:
                self._stop_openvpn(proc)

    def _write_uploaded_profile(self, uploaded_profile, profile_path):
        if hasattr(uploaded_profile, "chunks"):
            with profile_path.open("wb") as handle:
                for chunk in uploaded_profile.chunks():
                    if isinstance(chunk, str):
                        chunk = chunk.encode()
                    handle.write(chunk)
        else:
            content = uploaded_profile.read()
            if isinstance(content, str):
                content = content.encode()
            profile_path.write_bytes(content)

        profile_path.chmod(0o600)

    def _openvpn_binary(self):
        return (os.environ.get("OPENVPN_BINARY") or "openvpn").strip() or "openvpn"

    def _openvpn_start_timeout(self):
        raw_timeout = (os.environ.get("OPENVPN_START_TIMEOUT") or "90").strip()
        try:
            timeout = int(raw_timeout)
        except (TypeError, ValueError):
            timeout = 90
        return max(timeout, 1)

    def _start_openvpn(self, profile_path):
        command = [
            self._openvpn_binary(),
            "--config",
            str(profile_path),
            "--verb",
            "3",
        ]

        try:
            proc = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                start_new_session=True,
            )
        except FileNotFoundError as exc:
            raise AbortScript(
                f"OpenVPN binary not found: {command[0]}. Install openvpn or set OPENVPN_BINARY to the full path."
            ) from exc
        except Exception as exc:
            raise AbortScript(f"Could not start OpenVPN: {exc}") from exc

        if proc.stdout is None:
            self._stop_openvpn(proc)
            raise AbortScript("OpenVPN did not expose stdout, cannot monitor tunnel startup")

        deadline = time.monotonic() + self._openvpn_start_timeout()
        output = []

        while True:
            if proc.poll() is not None:
                last_output = "\n".join(output[-20:])
                raise AbortScript(
                    "OpenVPN exited before the tunnel was established"
                    + (f". Last output:\n{last_output}" if last_output else "")
                )

            ready, _, _ = select.select([proc.stdout], [], [], 1.0)
            if ready:
                line = proc.stdout.readline()
                if line:
                    line = line.rstrip()
                    output.append(line)
                    self.log_info(f"[openvpn] {line}")

                    if "Initialization Sequence Completed" in line:
                        self.log_success("OpenVPN tunnel established")
                        return proc

                    if "AUTH_FAILED" in line or "Exiting due to fatal error" in line:
                        raise AbortScript(
                            "OpenVPN reported a startup failure"
                            + (f". Last output: {line}" if line else "")
                        )

            if time.monotonic() > deadline:
                last_output = "\n".join(output[-20:])
                self._stop_openvpn(proc)
                raise AbortScript(
                    "Timed out waiting for OpenVPN tunnel to initialize"
                    + (f". Last output:\n{last_output}" if last_output else "")
                )

    def _stop_openvpn(self, proc):
        if proc is None or proc.poll() is not None:
            return

        try:
            proc.terminate()
            proc.wait(timeout=10)
        except Exception:
            try:
                proc.kill()
                proc.wait(timeout=10)
            except Exception:
                pass

    def _sync_vm(self, conn, vm, data, commit):
        server = self._find_server_for_vm(conn, vm)
        desired_name = vm.name
        changes = []

        if server is None:
            if not data.get("allow_create"):
                self.log_warning(
                    f"No matching OpenStack instance found for NetBox VM {vm.name}. "
                    f"Creation is disabled."
                )
                return "unchanged"

            creation_resources = self._resolve_creation_resources(conn, data, vm)
            image = creation_resources["image"]
            flavor = creation_resources["flavor"]
            network = creation_resources["network"]

            change_message = (
                f"Create OpenStack instance for NetBox VM {vm.name} "
                f"using image={image.name}, flavor={flavor.name}, network={network.name}"
            )
            if not commit:
                self.log_info(f"[dry-run] {change_message}")
                return "created"

            server = self._create_server(conn, vm, data, image, flavor, network)
            changes.append("created_instance")
            self._update_vm_openstack_id(vm, server.id, commit=True)
            self.log_success(f"Created OpenStack instance {server.name} for NetBox VM {vm.name}", vm)
            return "created"

        openstack_id = self._get_vm_openstack_id(vm)
        if openstack_id != str(server.id):
            if commit:
                self._update_vm_openstack_id(vm, server.id, commit=True)
                changes.append("saved_openstack_id_to_netbox")
            else:
                self.log_info(
                    f"[dry-run] Would store OpenStack instance UUID {server.id} in NetBox custom field openstack_id for {vm.name}"
                )
                changes.append("save_openstack_id_to_netbox")

        if data.get("allow_rename") and (server.name or "") != desired_name:
            changes.append(f"rename:{server.name}->{desired_name}")
            if commit:
                server = conn.compute.update_server(server, name=desired_name)
                self.log_info(f"Renamed OpenStack instance {server.id} to {desired_name}")

        if data.get("update_metadata"):
            metadata_changes = self._sync_metadata(conn, server, vm, commit)
            changes.extend(metadata_changes)

        if data.get("sync_power_state"):
            power_change = self._sync_power_state(conn, server, vm, commit)
            if power_change:
                changes.append(power_change)

        if not changes:
            self.log_info(f"No changes needed for NetBox VM {vm.name}")
            return "unchanged"

        if not commit:
            self.log_info(f"[dry-run] Would apply to {vm.name}: {', '.join(changes)}")
        else:
            self.log_success(f"Synchronized {vm.name}: {', '.join(changes)}", vm)
        return "updated"

    def _build_openstack_conn_kwargs(self, data, region_name=None, project_id=None, project_name=None):
        conn_kwargs = {
            "auth_url": data["auth_url"].strip(),
            "username": data["username"].strip(),
            "password": data["password"],
            "identity_api_version": 3,
            "verify": bool(data.get("verify", True)),
            "app_name": "NetBox",
            "app_version": "1.0",
        }

        user_domain_name = (data.get("user_domain_name") or "").strip()
        project_domain_name = (data.get("project_domain_name") or "").strip()
        if user_domain_name:
            conn_kwargs["user_domain_name"] = user_domain_name
        if project_domain_name:
            conn_kwargs["project_domain_name"] = project_domain_name
        if project_id:
            conn_kwargs["project_id"] = project_id
        if project_name:
            conn_kwargs["project_name"] = project_name
        if region_name:
            conn_kwargs["region_name"] = region_name

        return conn_kwargs

    def _get_base_connection(self, openstack, data, region_name, connection_cache):
        base_key = ("__base__", region_name or "")
        cached = connection_cache.get(base_key)
        if cached is not None:
            return cached

        conn_kwargs = self._build_openstack_conn_kwargs(data, region_name=region_name)
        try:
            base_conn = openstack.connect(**conn_kwargs)
        except Exception as exc:
            raise AbortScript(f"Could not establish OpenStack session for region {region_name or '<default>'}: {exc}") from exc

        connection_cache[base_key] = base_conn
        return base_conn

    def _resolve_project_resource(self, conn, project_id, project_name, vm_name):
        lookup_error = None

        if project_id:
            try:
                project = conn.identity.get_project(project_id)
            except Exception as exc:
                lookup_error = exc
            else:
                if project is not None:
                    return project

        if project_name:
            try:
                for project in conn.identity.projects():
                    if (getattr(project, "name", None) or "") == project_name:
                        return project
            except Exception as exc:
                lookup_error = exc

        project_ref = project_id or project_name or "<unknown>"
        if lookup_error is not None:
            raise AbortScript(
                f"Could not resolve OpenStack project {project_ref} for VM {vm_name}: {lookup_error}"
            ) from lookup_error

        raise AbortScript(f"Could not resolve OpenStack project {project_ref} for VM {vm_name}")

    def _get_connection_for_vm(self, openstack, data, vm, connection_cache):
        project_id = self._get_vm_cf_value(vm, "openstack_project_id")
        project_name = self._get_vm_cf_value(vm, "openstack_project_name")
        region_name = self._get_vm_cf_value(vm, "openstack_location_region")

        if not project_id and not project_name:
            raise AbortScript(
                f"NetBox VM {vm.name} is missing custom field openstack_project_id or openstack_project_name"
            )

        raw_cache_key = (project_id or "", project_name or "", region_name or "")
        cached = connection_cache.get(raw_cache_key)
        if cached is not None:
            return cached

        base_conn = self._get_base_connection(openstack, data, region_name, connection_cache)
        project = self._resolve_project_resource(base_conn, project_id, project_name, vm.name)
        resolved_project_id = getattr(project, "id", None) or project_id or project_name or ""
        cache_key = (resolved_project_id, region_name or "")
        cached = connection_cache.get(cache_key)
        if cached is not None:
            connection_cache[raw_cache_key] = cached
            return cached

        if hasattr(base_conn, "connect_as_project"):
            try:
                conn = base_conn.connect_as_project(project)
            except Exception as exc:
                raise AbortScript(
                    f"Could not switch OpenStack session to project {resolved_project_id} for VM {vm.name}: {exc}"
                ) from exc
        else:
            conn_kwargs = self._build_openstack_conn_kwargs(
                data,
                region_name=region_name,
                project_id=project_id,
                project_name=project_name,
            )
            try:
                conn = openstack.connect(**conn_kwargs)
            except Exception as exc:
                raise AbortScript(
                    f"Could not authenticate to OpenStack for VM {vm.name}: {exc}"
                ) from exc

        connection_cache[raw_cache_key] = conn
        connection_cache[cache_key] = conn
        return conn

    def _resolve_creation_resources(self, conn, data, vm):
        image_name = (
            self._get_vm_cf_value(vm, "openstack_image")
            or self._get_vm_cf_value(vm, "openstack_image_name")
            or (data.get("image_name") or "").strip()
        )
        network_name = (
            self._get_vm_cf_value(vm, "openstack_network")
            or self._get_vm_cf_value(vm, "openstack_network_name")
            or (data.get("network_name") or "").strip()
        )
        flavor_name = self._get_vm_cf_value(vm, "openstack_flavor")

        missing = []
        if not image_name:
            missing.append("image_name or custom field openstack_image/openstack_image_name")
        if not network_name:
            missing.append("network_name or custom field openstack_network/openstack_network_name")
        if not flavor_name:
            missing.append("custom field openstack_flavor")
        if missing:
            raise AbortScript(
                f"Creation is enabled for VM {vm.name} but required values are missing: {', '.join(missing)}"
            )

        image = conn.image.find_image(image_name)
        if image is None:
            raise AbortScript(f"OpenStack image not found for VM {vm.name}: {image_name}")

        flavor = conn.compute.find_flavor(flavor_name)
        if flavor is None:
            raise AbortScript(f"OpenStack flavor not found for VM {vm.name}: {flavor_name}")

        network = conn.network.find_network(network_name)
        if network is None:
            raise AbortScript(f"OpenStack network not found for VM {vm.name}: {network_name}")

        return {
            "image": image,
            "flavor": flavor,
            "network": network,
        }

    def _find_server_for_vm(self, conn, vm):
        openstack_id = self._get_vm_openstack_id(vm)
        if openstack_id:
            server = conn.compute.find_server(openstack_id, ignore_missing=True)
            if server is not None:
                return server

        serial = (vm.serial or "").strip()
        if serial:
            server = conn.compute.find_server(serial, ignore_missing=True)
            if server is not None:
                return server

        try:
            return conn.compute.find_server(vm.name, ignore_missing=True)
        except Exception:
            return None

    def _create_server(self, conn, vm, data, image, flavor, network):
        create_args = {
            "name": vm.name,
            "image_id": image.id,
            "flavor_id": flavor.id,
            "networks": [{"uuid": network.id}],
        }

        key_name = self._get_vm_cf_value(vm, "key_name")
        if key_name:
            create_args["key_name"] = key_name

        availability_zone = self._get_vm_cf_first(vm, "openstack_availability_zone") or self._get_vm_cf_value(vm, "openstack_location_zone")
        if availability_zone:
            create_args["availability_zone"] = availability_zone

        security_groups = self._get_vm_cf_list(vm, "openstack_security_groups")
        if security_groups:
            create_args["security_groups"] = [{"name": group_name} for group_name in security_groups]

        server = conn.compute.create_server(**create_args)

        if data.get("wait_for_active"):
            server = conn.compute.wait_for_server(
                server,
                status="ACTIVE",
                failures=["ERROR"],
                wait=600,
            )

        if data.get("update_metadata"):
            self._sync_metadata(conn, server, vm, commit=True)

        if data.get("sync_power_state"):
            self._sync_power_state(conn, server, vm, commit=True)

        return server

    def _sync_metadata(self, conn, server, vm, commit):
        desired_metadata = self._desired_metadata(vm)
        current_metadata_resource = conn.compute.get_server_metadata(server)
        current_metadata = getattr(current_metadata_resource, "metadata", {}) or {}

        changed = []
        pending = {}
        for key, value in desired_metadata.items():
            if current_metadata.get(key) != value:
                pending[key] = value
                changed.append(f"metadata:{key}")

        if not pending:
            return []

        if commit:
            conn.compute.set_server_metadata(server, **pending)
        return changed

    def _desired_metadata(self, vm):
        metadata = {
            "netbox_vm_id": str(vm.pk),
            "netbox_vm_name": vm.name,
            "netbox_cluster": vm.cluster.name if vm.cluster else "",
            "netbox_status": str(getattr(vm.status, "value", vm.status)),
        }

        if vm.tenant:
            metadata["netbox_tenant"] = vm.tenant.name
        if vm.role:
            metadata["netbox_role"] = vm.role.name
        if vm.vcpus is not None:
            metadata["netbox_vcpus"] = str(vm.vcpus)
        if vm.memory is not None:
            metadata["netbox_memory_mb"] = str(vm.memory)
        if vm.disk is not None:
            metadata["netbox_disk_mb"] = str(vm.disk)

        for field_name in (
            "hostname",
            "kubespray_groups",
            "openstack_project_id",
            "openstack_project_name",
            "openstack_location_region",
            "openstack_location_zone",
        ):
            value = self._get_vm_cf_value(vm, field_name)
            if value:
                metadata[field_name] = str(value)

        return metadata

    def _sync_power_state(self, conn, server, vm, commit):
        desired = self._desired_server_status(vm)
        actual = str(getattr(server, "status", "")).upper()

        if desired == "ACTIVE" and actual == "SHUTOFF":
            if commit:
                conn.compute.start_server(server)
            return "power:start"

        if desired == "SHUTOFF" and actual == "ACTIVE":
            if commit:
                conn.compute.stop_server(server)
            return "power:stop"

        return None

    def _desired_server_status(self, vm):
        status_value = str(getattr(vm.status, "value", vm.status)).lower()
        if status_value == "offline":
            return "SHUTOFF"
        return "ACTIVE"

    def _get_vm_openstack_id(self, vm):
        return self._get_vm_cf_value(vm, "openstack_id") or (vm.serial or "").strip()

    def _update_vm_openstack_id(self, vm, server_id, commit):
        self._set_vm_cf_value(vm, "openstack_id", str(server_id))
        vm.serial = str(server_id)
        vm.full_clean()
        vm.save()

    def _get_vm_cf_data(self, vm):
        data = getattr(vm, "custom_field_data", None)
        if isinstance(data, dict):
            return data
        data = getattr(vm, "cf", None)
        if isinstance(data, dict):
            return data
        return {}

    def _get_vm_cf_value(self, vm, field_name):
        value = self._get_vm_cf_data(vm).get(field_name)
        if value in (None, "", []):
            return None
        if isinstance(value, list) and len(value) == 1:
            return value[0]
        return value

    def _get_vm_cf_first(self, vm, field_name):
        value = self._get_vm_cf_data(vm).get(field_name)
        if isinstance(value, list):
            return value[0] if value else None
        return value

    def _get_vm_cf_list(self, vm, field_name):
        value = self._get_vm_cf_data(vm).get(field_name)
        if value in (None, ""):
            return []
        if isinstance(value, list):
            return [item for item in value if item not in (None, "")]
        return [value]

    def _set_vm_cf_value(self, vm, field_name, value):
        data = dict(self._get_vm_cf_data(vm))
        data[field_name] = value
        setattr(vm, "custom_field_data", data)
