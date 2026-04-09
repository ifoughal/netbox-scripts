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
                    "vpn_debug",
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
                    "sync_debug",
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
    vpn_debug = BooleanVar(
        required=False,
        default=False,
        description="Log OpenVPN startup output while the tunnel is being established",
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
    sync_debug = BooleanVar(
        required=False,
        default=False,
        description="Log unchanged metadata comparisons as debug output",
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

        with self._openvpn_tunnel(data.get("vpn_profile"), debug=bool(data.get("vpn_debug"))):
            sync_debug = bool(data.get("sync_debug"))
            cluster = data["cluster"]
            tenant = data.get("tenant")
            name_prefix = (data.get("name_prefix") or "").strip()

            queryset = VirtualMachine.objects.filter(cluster=cluster)
            if tenant is not None:
                queryset = queryset.filter(tenant=tenant)
            if name_prefix:
                queryset = queryset.filter(name__startswith=name_prefix)

            nb_vms = list(queryset.order_by("name"))
            if not nb_vms:
                self.log_info("No NetBox VMs matched the selected filters")
                return "No matching NetBox VMs found"

            connection_cache = {}
            created_count = 0
            updated_count = 0
            unchanged_count = 0
            failed_count = 0

            for nb_vm in nb_vms:
                try:
                    result = self._sync_vm(
                        conn=self._get_connection_for_vm(
                            openstack,
                            data,
                            nb_vm,
                            connection_cache
                        ),
                        nb_vm=nb_vm,
                        data=data,
                        commit=commit,
                        sync_debug=sync_debug,
                    )
                    if result == "created":
                        created_count += 1
                    elif result == "updated":
                        updated_count += 1
                    else:
                        unchanged_count += 1
                except Exception as exc:
                    failed_count += 1
                    self.log_failure(f"Failed to sync NetBox VM {nb_vm.name}: {exc}", nb_vm)

            return (
                f"NetBox to OpenStack sync complete: "
                f"created={created_count}, "
                f"updated={updated_count}, "
                f"unchanged={unchanged_count}, "
                f"failed={failed_count}, "
                f"dry_run={'yes' if not commit else 'no'}"
            )

    @contextmanager
    def _openvpn_tunnel(self, uploaded_profile, debug=False):
        if uploaded_profile is None:
            yield None
            return

        with tempfile.TemporaryDirectory(prefix="netbox-openvpn-") as temp_dir:
            profile_path = Path(temp_dir) / "uploaded-profile.ovpn"
            self._write_uploaded_profile(uploaded_profile, profile_path)
            proc = self._start_openvpn(profile_path, debug=debug)
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

    def _start_openvpn(self, profile_path, debug=False):
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
                    if debug:
                        self.log_info(f"[openvpn] {line}")

                    if "Initialization Sequence Completed" in line:
                        if debug:
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

    def _nb_vm_ref(self, nb_vm):
        return f"NetBox VM {nb_vm.name} (id={nb_vm.pk})"

    def _os_server_ref(self, os_server):
        server_name = getattr(os_server, "name", None) or "<unnamed>"
        server_id = getattr(os_server, "id", None) or "<unknown>"
        return f"OpenStack server {server_name} (id={server_id})"

    def _summary_value(self, value):
        if value in (None, ""):
            return "<empty>"
        return str(value)

    def _markdown_cell(self, value):
        return self._summary_value(value).replace("|", "\\|").replace("\n", " ")

    def _change_heading(self, row):
        change_type = row.get("change_type", "change")
        field = row.get("field", "")
        if change_type == "metadata":
            return f"Metadata change: `{field}`"
        if change_type == "identity":
            return "Identity change: `openstack_id`"
        if change_type == "rename":
            return "Rename change: `name`"
        if change_type == "power":
            return "Power state change: `status`"
        if change_type == "create":
            return "Create change: `instance`"
        return f"{change_type.title()} change: `{field}`"

    def _record_change(self, change_rows, nb_vm, os_server, change_type, field, openstack_value, netbox_value, commit, details=""):
        openstack_text = self._summary_value(openstack_value)
        netbox_text = self._summary_value(netbox_value)
        change_rows.append(
            {
                "netbox_vm": self._nb_vm_ref(nb_vm),
                "netbox_vm_id": str(nb_vm.pk),
                "openstack_server": self._os_server_ref(os_server) if os_server is not None else "<missing OpenStack server>",
                "openstack_server_id": str(getattr(os_server, "id", None) or "<missing>"),
                "change_type": change_type,
                "field": field,
                "openstack_value": openstack_text,
                "netbox_value": netbox_text,
                "diff": f"{openstack_text} -> {netbox_text}",
                "details": details,
                "mode": "apply" if commit else "dry-run",
            }
        )

    def _log_change_summary(self, nb_vm, os_server, change_rows, commit):
        if not change_rows:
            return

        summary_target = self._os_server_ref(os_server) if os_server is not None else "<missing OpenStack server>"
        change_order = {
            "create": 0,
            "identity": 1,
            "rename": 2,
            "metadata": 3,
            "power": 4,
        }
        ordered_rows = sorted(
            enumerate(change_rows),
            key=lambda item: (change_order.get(item[1]["change_type"], 99), item[0]),
        )

        self.log_info(
            f"### Change summary for {self._nb_vm_ref(nb_vm)} against {summary_target} "
            f"({len(change_rows)} change{'s' if len(change_rows) != 1 else ''})",
            obj=nb_vm,
        )

        for _, row in ordered_rows:
            field_headers = [
                "VM",
                "Server",
                "Field",
                "OpenStack",
                "NetBox",
                "Diff",
                "Mode",
                "Details",
            ]
            field_line = [
                row["netbox_vm"],
                row["openstack_server"],
                row["field"],
                row["openstack_value"],
                row["netbox_value"],
                row["diff"],
                row["mode"],
                row["details"],
            ]
            lines = [
                f"#### {self._change_heading(row)}",
                "",
                "| " + " | ".join(field_headers) + " |",
                "| " + " | ".join("---" for _ in field_headers) + " |",
                "| " + " | ".join(self._markdown_cell(cell) for cell in field_line) + " |",
            ]
            self.log_info("\n".join(lines), obj=nb_vm)

    def _sync_vm(self, conn, nb_vm, data, commit, sync_debug=False):
        change_rows = []
        os_server = self._find_server_for_vm(conn, nb_vm)
        nb_vm_ref = self._nb_vm_ref(nb_vm)
        desired_name = nb_vm.name

        if os_server is None:
            self.log_info(f"No matching OpenStack server found for {nb_vm_ref}", obj=nb_vm)
            if not data.get("allow_create"):
                self.log_warning(
                    f"No matching OpenStack server found for {nb_vm_ref}. "
                    f"Creation is disabled.",
                    obj=nb_vm,
                )
                return "unchanged"

            creation_resources = self._resolve_creation_resources(conn, data, nb_vm)
            image = creation_resources["image"]
            flavor = creation_resources["flavor"]
            network = creation_resources["network"]
            create_message = (
                f"Create OpenStack server for {nb_vm_ref} "
                f"using image={image.name}, flavor={flavor.name}, network={network.name}"
            )

            if commit:
                self.log_info(create_message, obj=nb_vm)
            else:
                self.log_info(f"[dry-run] {create_message}", obj=nb_vm)
                self._record_change(
                    change_rows,
                    nb_vm,
                    None,
                    "create",
                    "instance",
                    "<missing>",
                    nb_vm.name,
                    commit,
                    details=f"image={image.name}, flavor={flavor.name}, network={network.name}",
                )
                self._log_change_summary(nb_vm, None, change_rows, commit)
                return "created"

            os_server = self._create_server(
                conn,
                nb_vm,
                data,
                image,
                flavor,
                network,
                change_rows=change_rows,
                sync_debug=sync_debug,
            )
            self._record_change(
                change_rows,
                nb_vm,
                os_server,
                "create",
                "instance",
                "<missing>",
                os_server.name or nb_vm.name,
                commit,
                details=f"image={image.name}, flavor={flavor.name}, network={network.name}",
            )
            existing_openstack_id = self._get_vm_openstack_id(nb_vm)
            if existing_openstack_id != str(os_server.id):
                self._record_change(
                    change_rows,
                    nb_vm,
                    os_server,
                    "identity",
                    "openstack_id",
                    existing_openstack_id,
                    os_server.id,
                    commit,
                )
                self._update_vm_openstack_id(nb_vm, os_server.id, commit=True)
            self._log_change_summary(nb_vm, os_server, change_rows, commit)
            return "created"

        os_server_ref = self._os_server_ref(os_server)
        changed = False

        openstack_id = self._get_vm_openstack_id(nb_vm)
        if openstack_id != str(os_server.id):
            changed = True
            self._record_change(
                change_rows,
                nb_vm,
                os_server,
                "identity",
                "openstack_id",
                openstack_id,
                os_server.id,
                commit,
            )
            if commit:
                self._update_vm_openstack_id(nb_vm, os_server.id, commit=True)

        if data.get("allow_rename") and (os_server.name or "") != desired_name:
            changed = True
            self._record_change(
                change_rows,
                nb_vm,
                os_server,
                "rename",
                "name",
                os_server.name or "<empty>",
                desired_name,
                commit,
            )
            if commit:
                os_server = conn.compute.update_server(os_server, name=desired_name)
                os_server_ref = self._os_server_ref(os_server)

        if data.get("update_metadata"):
            changed = self._sync_metadata(
                conn,
                os_server,
                nb_vm,
                commit,
                change_rows=change_rows,
                sync_debug=sync_debug,
            ) or changed

        if data.get("sync_power_state"):
            changed = self._sync_power_state(
                conn,
                os_server,
                nb_vm,
                commit,
                change_rows=change_rows,
            ) or changed

        if change_rows:
            self._log_change_summary(nb_vm, os_server, change_rows, commit)

        if not changed:
            self.log_info(f"No changes needed for {nb_vm_ref} against {os_server_ref}", obj=nb_vm)
            return "unchanged"

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

    def _resolve_project_resource(self, conn, project_id, project_name, nb_vm_name):
        lookup_error = None

        if project_id:
            try:
                os_project = conn.identity.get_project(project_id)
            except Exception as exc:
                lookup_error = exc
            else:
                if os_project is not None:
                    return os_project

        if project_name:
            try:
                for os_project in conn.identity.projects():
                    if (getattr(os_project, "name", None) or "") == project_name:
                        return os_project
            except Exception as exc:
                lookup_error = exc

        project_ref = project_id or project_name or "<unknown>"
        if lookup_error is not None:
            raise AbortScript(
                f"Could not resolve OpenStack project {project_ref} for VM {nb_vm_name}: {lookup_error}"
            ) from lookup_error

        raise AbortScript(f"Could not resolve OpenStack project {project_ref} for VM {nb_vm_name}")

    def _get_connection_for_vm(self, openstack, data, nb_vm, connection_cache):
        project_id = self._get_vm_cf_value(nb_vm, "openstack_project_id")
        project_name = self._get_vm_cf_value(nb_vm, "openstack_project_name")
        region_name = self._get_vm_cf_value(nb_vm, "openstack_location_region")

        if not project_id and not project_name:
            raise AbortScript(
                f"NetBox VM {nb_vm.name} is missing custom field openstack_project_id or openstack_project_name"
            )

        raw_cache_key = (project_id or "", project_name or "", region_name or "")
        cached = connection_cache.get(raw_cache_key)
        if cached is not None:
            return cached

        base_conn = self._get_base_connection(openstack, data, region_name, connection_cache)
        os_project = self._resolve_project_resource(base_conn, project_id, project_name, nb_vm.name)
        resolved_project_id = getattr(os_project, "id", None) or project_id or project_name or ""
        cache_key = (resolved_project_id, region_name or "")
        cached = connection_cache.get(cache_key)
        if cached is not None:
            connection_cache[raw_cache_key] = cached
            return cached

        if hasattr(base_conn, "connect_as_project"):
            try:
                conn = base_conn.connect_as_project(os_project)
            except Exception as exc:
                raise AbortScript(
                    f"Could not switch OpenStack session to project {resolved_project_id} for VM {nb_vm.name}: {exc}"
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
                    f"Could not authenticate to OpenStack for VM {nb_vm.name}: {exc}"
                ) from exc

        connection_cache[raw_cache_key] = conn
        connection_cache[cache_key] = conn
        return conn

    def _resolve_creation_resources(self, conn, data, nb_vm):
        image_name = (
            self._get_vm_cf_value(nb_vm, "openstack_image")
            or self._get_vm_cf_value(nb_vm, "openstack_image_name")
            or (data.get("image_name") or "").strip()
        )
        network_name = (
            self._get_vm_cf_value(nb_vm, "openstack_network")
            or self._get_vm_cf_value(nb_vm, "openstack_network_name")
            or (data.get("network_name") or "").strip()
        )
        flavor_name = self._get_vm_cf_value(nb_vm, "openstack_flavor")

        missing = []
        if not image_name:
            missing.append("image_name or custom field openstack_image/openstack_image_name")
        if not network_name:
            missing.append("network_name or custom field openstack_network/openstack_network_name")
        if not flavor_name:
            missing.append("custom field openstack_flavor")
        if missing:
            raise AbortScript(
                f"Creation is enabled for VM {nb_vm.name} but required values are missing: {', '.join(missing)}"
            )

        image = conn.image.find_image(image_name)
        if image is None:
            raise AbortScript(f"OpenStack image not found for VM {nb_vm.name}: {image_name}")

        flavor = conn.compute.find_flavor(flavor_name)
        if flavor is None:
            raise AbortScript(f"OpenStack flavor not found for VM {nb_vm.name}: {flavor_name}")

        network = conn.network.find_network(network_name)
        if network is None:
            raise AbortScript(f"OpenStack network not found for VM {nb_vm.name}: {network_name}")

        return {
            "image": image,
            "flavor": flavor,
            "network": network,
        }

    def _find_server_for_vm(self, conn, nb_vm):
        openstack_id = self._get_vm_openstack_id(nb_vm)
        if openstack_id:
            os_server = conn.compute.find_server(openstack_id, ignore_missing=True)
            if os_server is not None:
                return os_server

        serial = (nb_vm.serial or "").strip()
        if serial:
            os_server = conn.compute.find_server(serial, ignore_missing=True)
            if os_server is not None:
                return os_server

        try:
            return conn.compute.find_server(nb_vm.name, ignore_missing=True)
        except Exception:
            return None

    def _create_server(self, conn, nb_vm, data, image, flavor, network, change_rows=None, sync_debug=False):
        create_args = {
            "name": nb_vm.name,
            "image_id": image.id,
            "flavor_id": flavor.id,
            "networks": [{"uuid": network.id}],
        }

        key_name = self._get_vm_cf_value(nb_vm, "key_name")
        if key_name:
            create_args["key_name"] = key_name

        availability_zone = self._get_vm_cf_first(nb_vm, "openstack_availability_zone") or self._get_vm_cf_value(nb_vm, "openstack_location_zone")
        if availability_zone:
            create_args["availability_zone"] = availability_zone

        security_groups = self._get_vm_cf_list(nb_vm, "openstack_security_groups")
        if security_groups:
            create_args["security_groups"] = [{"name": group_name} for group_name in security_groups]

        os_server = conn.compute.create_server(**create_args)

        if data.get("wait_for_active"):
            os_server = conn.compute.wait_for_server(
                os_server,
                status="ACTIVE",
                failures=["ERROR"],
                wait=600,
            )

        if data.get("update_metadata"):
            self._sync_metadata(
                conn,
                os_server,
                nb_vm,
                commit=True,
                change_rows=change_rows,
                sync_debug=sync_debug,
            )

        if data.get("sync_power_state"):
            self._sync_power_state(
                conn,
                os_server,
                nb_vm,
                commit=True,
                change_rows=change_rows,
            )

        return os_server

    def _sync_metadata(self, conn, os_server, nb_vm, commit, change_rows=None, sync_debug=False):
        if change_rows is None:
            change_rows = []

        desired_metadata = self._desired_metadata(nb_vm)
        current_metadata_resource = conn.compute.get_server_metadata(os_server)
        current_metadata = getattr(current_metadata_resource, "metadata", {}) or {}

        pending = {}
        for key, value in desired_metadata.items():
            current_value = current_metadata.get(key)
            current_normalized = self._normalize_metadata_value(key, current_value)
            desired_normalized = self._normalize_metadata_value(key, value)

            if current_normalized == desired_normalized:
                if sync_debug:
                    self.log_info(
                        f"[debug] Metadata {key} already matches on {self._os_server_ref(os_server)} "
                        f"for {self._nb_vm_ref(nb_vm)}: {desired_normalized!r}",
                        obj=nb_vm,
                    )
                continue

            pending[key] = desired_normalized
            self._record_change(
                change_rows,
                nb_vm,
                os_server,
                "metadata",
                key,
                current_normalized,
                desired_normalized,
                commit,
            )

        if not pending:
            return False

        if commit:
            conn.compute.set_server_metadata(os_server, **pending)
        return True

    def _normalize_metadata_value(self, field_name, value):
        if value is None or value == "":
            return ""

        if field_name == "kubespray_groups":
            if isinstance(value, (list, tuple, set)):
                raw_values = value
            else:
                raw_values = str(value).split(",")

            normalized_values = [str(entry).strip() for entry in raw_values if str(entry).strip()]
            return ",".join(normalized_values)

        if isinstance(value, (list, tuple, set)):
            normalized_values = [str(entry).strip() for entry in value if str(entry).strip()]
            return ",".join(normalized_values)

        return str(value).strip()

    def _desired_metadata(self, nb_vm):
        metadata = {
            "netbox_vm_id": str(nb_vm.pk),
            "netbox_vm_name": nb_vm.name,
            "netbox_cluster": nb_vm.cluster.name if nb_vm.cluster else "",
            "netbox_status": str(getattr(nb_vm.status, "value", nb_vm.status)),
        }

        if nb_vm.tenant:
            metadata["netbox_tenant"] = nb_vm.tenant.name
        if nb_vm.role:
            metadata["netbox_role"] = nb_vm.role.name
        if nb_vm.vcpus is not None:
            metadata["netbox_vcpus"] = str(nb_vm.vcpus)
        if nb_vm.memory is not None:
            metadata["netbox_memory_mb"] = str(nb_vm.memory)
        if nb_vm.disk is not None:
            metadata["netbox_disk_mb"] = str(nb_vm.disk)

        for field_name in (
            "hostname",
            "kubespray_groups",
            "openstack_project_id",
            "openstack_project_name",
            "openstack_location_region",
            "openstack_location_zone",
        ):
            value = self._get_vm_cf_value(nb_vm, field_name)
            if value:
                metadata[field_name] = str(value)

        return metadata

    def _sync_power_state(self, conn, os_server, nb_vm, commit, change_rows=None):
        if change_rows is None:
            change_rows = []

        desired = self._desired_server_status(nb_vm)
        actual = str(getattr(os_server, "status", "")).upper()

        if desired == "ACTIVE" and actual == "SHUTOFF":
            self._record_change(
                change_rows,
                nb_vm,
                os_server,
                "power",
                "status",
                actual,
                desired,
                commit,
                details="start_server",
            )
            if commit:
                conn.compute.start_server(os_server)
            return True

        if desired == "SHUTOFF" and actual == "ACTIVE":
            self._record_change(
                change_rows,
                nb_vm,
                os_server,
                "power",
                "status",
                actual,
                desired,
                commit,
                details="stop_server",
            )
            if commit:
                conn.compute.stop_server(os_server)
            return True

        return False

    def _desired_server_status(self, nb_vm):
        status_value = str(getattr(nb_vm.status, "value", nb_vm.status)).lower()
        if status_value == "offline":
            return "SHUTOFF"
        return "ACTIVE"

    def _get_vm_openstack_id(self, nb_vm):
        return self._get_vm_cf_value(nb_vm, "openstack_id") or (nb_vm.serial or "").strip()

    def _update_vm_openstack_id(self, nb_vm, server_id, commit):
        self._set_vm_cf_value(nb_vm, "openstack_id", str(server_id))
        nb_vm.serial = str(server_id)
        nb_vm.full_clean()
        nb_vm.save()

    def _get_vm_cf_data(self, nb_vm):
        data = getattr(nb_vm, "custom_field_data", None)
        if isinstance(data, dict):
            return data
        data = getattr(nb_vm, "cf", None)
        if isinstance(data, dict):
            return data
        return {}

    def _get_vm_cf_value(self, nb_vm, field_name):
        value = self._get_vm_cf_data(nb_vm).get(field_name)
        if value in (None, "", []):
            return None
        if isinstance(value, list) and len(value) == 1:
            return value[0]
        return value

    def _get_vm_cf_first(self, nb_vm, field_name):
        value = self._get_vm_cf_data(nb_vm).get(field_name)
        if isinstance(value, list):
            return value[0] if value else None
        return value

    def _get_vm_cf_list(self, nb_vm, field_name):
        value = self._get_vm_cf_data(nb_vm).get(field_name)
        if value in (None, ""):
            return []
        if isinstance(value, list):
            return [item for item in value if item not in (None, "")]
        return [value]

    def _set_vm_cf_value(self, nb_vm, field_name, value):
        data = dict(self._get_vm_cf_data(nb_vm))
        data[field_name] = value
        setattr(nb_vm, "custom_field_data", data)
