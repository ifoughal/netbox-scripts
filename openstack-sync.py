"""Synchronize NetBox virtual machines with OpenStack instances.

The script resolves a matching OpenStack server for each selected NetBox VM,
compares identity, metadata, and power state, then records the differences in
the NetBox job log before optionally applying changes.
"""

import json
import html
from contextlib import contextmanager
from decimal import Decimal
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


# Metadata fields that are mirrored into OpenStack but omitted from the summary
# because they are derived bookkeeping values rather than reconciliation signals.
METADATA_SUMMARY_SKIP_KEYS = frozenset({
    "netbox_vm_id",
    "netbox_vm_name",
    "netbox_status",
})

# NetBox-to-OpenStack metadata fields that the script builds, compares, and
# conditionally pushes into the OpenStack server metadata. The final boolean in
# each entry controls whether an empty value should be written explicitly.
METADATA_FIELDS_TO_SYNC = (
    ("netbox_vm_id", "attr", "pk", False),
    ("netbox_vm_name", "attr", "name", False),
    ("netbox_cluster", "attr", "cluster.name", True),
    ("netbox_status", "attr", "status.value", False),
    ("netbox_role", "attr", "role.name", False),
    ("kubespray_groups", "cf", "kubespray_groups", False),
    ("ssh_user", "cf", "ssh_user", False),
    ("use_access_ip", "cf", "user_access_ip", False),
)

# Field groups used by the comparison summary. `to_sync` values are sourced
# from NetBox and should be treated as sync-tracked configuration, while
# `report_only` values are checked for drift but are not written back.
FIELD_GROUPS = {
    "report_only": (
        {
            "field": "netbox_tenant",
            "openstack_path": "project_name",
            "source_kind": "attr",
            "source_name": "tenant.name",
        },
        {
            "field": "netbox_vcpus",
            "openstack_path": "flavor.vcpus",
            "source_kind": "attr",
            "source_name": "vcpus",
            "compare_kind": "size",
        },
        {
            "field": "netbox_memory_mb",
            "openstack_path": "flavor.ram",
            "source_kind": "attr",
            "source_name": "memory",
            "compare_kind": "size",
        },
        {
            "field": "netbox_disk_mb",
            "openstack_path": "flavor.total_disk_mb",
            "source_kind": "attr",
            "source_name": "disk",
            "compare_kind": "size",
        },
    ),
    "to_sync": (
        {
            "field": "flavor",
            "openstack_path": "flavor.name",
            "source_kind": "cf",
            "source_name": "openstack_flavor",
        },
        {
            "field": "availability_zone",
            "openstack_path": "availability_zone",
            "source_kind": "cf_first",
            "source_name": ("openstack_availability_zone", "openstack_location_zone"),
        },
        {
            "field": "key_name",
            "openstack_path": "key_name",
            "source_kind": "cf",
            "source_name": "key_name",
        },
        {
            "field": "security_groups",
            "openstack_path": "security_groups",
            "source_kind": "cf_list",
            "source_name": "openstack_security_groups",
        },
    ),
}


class OpenStackInstance:
    """Normalized view of an OpenStack server used by the sync workflow."""

    def __init__(self, conn, resource):
        """Load a raw OpenStack resource and cache the normalized fields."""
        self.conn = conn
        self.resource = None
        self.raw = {}
        self.load(resource)

    @staticmethod
    def _nb_vm_ref(nb_vm):
        """Return a stable human-readable label for a NetBox VM."""
        return f"NetBox VM {nb_vm.name} (id={nb_vm.pk})"

    @staticmethod
    def _os_server_ref(resource):
        """Return a stable human-readable label for an OpenStack server."""
        server_name = getattr(resource, "name", None) or "<unnamed>"
        server_id = getattr(resource, "id", None) or "<unknown>"
        return f"OpenStack server {server_name} (id={server_id})"

    @staticmethod
    def _get_attr(obj, *paths):
        """Return the first non-empty attribute or dictionary path that exists."""
        for path in paths:
            current = obj
            for part in path.split("."):
                if current is None:
                    break
                if isinstance(current, dict):
                    current = current.get(part)
                else:
                    current = getattr(current, part, None)
            if current not in (None, "", []):
                return current
        return None

    @staticmethod
    def _metadata_scalar(value):
        """Flatten a metadata value into a comparable scalar string."""
        if value in (None, ""):
            return ""

        if isinstance(value, dict):
            for key in ("name", "value", "label", "display", "slug", "id"):
                nested = value.get(key)
                if nested not in (None, ""):
                    return str(nested).strip()
            return json.dumps(value, sort_keys=True)

        if isinstance(value, (list, tuple, set)):
            parts = [OpenStackInstance._metadata_scalar(item) for item in value]
            parts = [item for item in parts if item]
            return ",".join(parts)

        if hasattr(value, "name") and getattr(value, "name", None) not in (None, ""):
            return str(getattr(value, "name")).strip()

        return str(value).strip()

    @staticmethod
    def _numeric_scalar(value):
        """Normalize numeric values so equivalent quantities compare equally."""
        if value in (None, ""):
            return ""

        try:
            decimal_value = Decimal(str(value).strip())
        except Exception:
            return OpenStackInstance._metadata_scalar(value)

        if decimal_value == decimal_value.to_integral():
            return str(int(decimal_value))

        normalized = format(decimal_value.normalize(), "f").rstrip("0").rstrip(".")
        return normalized or "0"

    @staticmethod
    def _flavor_total_disk_mb(disk_gb, ephemeral_gb):
        """Return the flavor's total local disk allocation in megabytes."""
        total_gb = Decimal("0")
        for value in (disk_gb, ephemeral_gb):
            if value in (None, ""):
                continue
            try:
                total_gb += Decimal(str(value).strip())
            except Exception:
                continue
        return int(total_gb * Decimal("1024"))

    @staticmethod
    def _resource_data(resource):
        """Convert an OpenStack SDK resource into a plain dictionary."""
        if resource is None:
            return {}
        if isinstance(resource, dict):
            return resource
        if hasattr(resource, "to_dict"):
            try:
                data = resource.to_dict()
                if isinstance(data, dict):
                    return data
            except Exception:
                pass
        return {}

    @staticmethod
    def _security_group_names(groups):
        """Normalize security group objects or names into sorted unique names."""
        if groups in (None, ""):
            return []

        names = []
        sequence = groups if isinstance(groups, (list, tuple, set)) else [groups]
        for group in sequence:
            name = OpenStackInstance._metadata_scalar(
                OpenStackInstance._get_attr(group, "name") if not isinstance(group, str) else group
            )
            if name:
                names.append(name)

        deduped = []
        seen = set()
        for name in sorted(names, key=str.lower):
            normalized = name.lower()
            if normalized in seen:
                continue
            seen.add(normalized)
            deduped.append(name)
        return deduped

    @staticmethod
    def _vm_cf_data(nb_vm):
        """Return the NetBox VM custom field dictionary regardless of API shape."""
        data = getattr(nb_vm, "custom_field_data", None)
        if isinstance(data, dict):
            return data
        data = getattr(nb_vm, "cf", None)
        if isinstance(data, dict):
            return data
        return {}

    @staticmethod
    def _vm_cf_value(nb_vm, field_name):
        """Return a normalized custom field value from a NetBox VM."""
        value = OpenStackInstance._vm_cf_data(nb_vm).get(field_name)
        if value in (None, "", []):
            return None
        if isinstance(value, list) and len(value) == 1:
            return value[0]
        return value

    @classmethod
    def retrieve(cls, conn, nb_vm, log_debug=None):
        """Look up and wrap the matching OpenStack server for a NetBox VM."""
        resource = cls._find_resource_for_vm(conn, nb_vm, log_debug=log_debug)
        if resource is None:
            return None
        return cls(conn, resource)

    @classmethod
    def _find_resource_for_vm(cls, conn, nb_vm, log_debug=None):
        """Find the best matching OpenStack server using IDs, serial, then name."""
        openstack_id = cls._vm_cf_value(nb_vm, "openstack_id") or (nb_vm.serial or "").strip()
        if openstack_id:
            os_server = conn.compute.find_server(openstack_id, ignore_missing=True)
            if log_debug is not None:
                log_debug(
                    f"Looking up OpenStack server by openstack_id={openstack_id!r} "
                    f"for {cls._nb_vm_ref(nb_vm)}: "
                    f"{'found ' + cls._os_server_ref(os_server) if os_server else 'not found'}",
                    obj=nb_vm,
                )
            if os_server is not None:
                return os_server

        serial = (nb_vm.serial or "").strip()
        if serial:
            os_server = conn.compute.find_server(serial, ignore_missing=True)
            if log_debug is not None:
                log_debug(
                    f"Looking up OpenStack server by serial={serial!r} "
                    f"for {cls._nb_vm_ref(nb_vm)}: "
                    f"{'found ' + cls._os_server_ref(os_server) if os_server else 'not found'}",
                    obj=nb_vm,
                )
            if os_server is not None:
                return os_server

        try:
            os_server = conn.compute.find_server(nb_vm.name, ignore_missing=True)
        except Exception:
            os_server = None

        if log_debug is not None:
            log_debug(
                f"Looking up OpenStack server by name={nb_vm.name!r} "
                f"for {cls._nb_vm_ref(nb_vm)}: "
                f"{'found ' + cls._os_server_ref(os_server) if os_server else 'not found'}",
                obj=nb_vm,
            )

        return os_server

    @classmethod
    def create(
        cls,
        conn,
        nb_vm,
        image,
        flavor,
        network,
        key_name=None,
        availability_zone=None,
        security_groups=None,
        metadata=None,
        wait_for_active=True,
    ):
        """Create a new OpenStack server from the supplied NetBox VM details."""
        create_args = {
            "name": nb_vm.name,
            "image_id": image.id,
            "flavor_id": flavor.id,
            "networks": [{"uuid": network.id}],
        }

        if key_name:
            create_args["key_name"] = key_name
        if availability_zone:
            create_args["availability_zone"] = availability_zone
        if security_groups:
            create_args["security_groups"] = [{"name": group_name} for group_name in security_groups]
        if metadata:
            create_args["metadata"] = metadata

        os_server = conn.compute.create_server(**create_args)
        if wait_for_active:
            os_server = conn.compute.wait_for_server(
                os_server,
                status="ACTIVE",
                failures=["ERROR"],
                wait=600,
            )

        return cls(conn, os_server)

    @classmethod
    def desired_metadata(cls, nb_vm):
        """Build the OpenStack metadata payload that should mirror the NetBox VM."""
        metadata = {}
        for key, source_kind, source_name, keep_empty in METADATA_FIELDS_TO_SYNC:
            value = cls._resolve_netbox_source_value(nb_vm, key, source_kind, source_name, keep_empty=keep_empty)
            if value is None:
                continue
            metadata[key] = value

        return metadata

    @classmethod
    def desired_server_status(cls, nb_vm):
        """Translate the NetBox VM status into the target OpenStack power state."""
        status_value = str(getattr(nb_vm.status, "value", nb_vm.status)).lower()
        if status_value == "offline":
            return "SHUTOFF"
        return "ACTIVE"

    def load(self, resource):
        """Populate all cached fields from a raw OpenStack resource."""
        self.resource = resource
        self.raw = self._resource_data(resource)

        # Read the fields we compare and log so later code can work with strings.
        self.id = self._metadata_scalar(self._get_attr(self.raw, "id"))
        self.name = self._metadata_scalar(self._get_attr(self.raw, "name"))
        self.status = self._metadata_scalar(self._get_attr(self.raw, "status"))
        self.project_id = self._metadata_scalar(
            self._get_attr(
                self.raw,
                "project_id",
                "tenant_id",
                "location.project.id",
                "location.project_id",
                "project.id",
            )
        )
        self.project_name = self._metadata_scalar(
            self._get_attr(self.raw, "location.project.name", "project.name")
        )
        self.project_domain_id = self._metadata_scalar(
            self._get_attr(self.raw, "location.project.domain_id", "project.domain_id")
        )
        self.availability_zone = self._metadata_scalar(
            self._get_attr(self.raw, "availability_zone", "OS-EXT-AZ:availability_zone")
        )
        self.host = self._metadata_scalar(self._get_attr(self.raw, "host"))
        self.host_id = self._metadata_scalar(self._get_attr(self.raw, "host_id", "hostId"))
        self.hostname = self._metadata_scalar(self._get_attr(self.raw, "hostname", "OS-EXT-SRV-ATTR:hostname"))
        self.hypervisor_hostname = self._metadata_scalar(self._get_attr(self.raw, "hypervisor_hostname"))
        self.key_name = self._metadata_scalar(self._get_attr(self.raw, "key_name"))
        self.launched_at = self._metadata_scalar(self._get_attr(self.raw, "launched_at", "OS-SRV-USG:launched_at"))
        self.created_at = self._metadata_scalar(self._get_attr(self.raw, "created", "created_at"))
        self.updated_at = self._metadata_scalar(self._get_attr(self.raw, "updated", "updated_at"))
        self.terminated_at = self._metadata_scalar(self._get_attr(self.raw, "OS-SRV-USG:terminated_at", "terminated_at"))
        self.access_ipv4 = self._metadata_scalar(self._get_attr(self.raw, "accessIPv4", "access_ipv4"))
        self.access_ipv6 = self._metadata_scalar(self._get_attr(self.raw, "accessIPv6", "access_ipv6"))
        self.description = self._metadata_scalar(self._get_attr(self.raw, "description"))
        self.progress = self._metadata_scalar(self._get_attr(self.raw, "progress"))
        self.config_drive = self._metadata_scalar(self._get_attr(self.raw, "config_drive"))
        self.locked = self._metadata_scalar(self._get_attr(self.raw, "locked"))
        self.locked_reason = self._metadata_scalar(self._get_attr(self.raw, "locked_reason"))
        self.task_state = self._metadata_scalar(self._get_attr(self.raw, "OS-EXT-STS:task_state", "task_state"))
        self.vm_state = self._metadata_scalar(self._get_attr(self.raw, "OS-EXT-STS:vm_state", "vm_state"))
        self.power_state = self._metadata_scalar(self._get_attr(self.raw, "OS-EXT-STS:power_state", "power_state"))
        self.addresses = self._get_attr(self.raw, "addresses") or {}
        self.server_groups = self._get_attr(self.raw, "server_groups") or []
        self.tags = self._get_attr(self.raw, "tags") or []

        flavor = self._get_attr(self.raw, "flavor") or {}
        self.flavor = {
            "id": self._metadata_scalar(self._get_attr(flavor, "id")),
            "name": self._metadata_scalar(self._get_attr(flavor, "original_name", "name", "id")),
            "vcpus": self._get_attr(flavor, "vcpus"),
            "ram": self._get_attr(flavor, "ram"),
            "disk": self._get_attr(flavor, "disk"),
            "ephemeral": self._get_attr(flavor, "ephemeral"),
            "swap": self._get_attr(flavor, "swap"),
            "total_disk_mb": self._flavor_total_disk_mb(self._get_attr(flavor, "disk"), self._get_attr(flavor, "ephemeral")),
            "extra_specs": self._get_attr(flavor, "extra_specs") or {},
        }
        self.flavor_id = self.flavor["id"]
        self.flavor_name = self.flavor["name"]

        location = self._get_attr(self.raw, "location") or {}
        self.location = {
            "cloud": self._metadata_scalar(self._get_attr(location, "cloud")),
            "region": self._metadata_scalar(self._get_attr(location, "region_name", "region")),
            "zone": self._metadata_scalar(self._get_attr(location, "zone")),
        }
        self.location_cloud = self.location["cloud"]
        self.location_region = self.location["region"]
        self.location_zone = self.location["zone"]

        self.metadata = dict(self._get_attr(self.raw, "metadata") or {})
        self.kubespray_groups = self._normalize_metadata_value("kubespray_groups", self.metadata.get("kubespray_groups"))
        self.ssh_user = self._metadata_scalar(self.metadata.get("ssh_user"))
        self.use_access_ip = self._metadata_scalar(self.metadata.get("use_access_ip"))
        self.security_groups = self._security_group_names(self._get_attr(self.raw, "security_groups"))

        self.openstack_id = self.id
        return self

    def refresh(self):
        """Reload the OpenStack resource if we still have a server ID."""
        if self.id:
            resource = self.conn.compute.get_server(self.id)
            if resource is not None:
                self.load(resource)
        return self

    def ref(self):
        """Return the canonical server label used in log output."""
        return self._os_server_ref(self)

    def snapshot(self):
        """Return a JSON-serializable snapshot of the normalized server state."""
        return {
            "id": self.id,
            "name": self.name,
            "status": self.status,
            "project_id": self.project_id,
            "project_name": self.project_name,
            "project_domain_id": self.project_domain_id,
            "availability_zone": self.availability_zone,
            "host": self.host,
            "host_id": self.host_id,
            "hostname": self.hostname,
            "hypervisor_hostname": self.hypervisor_hostname,
            "key_name": self.key_name,
            "launched_at": self.launched_at,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "terminated_at": self.terminated_at,
            "access_ipv4": self.access_ipv4,
            "access_ipv6": self.access_ipv6,
            "description": self.description,
            "progress": self.progress,
            "config_drive": self.config_drive,
            "locked": self.locked,
            "locked_reason": self.locked_reason,
            "task_state": self.task_state,
            "vm_state": self.vm_state,
            "power_state": self.power_state,
            "location": dict(self.location),
            "flavor": dict(self.flavor),
            "security_groups": list(self.security_groups),
            "server_groups": list(self.server_groups) if isinstance(self.server_groups, (list, tuple, set)) else self.server_groups,
            "tags": list(self.tags) if isinstance(self.tags, (list, tuple, set)) else self.tags,
            "addresses": self.addresses,
            "metadata": dict(self.metadata),
            "kubespray_groups": self.kubespray_groups,
            "ssh_user": self.ssh_user,
            "use_access_ip": self.use_access_ip,
        }

    def log_snapshot(self, log_debug, nb_vm):
        """Emit the current server snapshot to the debug logger."""
        if log_debug is None:
            return
        log_debug(
            f"Parsed OpenStack resource snapshot for {self.ref()} and {self._nb_vm_ref(nb_vm)}:\n"
            f"```json\n{json.dumps(self.snapshot(), indent=2, sort_keys=True)}\n```",
            obj=nb_vm,
        )

    @classmethod
    def _normalize_metadata_value(cls, field_name, value):
        """Normalize metadata values so equality checks are stable."""
        if value is None or value == "":
            return ""

        if field_name == "kubespray_groups":
            if isinstance(value, (list, tuple, set)):
                raw_values = value
            else:
                raw_values = str(value).split(",")

            normalized_values = [str(entry).strip() for entry in raw_values if str(entry).strip()]
            return ",".join(normalized_values)

        return cls._metadata_scalar(value)

    @classmethod
    def _resolve_netbox_source_value(cls, nb_vm, source_key, source_kind, source_name, keep_empty=False):
        """Resolve a desired NetBox value from the configured sync spec."""
        if source_kind == "attr":
            value = cls._get_attr(nb_vm, source_name)
            if value in (None, "", []):
                if keep_empty:
                    return ""
                return None
            return cls._metadata_scalar(value)

        if source_kind == "cf_first":
            candidate_names = source_name if isinstance(source_name, (list, tuple, set)) else [source_name]
            for candidate in candidate_names:
                value = cls._vm_cf_value(nb_vm, candidate)
                if value not in (None, "", []):
                    return cls._metadata_scalar(value)
            if keep_empty:
                return ""
            return None

        if source_kind == "cf_list":
            value = cls._vm_cf_data(nb_vm).get(source_name)
            if value in (None, ""):
                if keep_empty:
                    return []
                return None
            if isinstance(value, list):
                return [item for item in value if item not in (None, "")]
            return [value]

        if source_kind == "cf":
            value = cls._vm_cf_value(nb_vm, source_name)
            if value in (None, "", []):
                if keep_empty:
                    return ""
                return None
            if source_key == "kubespray_groups":
                return cls._normalize_metadata_value(source_key, value)
            return cls._metadata_scalar(value)

        raise ValueError(f"Unsupported metadata source kind: {source_kind}")

    @classmethod
    def _normalize_compare_value(cls, field_name, value, compare_kind=None):
        """Normalize comparison values so OpenStack and NetBox align."""
        if compare_kind == "size":
            return cls._numeric_scalar(value)
        if field_name == "security_groups":
            return ",".join(cls._security_group_names(value))
        return cls._metadata_scalar(value)

    def compare_field_groups(self, nb_vm, commit, change_rows=None, record_change=None, sync_debug=False, log_debug=None):
        """Compare sync-tracked and report-only OpenStack fields against NetBox."""
        if change_rows is None:
            change_rows = []

        drift_found = False
        for group_name, field_specs in FIELD_GROUPS.items():
            mode = "sync" if group_name == "to_sync" else "report"

            for spec in field_specs:
                field_name = spec["field"]
                desired_value = self._resolve_netbox_source_value(
                    nb_vm,
                    field_name,
                    spec["source_kind"],
                    spec["source_name"],
                )
                if desired_value in (None, "", []):
                    continue

                current_value = self._get_attr(self.__dict__, spec["openstack_path"])
                compare_kind = spec.get("compare_kind")
                current_normalized = self._normalize_compare_value(field_name, current_value, compare_kind)
                desired_normalized = self._normalize_compare_value(field_name, desired_value, compare_kind)
                change_type = group_name if current_normalized != desired_normalized else "match"

                if current_normalized == desired_normalized:
                    if record_change is not None and nb_vm is not None:
                        record_change(
                            change_rows,
                            nb_vm,
                            self,
                            change_type,
                            field_name,
                            current_normalized,
                            desired_normalized,
                            commit,
                            details="matched",
                            state="matched",
                            mode="match",
                            comparison_group=group_name,
                        )
                    if sync_debug and log_debug is not None and nb_vm is not None:
                        log_debug(
                            f"OpenStack field {field_name} already matches on {self.ref()} for {self._nb_vm_ref(nb_vm)}: {desired_normalized!r}",
                            obj=nb_vm,
                        )
                    continue

                drift_found = True
                if record_change is not None and nb_vm is not None:
                    record_change(
                        change_rows,
                        nb_vm,
                        self,
                        change_type,
                        field_name,
                        current_normalized,
                        desired_normalized,
                        commit,
                        details=group_name,
                        mode=mode,
                        comparison_group=group_name,
                    )
                if sync_debug and log_debug is not None and nb_vm is not None:
                    log_debug(
                        f"OpenStack field {field_name} differs on {self.ref()} for {self._nb_vm_ref(nb_vm)}: "
                        f"{current_normalized!r} != {desired_normalized!r}",
                        obj=nb_vm,
                    )

        return drift_found

    def update_name(self, desired_name, commit, change_rows=None, record_change=None, nb_vm=None):
        """Rename the OpenStack server when the NetBox VM name differs."""
        if change_rows is None:
            change_rows = []
        current_name = self.name or ""
        if current_name == desired_name:
            # Even matching values are recorded so the summary can show that the
            # field was checked and found to be in sync.
            if record_change is not None and nb_vm is not None:
                record_change(
                    change_rows,
                    nb_vm,
                    self,
                    "rename",
                    "name",
                    current_name or "<empty>",
                    desired_name,
                    commit,
                    details="matched",
                    state="matched",
                )
            return False

        if commit:
            updated = self.conn.compute.update_server(self.resource, name=desired_name)
            if updated is not None:
                self.load(updated)
            else:
                self.name = desired_name
                if isinstance(self.raw, dict):
                    self.raw["name"] = desired_name
                if isinstance(self.resource, dict):
                    self.resource["name"] = desired_name
        else:
            self.name = desired_name

        return True

    def sync_metadata(self, desired_metadata, commit, change_rows=None, record_change=None, nb_vm=None, sync_debug=False, log_debug=None):
        """Compare, report, and optionally merge the NetBox metadata payload."""
        if change_rows is None:
            change_rows = []
        current_metadata = dict(self.metadata)
        summary_skip_keys = METADATA_SUMMARY_SKIP_KEYS

        if sync_debug and log_debug is not None and nb_vm is not None:
            # Dump the raw metadata and normalized snapshot together when
            # debugging so a mismatch can be traced quickly.
            log_debug(
                f"Retrieved metadata for {self.ref()} and {self._nb_vm_ref(nb_vm)}:\n"
                f"```json\n{json.dumps(current_metadata, indent=2, sort_keys=True)}\n```",
                obj=nb_vm,
            )
            self.log_snapshot(log_debug, nb_vm)

        pending = {}
        for key, value in desired_metadata.items():
            # Compare each desired key independently so the summary can show
            # which fields were already aligned and which still need updates.
            current_value = current_metadata.get(key)
            current_normalized = self._normalize_metadata_value(key, current_value)
            desired_normalized = self._normalize_metadata_value(key, value)

            if current_normalized == desired_normalized:
                if key not in summary_skip_keys and record_change is not None and nb_vm is not None:
                    record_change(
                        change_rows,
                        nb_vm,
                        self,
                        "metadata",
                        key,
                        current_normalized,
                        desired_normalized,
                        commit,
                        details="matched",
                        state="matched",
                    )
                if key not in summary_skip_keys and sync_debug and log_debug is not None and nb_vm is not None:
                    log_debug(
                        f"Metadata {key} already matches on {self.ref()} for {self._nb_vm_ref(nb_vm)}: {desired_normalized!r}",
                        obj=nb_vm,
                    )
                continue

            pending[key] = desired_normalized
            if key not in summary_skip_keys and record_change is not None and nb_vm is not None:
                record_change(
                    change_rows,
                    nb_vm,
                    self,
                    "metadata",
                    key,
                    current_normalized,
                    desired_normalized,
                    commit,
                )

        if not pending:
            # No updates are needed, but the matched rows remain in the summary.
            return False

        if commit:
            # Merge only the changed keys when we are in apply mode.
            merged_metadata = dict(current_metadata)
            merged_metadata.update(pending)
            self.conn.compute.set_server_metadata(self.resource, **merged_metadata)
            self.metadata = merged_metadata
            self.kubespray_groups = self._normalize_metadata_value("kubespray_groups", merged_metadata.get("kubespray_groups"))
            self.ssh_user = self._metadata_scalar(merged_metadata.get("ssh_user"))
            self.use_access_ip = self._metadata_scalar(merged_metadata.get("use_access_ip"))
            if isinstance(self.raw, dict):
                self.raw["metadata"] = merged_metadata
            if isinstance(self.resource, dict):
                self.resource["metadata"] = merged_metadata

        return True

    def sync_power_state(self, desired_status, commit, change_rows=None, record_change=None, nb_vm=None):
        """Map NetBox status to OpenStack power state and record the result."""
        if change_rows is None:
            change_rows = []
        actual = self._metadata_scalar(self.status).upper()

        if desired_status == "ACTIVE" and actual == "SHUTOFF":
            # NetBox wants the VM online, so request a start on OpenStack.
            if record_change is not None and nb_vm is not None:
                record_change(
                    change_rows,
                    nb_vm,
                    self,
                    "power",
                    "status",
                    actual,
                    desired_status,
                    commit,
                    details="start_server",
                )
            if commit:
                self.conn.compute.start_server(self.resource)
                self.status = desired_status
                if isinstance(self.raw, dict):
                    self.raw["status"] = desired_status
            return True

        if desired_status == "SHUTOFF" and actual == "ACTIVE":
            # NetBox wants the VM offline, so request a stop on OpenStack.
            if record_change is not None and nb_vm is not None:
                record_change(
                    change_rows,
                    nb_vm,
                    self,
                    "power",
                    "status",
                    actual,
                    desired_status,
                    commit,
                    details="stop_server",
                )
            if commit:
                self.conn.compute.stop_server(self.resource)
                self.status = desired_status
                if isinstance(self.raw, dict):
                    self.raw["status"] = desired_status
            return True

        # Record the aligned state so the summary shows the power check too.
        if desired_status == actual and record_change is not None and nb_vm is not None:
            record_change(
                change_rows,
                nb_vm,
                self,
                "power",
                "status",
                actual,
                desired_status,
                commit,
                details="matched",
                state="matched",
            )

        return False


class SyncNetBoxVMsToOpenStack(Script):
    """NetBox script that reconciles selected virtual machines into OpenStack."""

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
        """Sync every selected NetBox VM and return a compact job summary."""
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
            # Build the NetBox queryset from the selected scope before iterating.
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
            # Track the job outcome buckets so the final return string is useful.
            created_count = 0
            updated_count = 0
            unchanged_count = 0
            failed_count = 0

            # Each VM gets reconciled independently so a failure does not stop the batch.
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
        """Bring up a temporary OpenVPN tunnel around the sync run."""
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
        """Write the uploaded VPN profile to disk with restrictive permissions."""
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
        """Return the OpenVPN executable to use for tunnel startup."""
        return (os.environ.get("OPENVPN_BINARY") or "openvpn").strip() or "openvpn"

    def _openvpn_start_timeout(self):
        """Return the number of seconds to wait for the VPN tunnel to initialize."""
        raw_timeout = (os.environ.get("OPENVPN_START_TIMEOUT") or "90").strip()
        try:
            timeout = int(raw_timeout)
        except (TypeError, ValueError):
            timeout = 90
        return max(timeout, 1)

    def _start_openvpn(self, profile_path, debug=False):
        """Launch OpenVPN and wait until the tunnel reports readiness."""
        command = [
            self._openvpn_binary(),
            "--config",
            str(profile_path),
            "--verb",
            "3",
        ]

        # Keep stdout attached so the script can surface startup failures.
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

            # Bail out if the tunnel never reaches a ready state.
            if time.monotonic() > deadline:
                last_output = "\n".join(output[-20:])
                self._stop_openvpn(proc)
                raise AbortScript(
                    "Timed out waiting for OpenVPN tunnel to initialize"
                    + (f". Last output:\n{last_output}" if last_output else "")
                )

    def _stop_openvpn(self, proc):
        """Terminate a running OpenVPN process, escalating to kill if needed."""
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
        """Return a human-readable label for a NetBox VM."""
        return f"NetBox VM {nb_vm.name} (id={nb_vm.pk})"

    def _os_server_ref(self, os_server):
        """Return a human-readable label for an OpenStack server."""
        server_name = getattr(os_server, "name", None) or "<unnamed>"
        server_id = getattr(os_server, "id", None) or "<unknown>"
        return f"OpenStack server {server_name} (id={server_id})"

    def _summary_value(self, value):
        """Format a comparison value for inclusion in the summary table."""
        if value in (None, ""):
            return "<empty>"
        return str(value)

    def _markdown_cell(self, value):
        """Escape a summary value so it remains valid inside a Markdown table."""
        return html.escape(self._summary_value(value), quote=False).replace("|", "\\|").replace("\n", " ")

    def _record_change(self, change_rows, nb_vm, os_server, change_type, field, openstack_value, netbox_value, commit, details="", state="changed", mode=None, comparison_group=None):
        """Append a single comparison row to the summary accumulator."""
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
                "diff": f"{netbox_text} -> {openstack_text}",
                "details": details,
                "state": state,
                "mode": mode or ("apply" if commit else "dry-run"),
                "comparison_group": comparison_group,
            }
        )

    def _comparison_row_log_method(self, row):
        """Return the NetBox logging method that matches a comparison row."""
        if row.get("state") == "matched":
            return self.log_success
        if row.get("comparison_group") == "to_sync":
            return self.log_failure
        if row.get("comparison_group") == "report_only":
            return self.log_warning
        if row.get("mode") == "apply":
            return self.log_success
        if row.get("mode") == "dry-run":
            return self.log_warning
        return self.log_info

    def _comparison_row_label(self, row):
        """Return a short human-readable status label for a comparison row."""
        if row.get("state") == "matched":
            return "PASS"
        if row.get("comparison_group") == "to_sync":
            return "FAIL"
        if row.get("comparison_group") == "report_only":
            return "WARN"
        if row.get("mode") == "apply":
            return "APPLIED"
        if row.get("mode") == "dry-run":
            return "DRY-RUN"
        return "INFO"

    def _log_change_summary(self, nb_vm, os_server, change_rows, commit):
        """Render the accumulated comparison rows as a Markdown summary block."""
        summary_target = self._os_server_ref(os_server) if os_server is not None else "<missing OpenStack server>"
        if not change_rows:
            return

        # Group changes by type so the rendered table is easy to scan.
        change_order = {
            "create": 0,
            "identity": 1,
            "rename": 2,
            "metadata": 3,
            "power": 4,
            "to_sync": 5,
            "report_only": 6,
            "match": 99,
        }
        ordered_rows = sorted(
            enumerate(change_rows),
            key=lambda item: (change_order.get(item[1]["change_type"], 99), item[0]),
        )

        applied_change_count = sum(
            1
            for row in change_rows
            if row.get("mode") not in {"report", "sync", "match"} and row.get("state", "changed") != "matched"
        )
        to_sync_count = sum(1 for row in change_rows if row.get("comparison_group") == "to_sync")
        report_only_count = sum(1 for row in change_rows if row.get("comparison_group") == "report_only")
        comparison_count = len(change_rows)
        # Count every comparison row, even matched ones, so the summary proves
        # which fields were checked and which ones were only reported.
        comparison_suffix = f"{comparison_count} field comparison{'s' if comparison_count != 1 else ''}"
        to_sync_suffix = (
            f"{to_sync_count} to_sync comparison{'s' if to_sync_count != 1 else ''}"
            if to_sync_count
            else ""
        )
        report_only_suffix = (
            f"{report_only_count} report-only comparison{'s' if report_only_count != 1 else ''}"
            if report_only_count
            else ""
        )
        if applied_change_count == 0:
            summary_suffix = f"(0 applied changes, {comparison_suffix}"
            if to_sync_suffix:
                summary_suffix += f", {to_sync_suffix}"
            if report_only_suffix:
                summary_suffix += f", {report_only_suffix}"
            summary_suffix += ")"
        else:
            summary_suffix = (
                f"({applied_change_count} applied change{'s' if applied_change_count != 1 else ''}, "
                f"{comparison_suffix}"
            )
            if to_sync_suffix:
                summary_suffix += f", {to_sync_suffix}"
            if report_only_suffix:
                summary_suffix += f", {report_only_suffix}"
            summary_suffix += ")"

        self.log_debug(
            f"### Change summary for {self._nb_vm_ref(nb_vm)} against {summary_target} {summary_suffix}",
            obj=nb_vm,
        )

        headers = [
            "Type",
            "Field",
            "NetBox",
            "OpenStack",
            "Diff",
            "Mode",
            "Details",
        ]
        lines = [
            "| " + " | ".join(headers) + " |",
            "| " + " | ".join("---" for _ in headers) + " |",
        ]

        for _, row in ordered_rows:
            line = [
                row["change_type"],
                row["field"],
                row["netbox_value"],
                row["openstack_value"],
                row["diff"],
                row["mode"],
                row["details"],
            ]
            lines.append("| " + " | ".join(self._markdown_cell(cell) for cell in line) + " |")

        self.log_debug("\n".join(lines), obj=nb_vm)

    def _log_change_report(self, nb_vm, os_server, change_rows, commit):
        """Render change rows as a severity-colored NetBox report."""
        summary_target = self._os_server_ref(os_server) if os_server is not None else "<missing OpenStack server>"
        if not change_rows:
            return

        # Keep the same grouping order as the table summary so the report is
        # easy to scan even though we emit one line per row.
        change_order = {
            "create": 0,
            "identity": 1,
            "rename": 2,
            "metadata": 3,
            "power": 4,
            "to_sync": 5,
            "report_only": 6,
            "match": 99,
        }
        ordered_rows = sorted(
            enumerate(change_rows),
            key=lambda item: (change_order.get(item[1]["change_type"], 99), item[0]),
        )

        applied_change_count = sum(
            1
            for row in change_rows
            if row.get("mode") not in {"report", "sync", "match"} and row.get("state", "changed") != "matched"
        )
        to_sync_count = sum(1 for row in change_rows if row.get("comparison_group") == "to_sync")
        report_only_count = sum(1 for row in change_rows if row.get("comparison_group") == "report_only")
        comparison_count = len(change_rows)
        comparison_suffix = f"{comparison_count} field comparison{'s' if comparison_count != 1 else ''}"
        to_sync_suffix = (
            f"{to_sync_count} to_sync comparison{'s' if to_sync_count != 1 else ''}"
            if to_sync_count
            else ""
        )
        report_only_suffix = (
            f"{report_only_count} report-only comparison{'s' if report_only_count != 1 else ''}"
            if report_only_count
            else ""
        )
        if applied_change_count == 0:
            summary_suffix = f"(0 applied changes, {comparison_suffix}"
            if to_sync_suffix:
                summary_suffix += f", {to_sync_suffix}"
            if report_only_suffix:
                summary_suffix += f", {report_only_suffix}"
            summary_suffix += ")"
        else:
            summary_suffix = (
                f"({applied_change_count} applied change{'s' if applied_change_count != 1 else ''}, "
                f"{comparison_suffix}"
            )
            if to_sync_suffix:
                summary_suffix += f", {to_sync_suffix}"
            if report_only_suffix:
                summary_suffix += f", {report_only_suffix}"
            summary_suffix += ")"

        self.log_info(
            f"### Change report for {self._nb_vm_ref(nb_vm)} against {summary_target} {summary_suffix}",
            obj=nb_vm,
        )

        for _, row in ordered_rows:
            label = self._comparison_row_label(row)
            if row.get("state") == "matched":
                comparison_text = (
                    f"NetBox `{self._summary_value(row['netbox_value'])}` "
                    f"== OpenStack `{self._summary_value(row['openstack_value'])}`"
                )
            else:
                comparison_text = (
                    f"NetBox `{self._summary_value(row['netbox_value'])}` "
                    f"-> OpenStack `{self._summary_value(row['openstack_value'])}`"
                )

            detail = row.get("details")
            detail_text = f" | Details `{detail}`" if detail and detail != "matched" else ""
            message = f"**{label}** `{row['field']}`: {comparison_text}{detail_text}"
            self._comparison_row_log_method(row)(message, obj=nb_vm)

    def _sync_vm(self, conn, nb_vm, data, commit, sync_debug=False):
        """Reconcile one NetBox VM against its matching OpenStack server."""
        change_rows = []
        os_instance = OpenStackInstance.retrieve(conn, nb_vm, log_debug=self.log_debug)
        nb_vm_ref = self._nb_vm_ref(nb_vm)
        desired_name = nb_vm.name
        desired_metadata = OpenStackInstance.desired_metadata(nb_vm)
        desired_status = OpenStackInstance.desired_server_status(nb_vm)

        if os_instance is None:
            # The VM has no matching server, so either create it or report that
            # creation is disabled for this run.
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

            if not commit:
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
                self._log_change_report(nb_vm, None, change_rows, commit)
                self._log_change_summary(nb_vm, None, change_rows, commit)
                return "created"

            # In apply mode we create the instance after resolving its resources.
            self.log_info(create_message, obj=nb_vm)
            os_instance = OpenStackInstance.create(
                conn,
                nb_vm,
                image,
                flavor,
                network,
                key_name=self._get_vm_cf_value(nb_vm, "key_name"),
                availability_zone=self._get_vm_cf_first(nb_vm, "openstack_availability_zone") or self._get_vm_cf_value(nb_vm, "openstack_location_zone"),
                security_groups=self._get_vm_cf_list(nb_vm, "openstack_security_groups"),
                metadata=desired_metadata,
                wait_for_active=data.get("wait_for_active", True),
            )
            self._record_change(
                change_rows,
                nb_vm,
                os_instance,
                "create",
                "instance",
                "<missing>",
                os_instance.name or nb_vm.name,
                commit,
                details=f"image={image.name}, flavor={flavor.name}, network={network.name}",
            )
            existing_openstack_id = self._get_vm_openstack_id(nb_vm)
            if existing_openstack_id != str(os_instance.id):
                self._record_change(
                    change_rows,
                    nb_vm,
                    os_instance,
                    "identity",
                    "openstack_id",
                    existing_openstack_id,
                    os_instance.id,
                    commit,
                )
                self._update_vm_openstack_id(nb_vm, os_instance.id, commit=True)
            if data.get("update_metadata"):
                os_instance.sync_metadata(
                    desired_metadata,
                    commit=commit,
                    change_rows=change_rows,
                    record_change=self._record_change,
                    nb_vm=nb_vm,
                    sync_debug=sync_debug,
                    log_debug=self.log_debug,
                )
            if data.get("sync_power_state"):
                os_instance.sync_power_state(
                    desired_status,
                    commit=commit,
                    change_rows=change_rows,
                    record_change=self._record_change,
                    nb_vm=nb_vm,
                )
            # Emit the comparison output as a NetBox-style report, with
            # severity coloring for matched and drifting rows.
            os_instance.compare_field_groups(
                nb_vm=nb_vm,
                commit=commit,
                change_rows=change_rows,
                record_change=self._record_change,
                sync_debug=sync_debug,
                log_debug=self.log_debug,
            )
            self._log_change_report(nb_vm, os_instance, change_rows, commit)
            self._log_change_summary(nb_vm, os_instance, change_rows, commit)
            return "created"

        os_server_ref = os_instance.ref()
        changed = False

        # Keep the NetBox openstack_id in sync with the actual OpenStack server.
        openstack_id = self._get_vm_openstack_id(nb_vm)
        if openstack_id != str(os_instance.id):
            changed = True
            self._record_change(
                change_rows,
                nb_vm,
                os_instance,
                "identity",
                "openstack_id",
                openstack_id,
                os_instance.id,
                commit,
            )
            if commit:
                self._update_vm_openstack_id(nb_vm, os_instance.id, commit=True)
        else:
            self._record_change(
                change_rows,
                nb_vm,
                os_instance,
                "identity",
                "openstack_id",
                openstack_id,
                os_instance.id,
                commit,
                details="matched",
                state="matched",
            )

        if data.get("allow_rename"):
            # Name changes are optional because some environments want stable
            # OpenStack server names even when NetBox naming differs.
            changed = os_instance.update_name(
                desired_name,
                commit=commit,
                change_rows=change_rows,
                record_change=self._record_change,
                nb_vm=nb_vm,
            ) or changed

        if data.get("update_metadata"):
            # Metadata sync is where most of the reconciliation happens.
            changed = os_instance.sync_metadata(
                desired_metadata,
                commit=commit,
                change_rows=change_rows,
                record_change=self._record_change,
                nb_vm=nb_vm,
                sync_debug=sync_debug,
                log_debug=self.log_debug,
            ) or changed

        if data.get("sync_power_state"):
            # Power state is optional because some operators prefer manual control.
            changed = os_instance.sync_power_state(
                desired_status,
                commit=commit,
                change_rows=change_rows,
                record_change=self._record_change,
                nb_vm=nb_vm,
            ) or changed

        # Emit the comparison output as a NetBox-style report, with severity
        # coloring for matched and drifting rows.
        os_instance.compare_field_groups(
            nb_vm=nb_vm,
            commit=commit,
            change_rows=change_rows,
            record_change=self._record_change,
            sync_debug=sync_debug,
            log_debug=self.log_debug,
        )

        self._log_change_report(nb_vm, os_instance, change_rows, commit)
        self._log_change_summary(nb_vm, os_instance, change_rows, commit)

        if not changed:
            self.log_info(f"No changes needed for {nb_vm_ref} against {os_server_ref}", obj=nb_vm)
            return "unchanged"

        return "updated"

    def _build_openstack_conn_kwargs(self, data, region_name=None, project_id=None, project_name=None):
        """Build the keyword arguments shared by every OpenStack connection."""
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
        """Return a cached base connection for the requested region."""
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
        """Resolve a project object by ID first, then by name if needed."""
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
        """Return a cached project-scoped OpenStack connection for one VM."""
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
        """Resolve the image, flavor, and network needed to create a server."""
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

    def _get_vm_openstack_id(self, nb_vm):
        """Return the OpenStack server ID stored on the NetBox VM."""
        return self._get_vm_cf_value(nb_vm, "openstack_id") or (nb_vm.serial or "").strip()

    def _update_vm_openstack_id(self, nb_vm, server_id, commit):
        """Persist the OpenStack server ID back onto the NetBox VM."""
        self._set_vm_cf_value(nb_vm, "openstack_id", str(server_id))
        nb_vm.serial = str(server_id)
        nb_vm.full_clean()
        nb_vm.save()

    def _get_vm_cf_data(self, nb_vm):
        """Return the NetBox VM custom field dictionary in a version-agnostic way."""
        data = getattr(nb_vm, "custom_field_data", None)
        if isinstance(data, dict):
            return data
        data = getattr(nb_vm, "cf", None)
        if isinstance(data, dict):
            return data
        return {}

    def _get_vm_cf_value(self, nb_vm, field_name):
        """Return a single custom field value, collapsing trivial one-item lists."""
        value = self._get_vm_cf_data(nb_vm).get(field_name)
        if value in (None, "", []):
            return None
        if isinstance(value, list) and len(value) == 1:
            return value[0]
        return value

    def _get_vm_cf_first(self, nb_vm, field_name):
        """Return the first item from a custom field value that may be a list."""
        value = self._get_vm_cf_data(nb_vm).get(field_name)
        if isinstance(value, list):
            return value[0] if value else None
        return value

    def _get_vm_cf_list(self, nb_vm, field_name):
        """Return a custom field value as a filtered list."""
        value = self._get_vm_cf_data(nb_vm).get(field_name)
        if value in (None, ""):
            return []
        if isinstance(value, list):
            return [item for item in value if item not in (None, "")]
        return [value]

    def _set_vm_cf_value(self, nb_vm, field_name, value):
        """Update the cached custom field data on the NetBox VM object."""
        data = dict(self._get_vm_cf_data(nb_vm))
        data[field_name] = value
        setattr(nb_vm, "custom_field_data", data)
