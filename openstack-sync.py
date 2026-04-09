from django import forms
from extras.scripts import Script, StringVar, ObjectVar, BooleanVar
from tenancy.models import Tenant
from utilities.exceptions import AbortScript
from virtualization.models import Cluster, VirtualMachine


class SyncNetBoxVMsToOpenStack(Script):
    class Meta:
        name = "Sync NetBox VMs to OpenStack"
        description = "Push NetBox virtual machine data into OpenStack instances. Leave Commit unchecked to run in dry-run mode."
        fieldsets = (
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
        description="User domain name, for example Default",
    )
    project_domain_name = StringVar(
        required=False,
        description="Project domain name, for example Default",
    )
    verify = BooleanVar(
        required=False,
        default=True,
        description="Validate the OpenStack API TLS certificate",
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
