from warehouse_permission.ttypes import (
    AuthorizationInput,
    AuthorizationResultCode,
    CatalogType,
    ClientContext,
    MultiCheckAuthorizedRequest,
    Privilege,
    ResourceRef,
    ResourceType,
)
from wps import wps

NAMESPACE = "instagram"
TABLE = "xldb_test_ig_muddler_generic_training_data_explore_v2_clustered_flattened"


def verify_permissions(data_project):
    print(
        f"Verifying permissions for data project {data_project} on table {NAMESPACE}:{TABLE}..."
    )
    ref = ResourceRef(ResourceType.TABLE, NAMESPACE, TABLE, CatalogType.HIVE)
    client_context = ClientContext(meta_data_only=True)

    request = MultiCheckAuthorizedRequest(
        f"grp:{data_project}",
        [
            AuthorizationInput(ref, Privilege.OWNERSHIP),
            AuthorizationInput(ref, Privilege.SELECT),
            AuthorizationInput(ref, Privilege.INSERT),
            AuthorizationInput(ref, Privilege.DELETE),
        ],
        client_context=client_context,
    )
    multi_check_auth_response = wps().multi_check_authorized(request)
    for response in multi_check_auth_response.results:
        if response.result_code == AuthorizationResultCode.DENIED:
            raise RuntimeError(
                f"Unauthorized access to {NAMESPACE=}:{TABLE=}. {response=}"
            )
    print("Permissions verified successfully.")


verify_permissions("xldb_alpha_rollback")
