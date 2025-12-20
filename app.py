# Databricks User Management Application
#
# DESCRIPTION:
# This script, intended to be run as a Databricks notebook, provides a UI to add or remove
# users from Databricks groups.
#
# PREREQUISITES:
# 1. LIBRARIES:
#    - Install the following libraries on the cluster:
#      - databricks-sdk
#      - azure-identity
#      - azure-keyvault-secrets
#
# 2. DATABRICKS SECRETS (For the App's Service Principal - SPN1):
#    - Create a Databricks secret scope (e.g., 'keyvault-scope').
#    - Add the following secrets to the scope, corresponding to the Service Principal
#      that has GET/LIST permissions on the Azure Key Vault:
#      - 'kv-tenant-id': The Azure Tenant ID.
#      - 'kv-client-id': The Application (client) ID of the Service Principal.
#      - 'kv-client-secret': The client secret for the Service Principal.
#
# 3. AZURE KEY VAULT (To store the Management Service Principal's credentials - SPN2):
#    - Create an Azure Key Vault.
#    - Create a secret in the Key Vault that stores a Databricks Personal Access Token (PAT)
#      for a user or Service Principal (SPN2) with permissions to manage users and groups
#      (e.g., a workspace admin).
#      - Example Secret Name: 'databricks-management-pat'
#
# HOW TO USE:
# 1. Import this script as a Databricks notebook or copy its content into notebook cells.
# 2. Run the notebook.
# 3. Fill in the parameters in the UI widgets that appear at the top of the notebook.
# 4. The main logic will execute, performing the requested action.

import os
from azure.identity import DefaultAzureCredential, ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import ScimUser

# ---------------------------------------------------------------------------
# 1. SETUP & UI WIDGETS
# ---------------------------------------------------------------------------
# dbutils is globally available in Databricks notebooks.
# If running locally for testing, you might need to mock this.
try:
    import dbutils
    notebook_context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    dbutils.widgets.text("key_vault_uri", "", "1. Key Vault URI")
    dbutils.widgets.text("kv_secret_name", "databricks-management-pat", "2. Management PAT Secret Name (in KV)")
    dbutils.widgets.text("target_user_email", "", "3. Target User Email")
    dbutils.widgets.dropdown("action", "ADD", ["ADD", "REMOVE"], "4. Action")

    # Initialize WorkspaceClient with the user's context to fetch groups
    # This authenticates as the user running the notebook.
    user_ws_client = WorkspaceClient()
    
    # Fetch all groups in the workspace
    try:
        all_groups = user_ws_client.groups.list()
        group_display_names = [g.display_name for g in all_groups if g.display_name]
        if not group_display_names:
            raise Exception("No groups found or user lacks permissions to list them.")
        dbutils.widgets.dropdown("target_group_name", group_display_names[0], group_display_names, "5. Target Group")
        IS_DATABRICKS = True
    except Exception as e:
        print(f"Error fetching groups: {e}")
        print("Please ensure the user running this notebook has permissions to list groups.")
        # Create a dummy dropdown if group fetching fails
        dbutils.widgets.dropdown("target_group_name", "error-fetching-groups", ["error-fetching-groups"], "5. Target Group")
        IS_DATABRICKS = False

except (ImportError, ModuleNotFoundError):
    print("WARNING: 'dbutils' not found. Running in a non-Databricks environment.")
    print("UI widgets and automatic authentication will not be available.")
    IS_DATABRICKS = False

# ---------------------------------------------------------------------------
# 2. HELPER FUNCTIONS
# ---------------------------------------------------------------------------

def get_management_pat_from_key_vault(kv_uri, secret_name):
    """
    Authenticates to Azure Key Vault using the app's service principal (SPN1)
    and retrieves the management PAT for the management principal (SPN2).
    """
    print(f"Attempting to retrieve secret '{secret_name}' from Key Vault: {kv_uri}")
    try:
        # In Databricks, we configure the DefaultAzureCredential to use the environment
        # variables set up by the secret scope.
        # For local dev, it can use Azure CLI login, VSCode login, etc.
        credential = DefaultAzureCredential()

        # Create a secret client
        secret_client = SecretClient(vault_url=kv_uri, credential=credential)
        
        # Retrieve the secret
        retrieved_secret = secret_client.get_secret(secret_name)
        print("Successfully retrieved secret from Key Vault.")
        return retrieved_secret.value
    except Exception as e:
        print(f"ERROR: Failed to retrieve secret from Key Vault: {e}")
        raise

def get_user_by_email(client: WorkspaceClient, email: str) -> ScimUser:
    """Fetches a user object by their email address."""
    print(f"Searching for user with email: {email}")
    try:
        users = client.users.list(filter=f"userName eq '{email}'")
        user_list = [u for u in users]
        if not user_list:
            raise ValueError(f"User with email '{email}' not found in the workspace.")
        if len(user_list) > 1:
            print(f"Warning: Found multiple users with email '{email}'. Using the first one.")
        
        found_user = user_list[0]
        print(f"Found user '{found_user.display_name}' with ID: {found_user.id}")
        return found_user
    except Exception as e:
        print(f"ERROR: Could not find user '{email}'. {e}")
        raise

def get_group_by_name(client: WorkspaceClient, group_name: str):
    """Fetches a group object by its display name."""
    print(f"Searching for group with name: {group_name}")
    try:
        groups = client.groups.list(filter=f"displayName eq '{group_name}'")
        group_list = [g for g in groups]
        if not group_list:
            raise ValueError(f"Group with name '{group_name}' not found.")
        if len(group_list) > 1:
            print(f"Warning: Found multiple groups named '{group_name}'. Using the first one.")

        found_group = group_list[0]
        print(f"Found group '{found_group.display_name}' with ID: {found_group.id}")
        return found_group
    except Exception as e:
        print(f"ERROR: Could not find group '{group_name}'. {e}")
        raise

# ---------------------------------------------------------------------------
# 3. MAIN EXECUTION LOGIC
# ---------------------------------------------------------------------------

def main():
    """
    Main function to execute the user/group management logic.
    """
    if not IS_DATABRICKS:
        print("This script is designed to run in a Databricks notebook.")
        print("Please configure the environment variables manually if running elsewhere.")
        # Example of manual configuration for local testing
        os.environ['AZURE_TENANT_ID'] = "your-tenant-id"
        os.environ['AZURE_CLIENT_ID'] = "your-app-spn-client-id"
        os.environ['AZURE_CLIENT_SECRET'] = "your-app-spn-client-secret"
        key_vault_uri = "https://your-keyvault-name.vault.azure.net/"
        kv_secret_name = "databricks-management-pat"
        target_user_email = "user@example.com"
        action = "ADD"
        target_group_name = "my-test-group"
    else:
        # Get parameters from Databricks widgets
        key_vault_uri = dbutils.widgets.get("key_vault_uri")
        kv_secret_name = dbutils.widgets.get("kv_secret_name")
        target_user_email = dbutils.widgets.get("target_user_email")
        action = dbutils.widgets.get("action")
        target_group_name = dbutils.widgets.get("target_group_name")

    # --- Parameter Validation ---
    if not all([key_vault_uri, kv_secret_name, target_user_email, action, target_group_name]):
        print("ERROR: All parameters in the widgets must be filled out.")
        return
    if "error-fetching-groups" in target_group_name:
        print("ERROR: Cannot proceed because groups could not be fetched.")
        return

    print("--- Starting User Management Process ---")
    print(f"Action: {action}")
    print(f"Target User: {target_user_email}")
    print(f"Target Group: {target_group_name}")
    print("----------------------------------------")

    try:
        # 1. Get the management PAT from Key Vault
        # This uses DefaultAzureCredential, which in Databricks will use the environment
        # variables you've configured via the secrets.
        management_pat = get_management_pat_from_key_vault(key_vault_uri, kv_secret_name)
        
        # 2. Initialize a new WorkspaceClient with the management PAT
        # This client has the elevated permissions required to manage users.
        management_ws_client = WorkspaceClient(
            host=user_ws_client.config.host, # Reuse host from user's context
            token=management_pat
        )
        print("Successfully created management client.")

        # 3. Get the target user and group objects
        target_user = get_user_by_email(management_ws_client, target_user_email)
        target_group = get_group_by_name(management_ws_client, target_group_name)

        # 4. Perform the add or remove action
        if action == "ADD":
            print(f"Adding user '{target_user.display_name}' to group '{target_group.display_name}'...")
            management_ws_client.groups.patch(
                id=target_group.id,
                schemas=["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                operations=[{
                    "op": "add",
                    "path": "members",
                    "value": [{"value": target_user.id}]
                }]
            )
            print("--- User successfully added. ---")

        elif action == "REMOVE":
            print(f"Removing user '{target_user.display_name}' from group '{target_group.display_name}'...")
            management_ws_client.groups.patch(
                id=target_group.id,
                schemas=["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                operations=[{
                    "op": "remove",
                    "path": f"members[value eq {target_user.id}]"
                }]
            )
            print("--- User successfully removed. ---")

    except Exception as e:
        print(f"\n--- PROCESS FAILED ---")
        print(f"An error occurred: {e}")
        # In a real notebook, you might want to use dbutils.notebook.exit() here
        # dbutils.notebook.exit(f"An error occurred: {e}")

# Entry point for the script
if __name__ == "__main__":
    main()
