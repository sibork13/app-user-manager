"""Authentication and authorization service for Databricks User Group Manager."""
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from typing import Optional, Dict, Any
import logging

from config import settings

class AuthService:
    """Handles authentication and authorization for the application."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._service_principal_creds = None
        
    def get_user_client(self) -> WorkspaceClient:
        """Get a Databricks workspace client authenticated as the current user."""
        return WorkspaceClient()
    
    def _get_service_principal_credentials(self) -> Dict[str, str]:
        """Retrieve service principal credentials from Azure Key Vault."""
        if not self._service_principal_creds:
            try:
                credential = ClientSecretCredential(
                    tenant_id=settings.TENANT_ID,
                    client_id=settings.SERVICE_PRINCIPAL_CLIENT_ID,
                    client_secret=settings.SERVICE_PRINCIPAL_SECRET.get_secret_value()
                )
                
                key_vault_url = f"https://{settings.KEY_VAULT_NAME}.vault.azure.net"
                client = SecretClient(vault_url=key_vault_url, credential=credential)
                
                self._service_principal_creds = {
                    'client_id': settings.SERVICE_PRINCIPAL_CLIENT_ID,
                    'client_secret': settings.SERVICE_PRINCIPAL_SECRET.get_secret_value(),
                    'tenant_id': settings.TENANT_ID
                }
                
            except Exception as e:
                self.logger.error(f"Failed to retrieve service principal credentials: {str(e)}")
                raise
                
        return self._service_principal_creds
    
    def get_service_principal_client(self) -> WorkspaceClient:
        """Get a Databricks workspace client authenticated as the service principal."""
        try:
            creds = self._get_service_principal_credentials()
            config = Config(
                host=settings.DATABRICKS_WORKSPACE_URL,
                client_id=creds['client_id'],
                client_secret=creds['client_secret']
            )
            return WorkspaceClient(config=config)
        except Exception as e:
            self.logger.error(f"Failed to create service principal client: {str(e)}")
            raise
    
    def can_manage_group(self, user_client: WorkspaceClient, group_name: str) -> bool:
        """Check if the current user has permissions to manage the specified group."""
        try:
            # Get the current user's groups
            current_user = user_client.current_user.me()
            user_groups = user_client.groups.list(attributes="id,displayName")
            
            # Check if user is an admin or owner of the group
            for group in user_groups:
                if group.display_name == group_name:
                    # Check group permissions using the Permissions API
                    # Note: This is a simplified check - you might need to adjust based on your exact permission model
                    group_permissions = user_client.permissions.get("groups", group.id)
                    # Check if user has admin or manage permissions on the group
                    # This is a placeholder - adjust based on your permission model
                    return any(perm.permission_level in ['ADMIN', 'MANAGE'] 
                             for perm in group_permissions.access_control_list)
            return False
            
        except Exception as e:
            self.logger.error(f"Failed to check group management permissions: {str(e)}")
            return False
