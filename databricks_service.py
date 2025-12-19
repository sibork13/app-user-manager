import logging
from typing import List, Dict, Optional
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam
from databricks.sdk.core import Config
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from datetime import datetime
import json

from config import settings

# Configure logging
logging.basicConfig(level=settings.LOG_LEVEL)
logger = logging.getLogger(__name__)

class DatabricksGroupService:
    """Service for managing Databricks group memberships with proper security controls."""
    
    def __init__(self):
        self._user_client = WorkspaceClient()  # Client using current user's credentials
        self._service_principal_client = None
        
    def _get_service_principal_client(self) -> WorkspaceClient:
        """Get a Databricks client authenticated as the service principal."""
        if self._service_principal_client is None:
            # Get service principal credentials from Azure Key Vault
            credential = ClientSecretCredential(
                tenant_id=settings.TENANT_ID,
                client_id=settings.SERVICE_PRINCIPAL_CLIENT_ID,
                client_secret=settings.SERVICE_PRINCIPAL_SECRET.get_secret_value()
            )
            
            self._service_principal_client = WorkspaceClient(
                host=settings.DATABRICKS_WORKSPACE_URL,
                token=credential.get_token("https://management.azure.com/.default").token
            )
        return self._service_principal_client
    
    def get_available_groups(self) -> List[Dict]:
        """Get list of groups that the current user has permissions to manage."""
        try:
            groups = []
            for group in self._user_client.groups.list():
                try:
                    # Check if user has permission to manage members
                    self._user_client.groups.get(group.id)
                    groups.append({
                        'id': group.id,
                        'display_name': group.display_name,
                        'url': f"{settings.DATABRICKS_WORKSPACE_URL}/#setting/accounts/groups/{group.id}"
                    })
                except Exception as e:
                    logger.debug(f"Skipping group {group.display_name}: {str(e)}")
                    continue
            return groups
        except Exception as e:
            logger.error(f"Error fetching groups: {str(e)}")
            raise
    
    def get_group_members(self, group_id: str) -> List[Dict]:
        """Get list of members in a specific group."""
        try:
            members = []
            for member in self._user_client.groups.list_members(group_id):
                members.append({
                    'id': member.id,
                    'user_name': member.user_name,
                    'display_name': getattr(member, 'display_name', '')
                })
            return members
        except Exception as e:
            logger.error(f"Error fetching group members: {str(e)}")
            raise
    
    def _audit_log(self, action: str, target_user: str, group_name: str, success: bool, 
                  details: Optional[Dict] = None):
        """Log an audit event for the action."""
        log_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'action': action,
            'executing_user': self._user_client.current_user.me().user_name,
            'target_user': target_user,
            'group': group_name,
            'success': success,
            'details': details or {}
        }
        logger.info(f"AUDIT: {json.dumps(log_entry)}")
    
    def add_user_to_group(self, user_email: str, group_id: str) -> bool:
        """Add a user to a group using the service principal."""
        try:
            # First verify the current user has permission to modify this group
            try:
                self._user_client.groups.get(group_id)
            except Exception as e:
                self._audit_log(
                    action="ADD_USER_TO_GROUP",
                    target_user=user_email,
                    group_name=group_id,
                    success=False,
                    details={"error": "Permission denied", "details": str(e)}
                )
                return False
            
            # Use service principal to perform the action
            client = self._get_service_principal_client()
            
            # Get or create the user
            try:
                user = client.users.get(user_email)
            except Exception:
                # User doesn't exist, create them
                user = client.users.create(
                    user_name=user_email,
                    entitlements=[iam.ComplexValue(value="allow-cluster-create")]
                )
            
            # Add user to group
            client.groups.add_member(
                group_id=group_id,
                user_name=user_email
            )
            
            self._audit_log(
                action="ADD_USER_TO_GROUP",
                target_user=user_email,
                group_name=group_id,
                success=True
            )
            return True
            
        except Exception as e:
            self._audit_log(
                action="ADD_USER_TO_GROUP",
                target_user=user_email,
                group_name=group_id,
                success=False,
                details={"error": str(e)}
            )
            logger.exception(f"Failed to add user {user_email} to group {group_id}")
            return False
    
    def remove_user_from_group(self, user_email: str, group_id: str) -> bool:
        """Remove a user from a group using the service principal."""
        try:
            # First verify the current user has permission to modify this group
            try:
                self._user_client.groups.get(group_id)
            except Exception as e:
                self._audit_log(
                    action="REMOVE_USER_FROM_GROUP",
                    target_user=user_email,
                    group_name=group_id,
                    success=False,
                    details={"error": "Permission denied", "details": str(e)}
                )
                return False
            
            # Use service principal to perform the action
            client = self._get_service_principal_client()
            
            # Get the user ID
            user = client.users.get(user_email)
            
            # Remove user from group
            client.groups.remove_member(
                group_id=group_id,
                user_id=user.id
            )
            
            self._audit_log(
                action="REMOVE_USER_FROM_GROUP",
                target_user=user_email,
                group_name=group_id,
                success=True
            )
            return True
            
        except Exception as e:
            self._audit_log(
                action="REMOVE_USER_FROM_GROUP",
                target_user=user_email,
                group_name=group_id,
                success=False,
                details={"error": str(e)}
            )
            logger.exception(f"Failed to remove user {user_email} from group {group_id}")
            return False
