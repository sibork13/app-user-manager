"""Group management service for Databricks User Group Manager."""
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import Group, ComplexValue
from typing import List, Optional, Dict, Any
import logging

class GroupManager:
    """Handles group management operations in Databricks."""
    
    def __init__(self, auth_service):
        self.auth_service = auth_service
        self.logger = logging.getLogger(__name__)
        self._user_client = None
        self._service_principal_client = None
    
    @property
    def user_client(self) -> WorkspaceClient:
        """Lazy-loading user client."""
        if self._user_client is None:
            self._user_client = self.auth_service.get_user_client()
        return self._user_client
    
    @property
    def service_principal_client(self) -> WorkspaceClient:
        """Lazy-loading service principal client."""
        if self._service_principal_client is None:
            self._service_principal_client = self.auth_service.get_service_principal_client()
        return self._service_principal_client
    
    def list_manageable_groups(self) -> List[Dict[str, Any]]:
        """
        List all groups that the current user has permission to manage.
        
        Returns:
            List of group dictionaries with 'id' and 'displayName' keys
        """
        try:
            all_groups = self.user_client.groups.list(attributes="id,displayName")
            manageable_groups = []
            
            for group in all_groups:
                if self.auth_service.can_manage_group(self.user_client, group.display_name):
                    manageable_groups.append({
                        'id': group.id,
                        'displayName': group.display_name
                    })
                    
            return manageable_groups
            
        except Exception as e:
            self.logger.error(f"Failed to list manageable groups: {str(e)}")
            raise
    
    def get_group_members(self, group_name: str) -> List[Dict[str, str]]:
        """
        Get all members of a group.
        
        Args:
            group_name: Name of the group
            
        Returns:
            List of member dictionaries with 'id', 'userName', and 'displayName' keys
        """
        try:
            # First, get the group ID
            group = next((g for g in self.user_client.groups.list(attributes="id,displayName") 
                         if g.display_name == group_name), None)
            
            if not group:
                self.logger.warning(f"Group '{group_name}' not found")
                return []
                
            # Get group members
            members = self.service_principal_client.groups.list_members(group.id)
            
            return [{
                'id': member.id,
                'userName': member.user_name,
                'displayName': member.display_name
            } for member in members]
            
        except Exception as e:
            self.logger.error(f"Failed to get group members for '{group_name}': {str(e)}")
            raise
    
    def add_user_to_groups(self, user_email: str, group_names: List[str]) -> Dict[str, Any]:
        """
        Add a user to one or more groups.
        
        Args:
            user_email: Email of the user to add
            group_names: List of group names to add the user to
            
        Returns:
            Dictionary with operation results
        """
        results = {
            'success': [],
            'failed': [],
            'unauthorized': []
        }
        
        # First, verify the user exists
        try:
            user = next((u for u in self.service_principal_client.users.list() 
                        if u.emails and user_email in [e.value for e in u.emails]), None)
            
            if not user:
                raise ValueError(f"User with email '{user_email}' not found")
                
            # Process each group
            for group_name in group_names:
                try:
                    # Verify permission to manage this group
                    if not self.auth_service.can_manage_group(self.user_client, group_name):
                        results['unauthorized'].append(group_name)
                        continue
                        
                    # Get the group
                    group = next((g for g in self.service_principal_client.groups.list() 
                                if g.display_name == group_name), None)
                    
                    if not group:
                        results['failed'].append({
                            'group': group_name,
                            'reason': 'Group not found'
                        })
                        continue
                    
                    # Check if user is already in the group
                    members = self.service_principal_client.groups.list_members(group.id)
                    if any(m.id == user.id for m in members):
                        results['success'].append({
                            'group': group_name,
                            'status': 'already_member'
                        })
                        continue
                    
                    # Add user to group using service principal
                    self.service_principal_client.groups.add_member(
                        group_id=group.id,
                        user_name=user_email
                    )
                    
                    results['success'].append({
                        'group': group_name,
                        'status': 'added'
                    })
                    
                    # Log the action
                    self.logger.info(
                        f"User '{user_email}' added to group '{group_name}'. "
                        f"Action performed by service principal."
                    )
                    
                except Exception as e:
                    self.logger.error(
                        f"Failed to add user '{user_email}' to group '{group_name}': {str(e)}"
                    )
                    results['failed'].append({
                        'group': group_name,
                        'reason': str(e)
                    })
                    
        except Exception as e:
            self.logger.error(f"Error in add_user_to_groups: {str(e)}")
            raise
            
        return results
    
    def remove_user_from_groups(self, user_email: str, group_names: List[str]) -> Dict[str, Any]:
        """
        Remove a user from one or more groups.
        
        Args:
            user_email: Email of the user to remove
            group_names: List of group names to remove the user from
            
        Returns:
            Dictionary with operation results
        """
        results = {
            'success': [],
            'failed': [],
            'unauthorized': []
        }
        
        # First, verify the user exists
        try:
            user = next((u for u in self.service_principal_client.users.list() 
                        if u.emails and user_email in [e.value for e in u.emails]), None)
            
            if not user:
                raise ValueError(f"User with email '{user_email}' not found")
                
            # Process each group
            for group_name in group_names:
                try:
                    # Verify permission to manage this group
                    if not self.auth_service.can_manage_group(self.user_client, group_name):
                        results['unauthorized'].append(group_name)
                        continue
                        
                    # Get the group
                    group = next((g for g in self.service_principal_client.groups.list() 
                                if g.display_name == group_name), None)
                    
                    if not group:
                        results['failed'].append({
                            'group': group_name,
                            'reason': 'Group not found'
                        })
                        continue
                    
                    # Check if user is in the group
                    members = self.service_principal_client.groups.list_members(group.id)
                    member = next((m for m in members if m.id == user.id), None)
                    
                    if not member:
                        results['success'].append({
                            'group': group_name,
                            'status': 'not_in_group'
                        })
                        continue
                    
                    # Remove user from group using service principal
                    self.service_principal_client.groups.remove_member(
                        group_id=group.id,
                        user_id=user.id
                    )
                    
                    results['success'].append({
                        'group': group_name,
                        'status': 'removed'
                    })
                    
                    # Log the action
                    self.logger.info(
                        f"User '{user_email}' removed from group '{group_name}'. "
                        f"Action performed by service principal."
                    )
                    
                except Exception as e:
                    self.logger.error(
                        f"Failed to remove user '{user_email}' from group '{group_name}': {str(e)}"
                    )
                    results['failed'].append({
                        'group': group_name,
                        'reason': str(e)
                    })
                    
        except Exception as e:
            self.logger.error(f"Error in remove_user_from_groups: {str(e)}")
            raise
            
        return results
