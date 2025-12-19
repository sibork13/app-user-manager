"""Databricks User Group Manager Application."""
import logging
import sys
from typing import List, Dict, Any, Optional
from datetime import datetime

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import Group, User, ComplexValue

from auth import AuthService
from group_manager import GroupManager
from config import settings

# Set up logging
logging.basicConfig(
    level=settings.LOG_LEVEL,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('user_group_manager.log')
    ]
)
logger = logging.getLogger(__name__)

class UserGroupManagerApp:
    """Main application class for managing user groups in Databricks."""
    
    def __init__(self):
        """Initialize the application with required services."""
        self.auth_service = AuthService()
        self.group_manager = GroupManager(self.auth_service)
        self.logger = logging.getLogger(__name__)
    
    def get_manageable_groups(self) -> List[Dict[str, str]]:
        """Get a list of groups that the current user can manage."""
        self.logger.info("Fetching manageable groups...")
        return self.group_manager.list_manageable_groups()
    
    def get_group_members(self, group_name: str) -> List[Dict[str, str]]:
        """Get members of a specific group."""
        self.logger.info(f"Fetching members of group: {group_name}")
        return self.group_manager.get_group_members(group_name)
    
    def add_user_to_groups(self, user_email: str, group_names: List[str]) -> Dict[str, Any]:
        """
        Add a user to one or more groups.
        
        Args:
            user_email: Email of the user to add
            group_names: List of group names to add the user to
            
        Returns:
            Dictionary with operation results
        """
        self.logger.info(f"Adding user {user_email} to groups: {', '.join(group_names)}")
        return self.group_manager.add_user_to_groups(user_email, group_names)
    
    def remove_user_from_groups(self, user_email: str, group_names: List[str]) -> Dict[str, Any]:
        """
        Remove a user from one or more groups.
        
        Args:
            user_email: Email of the user to remove
            group_names: List of group names to remove the user from
            
        Returns:
            Dictionary with operation results
        """
        self.logger.info(f"Removing user {user_email} from groups: {', '.join(group_names)}")
        return self.group_manager.remove_user_from_groups(user_email, group_names)
    
    def display_manageable_groups(self):
        """Display a list of groups the current user can manage."""
        try:
            groups = self.get_manageable_groups()
            if not groups:
                print("No manageable groups found or you don't have permissions to any groups.")
                return
                
            print("\nManageable Groups:")
            print("-" * 50)
            for i, group in enumerate(groups, 1):
                print(f"{i}. {group['displayName']} (ID: {group['id']})")
            print("")
            
        except Exception as e:
            self.logger.error(f"Failed to display manageable groups: {str(e)}")
            print(f"Error: {str(e)}")
    
    def display_group_members(self, group_name: str):
        """Display members of a specific group."""
        try:
            members = self.get_group_members(group_name)
            if not members:
                print(f"No members found in group '{group_name}' or group does not exist.")
                return
                
            print(f"\nMembers of group '{group_name}':")
            print("-" * 50)
            for i, member in enumerate(members, 1):
                print(f"{i}. {member.get('displayName', 'N/A')} ({member.get('userName', 'N/A')})")
            print("")
            
        except Exception as e:
            self.logger.error(f"Failed to display group members: {str(e)}")
            print(f"Error: {str(e)}")
    
    def interactive_add_user(self):
        """Interactive mode to add a user to groups."""
        try:
            # Get user email
            user_email = input("\nEnter user email to add to groups: ").strip()
            if not user_email:
                print("Error: User email cannot be empty.")
                return
            
            # Get manageable groups
            groups = self.get_manageable_groups()
            if not groups:
                print("No manageable groups found. You don't have permissions to add users to any groups.")
                return
            
            # Display groups
            print("\nAvailable Groups (you have permission to manage):")
            for i, group in enumerate(groups, 1):
                print(f"{i}. {group['displayName']}")
            
            # Get group selection
            selection = input("\nEnter group numbers to add the user to (comma-separated, e.g., 1,3,5): ").strip()
            try:
                selected_indices = [int(idx.strip()) - 1 for idx in selection.split(',')]
                selected_groups = [groups[idx]['displayName'] for idx in selected_indices 
                                 if 0 <= idx < len(groups)]
            except (ValueError, IndexError):
                print("Invalid selection. Please enter valid group numbers.")
                return
            
            if not selected_groups:
                print("No valid groups selected.")
                return
            
            # Confirm and execute
            print(f"\nYou are about to add user '{user_email}' to the following groups:")
            for group in selected_groups:
                print(f"- {group}")
            
            confirm = input("\nDo you want to proceed? (y/n): ").strip().lower()
            if confirm != 'y':
                print("Operation cancelled.")
                return
            
            # Add user to groups
            results = self.add_user_to_groups(user_email, selected_groups)
            
            # Display results
            print("\nOperation Results:")
            print("-" * 50)
            if results['success']:
                print("\nSuccessfully added to:")
                for item in results['success']:
                    print(f"- {item['group']} ({item['status']})")
            
            if results['failed']:
                print("\nFailed to add to:")
                for item in results['failed']:
                    print(f"- {item['group']}: {item['reason']}")
            
            if results['unauthorized']:
                print("\nNot authorized to manage these groups:")
                for group in results['unauthorized']:
                    print(f"- {group}")
            
            print("")
            
        except Exception as e:
            self.logger.error(f"Error in interactive_add_user: {str(e)}")
            print(f"An error occurred: {str(e)}")
    
    def interactive_remove_user(self):
        """Interactive mode to remove a user from groups."""
        try:
            # Get user email
            user_email = input("\nEnter user email to remove from groups: ").strip()
            if not user_email:
                print("Error: User email cannot be empty.")
                return
            
            # Get manageable groups where the user is a member
            groups = self.get_manageable_groups()
            if not groups:
                print("No manageable groups found. You don't have permissions to manage any groups.")
                return
            
            # Find groups where the user is a member
            user_groups = []
            for group in groups:
                members = self.get_group_members(group['displayName'])
                if any(member.get('userName') == user_email for member in members):
                    user_groups.append(group)
            
            if not user_groups:
                print(f"User '{user_email}' is not a member of any groups you can manage.")
                return
            
            # Display groups
            print(f"\nGroups containing user '{user_email}' that you can manage:")
            for i, group in enumerate(user_groups, 1):
                print(f"{i}. {group['displayName']}")
            
            # Get group selection
            selection = input("\nEnter group numbers to remove the user from (comma-separated, e.g., 1,3,5 or 'all'): ").strip().lower()
            
            if selection == 'all':
                selected_groups = [group['displayName'] for group in user_groups]
            else:
                try:
                    selected_indices = [int(idx.strip()) - 1 for idx in selection.split(',')]
                    selected_groups = [user_groups[idx]['displayName'] for idx in selected_indices 
                                     if 0 <= idx < len(user_groups)]
                except (ValueError, IndexError):
                    print("Invalid selection. Please enter valid group numbers or 'all'.")
                    return
            
            if not selected_groups:
                print("No valid groups selected.")
                return
            
            # Confirm and execute
            print(f"\nYou are about to remove user '{user_email}' from the following groups:")
            for group in selected_groups:
                print(f"- {group}")
            
            confirm = input("\nDo you want to proceed? This action cannot be undone. (y/n): ").strip().lower()
            if confirm != 'y':
                print("Operation cancelled.")
                return
            
            # Remove user from groups
            results = self.remove_user_from_groups(user_email, selected_groups)
            
            # Display results
            print("\nOperation Results:")
            print("-" * 50)
            if results['success']:
                print("\nSuccessfully removed from:")
                for item in results['success']:
                    print(f"- {item['group']} ({item['status']})")
            
            if results['failed']:
                print("\nFailed to remove from:")
                for item in results['failed']:
                    print(f"- {item['group']}: {item['reason']}")
            
            if results['unauthorized']:
                print("\nNot authorized to manage these groups:")
                for group in results['unauthorized']:
                    print(f"- {group}")
            
            print("")
            
        except Exception as e:
            self.logger.error(f"Error in interactive_remove_user: {str(e)}")
            print(f"An error occurred: {str(e)}")

def main():
    """Main entry point for the application."""
    try:
        app = UserGroupManagerApp()
        
        while True:
            print("\n" + "=" * 50)
            print("DATABRICKS USER GROUP MANAGER".center(50))
            print("=" * 50)
            print("1. List groups you can manage")
            print("2. View group members")
            print("3. Add user to groups")
            print("4. Remove user from groups")
            print("5. Exit")
            
            choice = input("\nEnter your choice (1-5): ").strip()
            
            if choice == '1':
                app.display_manageable_groups()
            elif choice == '2':
                group_name = input("Enter group name to view members: ").strip()
                if group_name:
                    app.display_group_members(group_name)
                else:
                    print("Error: Group name cannot be empty.")
            elif choice == '3':
                app.interactive_add_user()
            elif choice == '4':
                app.interactive_remove_user()
            elif choice == '5':
                print("Exiting...")
                break
            else:
                print("Invalid choice. Please enter a number between 1 and 5.")
            
            input("\nPress Enter to continue...")
            
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
    except Exception as e:
        logger.critical(f"Critical error: {str(e)}", exc_info=True)
        print(f"\nA critical error occurred: {str(e)}")
        print("Please check the logs for more details.")

if __name__ == "__main__":
    main()
