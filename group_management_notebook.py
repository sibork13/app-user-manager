#!/usr/bin/env python
# Databricks notebook source
# DBTITLE 1,Databricks Group Management Tool

# COMMAND ----------

# MAGIC %md
# MAGIC # Databricks Group Management
# MAGIC 
# MAGIC This notebook provides a secure interface for managing user memberships in Databricks groups.
# MAGIC 
# MAGIC ## Features:
# MAGIC - View groups you have permission to manage
# MAGIC - Add users to groups
# MAGIC - Remove users from groups
# MAGIC - Full audit logging of all actions
# MAGIC 
# MAGIC ## Security:
# MAGIC - Uses service principal for all modification operations
# MAGIC - Validates user permissions before any action
# MAGIC - All actions are logged with full audit trail

# COMMAND ----------

# DBTITLE 1,Install required packages
# MAGIC %pip install --upgrade databricks-sdk azure-identity azure-keyvault-secrets python-dotenv

# COMMAND ----------

# DBTITLE 1,Import required libraries
import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam
import ipywidgets as widgets
from IPython.display import display, HTML, clear_output
from datetime import datetime
import json

# Import our custom service
from databricks_service import DatabricksGroupService

# COMMAND ----------

# DBTITLE 1,Initialize the service
service = DatabricksGroupService()

# COMMAND ----------

# DBTITLE 1,UI Components
# Create UI components
group_dropdown = widgets.Dropdown(
    options=[],
    description='Group:',
    disabled=False,
    layout=widgets.Layout(width='500px')
)

user_email = widgets.Text(
    value='',
    placeholder='user@example.com',
    description='User Email:',
    disabled=False,
    layout=widgets.Layout(width='500px')
)

add_button = widgets.Button(
    description='Add User to Group',
    button_style='success',
    tooltip='Click to add the user to the selected group',
    icon='plus'
)

remove_button = widgets.Button(
    description='Remove User from Group',
    button_style='danger',
    tooltip='Click to remove the user from the selected group',
    icon='minus'
)

refresh_button = widgets.Button(
    description='⟳ Refresh',
    button_style='info',
    tooltip='Refresh the group list',
    icon='refresh'
)

output = widgets.Output()

# COMMAND ----------

# DBTITLE 1,Load Groups
# Function to load groups into the dropdown
def load_groups():
    try:
        groups = service.get_available_groups()
        group_options = [(f"{g['display_name']} ({g['id']})", g['id']) for g in groups]
        group_dropdown.options = group_options
        if group_options:
            group_dropdown.value = group_options[0][1]
        return groups
    except Exception as e:
        with output:
            print(f"Error loading groups: {str(e)}")
        return []

# Initial load of groups
groups = load_groups()

# COMMAND ----------

# DBTITLE 1,Button Handlers
def on_add_button_clicked(b):
    with output:
        clear_output()
        email = user_email.value.strip()
        if not email or '@' not in email:
            print("Please enter a valid email address")
            return
            
        group_id = group_dropdown.value
        group_name = next((g['display_name'] for g in groups if g['id'] == group_id), group_id)
        
        print(f"Adding {email} to {group_name}...")
        success = service.add_user_to_group(email, group_id)
        
        if success:
            print(f"✅ Successfully added {email} to {group_name}")
        else:
            print(f"❌ Failed to add {email} to {group_name}. Check the logs for details.")


def on_remove_button_clicked(b):
    with output:
        clear_output()
        email = user_email.value.strip()
        if not email or '@' not in email:
            print("Please enter a valid email address")
            return
            
        group_id = group_dropdown.value
        group_name = next((g['display_name'] for g in groups if g['id'] == group_id), group_id)
        
        print(f"Removing {email} from {group_name}...")
        success = service.remove_user_from_group(email, group_id)
        
        if success:
            print(f"✅ Successfully removed {email} from {group_name}")
        else:
            print(f"❌ Failed to remove {email} from {group_name}. Check the logs for details.")


def on_refresh_button_clicked(b):
    with output:
        clear_output()
        print("Refreshing groups...")
        global groups
        groups = load_groups()
        print(f"Loaded {len(groups)} groups")

# Register button handlers
add_button.on_click(on_add_button_clicked)
remove_button.on_click(on_remove_button_clicked)
refresh_button.on_click(on_refresh_button_clicked)

# COMMAND ----------

# DBTITLE 1,Display the UI
# Create a form layout
form_items = [
    widgets.HBox([widgets.Label('User Management'), refresh_button]),
    widgets.HTML('<hr>'),
    user_email,
    group_dropdown,
    widgets.HBox([add_button, remove_button]),
    output
]

# Display the form
form = widgets.VBox(form_items, layout=widgets.Layout(align_items='stretch', width='100%'))
display(form)

# Initial message
with output:
    print("Welcome to Databricks Group Management Tool")
    print(f"You have access to {len(groups)} groups")
    print("\nTo get started:")
    print("1. Enter a user's email address")
    print("2. Select a group from the dropdown")
    print("3. Click 'Add' or 'Remove' to manage group membership")

# COMMAND ----------

# DBTITLE 1,View Group Members (Optional)
# This is an optional section to view current group members
view_members_button = widgets.Button(
    description='View Group Members',
    button_style='info',
    tooltip='Click to view current members of the selected group'
)

members_output = widgets.Output()

def on_view_members_clicked(b):
    with members_output:
        clear_output()
        group_id = group_dropdown.value
        group_name = next((g['display_name'] for g in groups if g['id'] == group_id), group_id)
        
        print(f"Loading members of {group_name}...")
        try:
            members = service.get_group_members(group_id)
            if members:
                print(f"\nMembers of {group_name}:")
                for i, member in enumerate(members, 1):
                    print(f"{i}. {member['display_name']} ({member['user_name']})")
            else:
                print(f"No members found in {group_name}")
        except Exception as e:
            print(f"Error loading group members: {str(e)}")

view_members_button.on_click(on_view_members_clicked)

display(widgets.VBox([
    widgets.HTML('<hr>'),
    widgets.HBox([view_members_button]),
    members_output
]))
