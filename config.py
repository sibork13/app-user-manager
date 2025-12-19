from pydantic import BaseSettings, Field, SecretStr
from typing import Optional

class Settings(BaseSettings):
    # Azure Key Vault settings
    KEY_VAULT_NAME: str = Field(..., env='KEY_VAULT_NAME')
    SERVICE_PRINCIPAL_CLIENT_ID: str = Field(..., env='SERVICE_PRINCIPAL_CLIENT_ID')
    SERVICE_PRINCIPAL_SECRET: SecretStr = Field(..., env='SERVICE_PRINCIPAL_SECRET')
    TENANT_ID: str = Field(..., env='TENANT_ID')
    
    # Databricks workspace URL (e.g., https://adb-1234567890123456.16.azuredatabricks.net/)
    DATABRICKS_WORKSPACE_URL: str = Field(..., env='DATABRICKS_WORKSPACE_URL')
    
    # Logging configuration
    LOG_LEVEL: str = Field('INFO', env='LOG_LEVEL')
    
    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'
        
settings = Settings()
