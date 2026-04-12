"""
Run this script ONCE, locally, to obtain a refresh token for OneDrive access.

Steps:
  1. Make sure AZURE_CLIENT_ID (and optionally AZURE_TENANT_ID) are in your .env file.
  2. Run:  python setup_onedrive_auth.py
  3. Follow the printed instructions (visit a URL, enter a code).
  4. Copy the printed ONEDRIVE_REFRESH_TOKEN value into your .env file.
  5. When setting up GitHub Actions later, add it as a repository secret too.

You should only need to do this once. The refresh token lasts a long time (typically 90 days
of inactivity for personal Microsoft accounts). personal_log_etl_cloud.py will automatically
rotate it on each run.
"""

import os
import msal
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
# For personal Microsoft accounts (@outlook.com, @hotmail.com, etc.) use "consumers".
# For a work/school account, use your specific tenant ID.
# "common" accepts both — a safe default if you're unsure.
TENANT_ID = os.getenv("AZURE_TENANT_ID", "common")

SCOPES = [
    "https://graph.microsoft.com/Files.ReadWrite",
    "https://graph.microsoft.com/User.Read",
    # Note: do NOT include "offline_access" here — MSAL adds it automatically
    # when using a PublicClientApplication, and it will error if you list it
    # explicitly alongside full-URI scopes.
]

if not CLIENT_ID:
    raise SystemExit("AZURE_CLIENT_ID is not set. Add it to your .env file and try again.")

app = msal.PublicClientApplication(
    client_id=CLIENT_ID,
    authority=f"https://login.microsoftonline.com/{TENANT_ID}",
)

print("Starting device code flow...\n")
flow = app.initiate_device_flow(scopes=SCOPES)

if "user_code" not in flow:
    raise SystemExit(f"Failed to start device flow: {flow.get('error_description', flow)}")

# This prints something like:
#   "To sign in, use a web browser to open https://microsoft.com/devicelogin
#    and enter the code XXXXXXXX to authenticate."
print(flow["message"])
print()

result = app.acquire_token_by_device_flow(flow)  # blocks until the user completes login

if "access_token" not in result:
    raise SystemExit(f"Authentication failed: {result.get('error_description', result)}")

print("Authentication successful!\n")
print("Add the following line to your .env file:\n")
print(f"APARTMENT_ONEDRIVE_REFRESH_TOKEN={result['refresh_token']}")
