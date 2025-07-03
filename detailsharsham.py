import pandas as pd
import re
import json
import os
import requests
import boto3
from datetime import datetime # Import datetime for timestamps

class TelegramBot:
    """
    A class to encapsulate all the logic for the Telegram bot, using the
    correct AWS services (S3, DynamoDB) for persistent storage.
    """
    def __init__(self, token):
        """
        Initializes the bot by setting up AWS clients and loading data.
        """
        if not token:
            raise ValueError("Telegram Bot Token is required and cannot be empty.")
        self.TOKEN = token
        self.BASE_URL = f"https://api.telegram.org/bot{self.TOKEN}/"
        
        # --- AWS & Environment Setup ---
        # Get S3 Bucket name from environment variable
        self.S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")
        # Get DynamoDB Table name from environment variable, with a fallback default
        self.DYNAMODB_TABLE_NAME = os.environ.get("VERIFIED_USERS_TABLE_NAME", "TelegramVerifiedUsers")
        
        # Define S3 object keys for Excel files and the new PDF
        self.ALLOWED_USERS_KEY = "allowed_users.xlsx"
        self.CONTACTS_KEY = "contacts.xlsx"
        self.WASTE_MANAGEMENT_PDF_KEY = "waste_management.pdf" # <--- NEW: PDF file key

        # Initialize AWS clients
        try:
            self.s3_client = boto3.client("s3")
            self.dynamodb = boto3.resource("dynamodb")
            self.verified_users_table = self.dynamodb.Table(self.DYNAMODB_TABLE_NAME)
            print(f"AWS clients initialized. S3 Bucket: {self.S3_BUCKET_NAME}, DynamoDB Table: {self.DYNAMODB_TABLE_NAME}")
        except Exception as e:
            print(f"❌ FATAL ERROR: Could not initialize AWS clients or DynamoDB table: {e}")
            raise # Re-raise to stop initialization if AWS clients fail

        # Load data upon initialization
        self.allowed_numbers = self.load_allowed_users_from_s3()
        self.contacts_df = self.load_contacts_from_s3()

    @staticmethod
    def normalize_phone(phone: str) -> str:
        """
        Formates a phone number into the "+91 XXXXX XXXXX" format.
        Handles various input formats to clean and standardize.
        """
        # Remove all non-digit characters
        cleaned = re.sub(r'\D', '', str(phone))

        if len(cleaned) == 10: # Assumes Indian 10-digit number
            return f"+91 {cleaned[:5]} {cleaned[5:]}"
        elif len(cleaned) == 12 and cleaned.startswith("91"): # Already includes 91 prefix
            return f"+91 {cleaned[2:7]} {cleaned[7:]}"
        elif len(cleaned) == 13 and cleaned.startswith("+91"): # Already includes +91 prefix
            return f"+91 {cleaned[3:8]} {cleaned[8:]}"
        
        # If none of the above, return as is (might be international or malformed)
        print(f"WARNING: Phone number '{phone}' could not be normalized to +91 format. Returning as is: {cleaned}")
        return cleaned

    def load_allowed_users_from_s3(self) -> set:
        """
        Loads allowed phone numbers from an Excel file stored in S3.
        Downloads the file to Lambda's /tmp directory first.
        """
        if not self.S3_BUCKET_NAME:
            print("❌ Error: S3_BUCKET_NAME environment variable not set. Cannot load allowed users from S3.")
            return set()
        
        file_path = f"/tmp/{self.ALLOWED_USERS_KEY}"
        try:
            self.s3_client.download_file(self.S3_BUCKET_NAME, self.ALLOWED_USERS_KEY, file_path)
            df = pd.read_excel(file_path, dtype=str)
            df.columns = df.columns.str.strip() # Strip whitespace from column names
            
            if "Number" not in df.columns:
                print(f"❌ Error: 'Number' column not found in {self.ALLOWED_USERS_KEY} in S3 bucket '{self.S3_BUCKET_NAME}'.")
                return set()
            
            formatted_numbers = set(df["Number"].apply(self.normalize_phone))
            print(f"✅ Loaded {len(formatted_numbers)} allowed numbers from S3: {self.S3_BUCKET_NAME}/{self.ALLOWED_USERS_KEY}")
            return formatted_numbers
        except self.s3_client.exceptions.NoSuchBucket:
            print(f"❌ Error: S3 Bucket '{self.S3_BUCKET_NAME}' does not exist or is not accessible.")
            return set()
        except self.s3_client.exceptions.NoSuchKey:
            print(f"❌ Error: S3 object '{self.ALLOWED_USERS_KEY}' not found in bucket '{self.S3_BUCKET_NAME}'.")
            return set()
        except Exception as e:
            print(f"❌ General error loading allowed users from S3: {e}")
            return set()

    def load_contacts_from_s3(self) -> pd.DataFrame:
        """
        Loads contact details from an Excel file stored in S3.
        Downloads the file to Lambda's /tmp directory first.
        """
        if not self.S3_BUCKET_NAME:
            print("❌ Error: S3_BUCKET_NAME environment variable not set. Cannot load contacts from S3.")
            return pd.DataFrame(columns=["Category", "Name", "Number"]) # Return empty DataFrame
        
        file_path = f"/tmp/{self.CONTACTS_KEY}"
        try:
            self.s3_client.download_file(self.S3_BUCKET_NAME, self.CONTACTS_KEY, file_path)
            
            print(f"DEBUG: Successfully downloaded {self.CONTACTS_KEY} to {file_path}")
            
            df = pd.read_excel(file_path, dtype=str)
            
            print(f"DEBUG: Original columns read by pandas: {df.columns.tolist()}")
            
            df.columns = df.columns.str.strip() # Strip whitespace from column names
            
            print(f"DEBUG: Columns after stripping whitespace: {df.columns.tolist()}")
            
            print(f"✅ Loaded {len(df)} contacts from S3: {self.S3_BUCKET_NAME}/{self.CONTACTS_KEY}")
            return df
        except self.s3_client.exceptions.NoSuchBucket:
            print(f"❌ Error: S3 Bucket '{self.S3_BUCKET_NAME}' does not exist or is not accessible.")
            return pd.DataFrame(columns=["Category", "Name", "Number"])
        except self.s3_client.exceptions.NoSuchKey:
            print(f"❌ Error: S3 object '{self.CONTACTS_KEY}' not found in bucket '{self.S3_BUCKET_NAME}'.")
            return pd.DataFrame(columns=["Category", "Name", "Number"])
        except Exception as e:
            print(f"❌ General error loading contacts from S3: {e}")
            return pd.DataFrame(columns=["Category", "Name", "Number"])

    def send_message(self, chat_id, text):
        """Sends a Markdown-formatted message via the Telegram API."""
        url = f"{self.BASE_URL}sendMessage"
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
        try:
            response = requests.post(url, json=payload)
            response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
            print(f"✅ Sent message to {chat_id}. Status Code: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"❌ Error sending message to {chat_id}: {e}")

    def request_contact(self, chat_id):
        """Asks the user to share their phone number with a custom keyboard button."""
        payload = {
            "chat_id": chat_id,
            "text": "📱 Please share your contact number to proceed. (Tap the 'Share Contact' button below)",
            "reply_markup": {
                "keyboard": [[{"text": "Share Contact", "request_contact": True}]],
                "resize_keyboard": True,
                "one_time_keyboard": True
            }
        }
        try:
            response = requests.post(f"{self.BASE_URL}sendMessage", json=payload)
            response.raise_for_status()
            print(f"✅ Requested contact from {chat_id}. Status Code: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"❌ Error requesting contact from {chat_id}: {e}")

    def is_user_verified(self, chat_id: int) -> bool:
        """
        Checks DynamoDB to see if a user is verified using their chat_id as the primary key.
        """
        print(f"DEBUG: Checking verification status for chat_id: {chat_id} in table '{self.DYNAMODB_TABLE_NAME}'")
        try:
            response = self.verified_users_table.get_item(
                Key={'chat_id': chat_id} # Use 'chat_id' as the key name, value is int
            )
            is_verified = 'Item' in response
            print(f"DEBUG: DynamoDB GetItem response for {chat_id}: {response}")
            print(f"DEBUG: User {chat_id} verification status: {is_verified}")
            return is_verified
        except Exception as e:
            print(f"❌ Error checking user verification status for {chat_id} in DynamoDB: {e}")
            import traceback
            print(traceback.format_exc())
            return False

    def save_verified_user(self, chat_id: int, phone_number: str):
        """
        Saves a verified user's chat_id and phone_number to DynamoDB.
        Includes a timestamp for auditing.
        """
        try:
            self.verified_users_table.put_item(
                Item={
                    'chat_id': chat_id, # Must match Partition Key name and type in DynamoDB
                    'phone_number': phone_number,
                    'verified_at': datetime.now().isoformat()
                }
            )
            print(f"✅ Saved user {chat_id} with phone {phone_number} to DynamoDB.")
        except Exception as e:
            print(f"❌ Error saving user {chat_id} to DynamoDB: {e}")
            import traceback
            print(traceback.format_exc()) # Print stack trace for debugging

    def verify_contact(self, chat_id: int, phone_number: str) -> bool:
        """
        Verifies if the provided phone number exists in the allowed numbers list.
        If it does, the user's chat_id and phone_number are stored in DynamoDB.
        """
        formatted_number = self.normalize_phone(phone_number)
        print(f"DEBUG: Attempting to verify contact {formatted_number} for chat_id: {chat_id}")
        if formatted_number in self.allowed_numbers:
            self.save_verified_user(chat_id, formatted_number)
            return True
        else:
            print(f"DEBUG: Phone number {formatted_number} not in allowed_numbers for chat_id: {chat_id}")
            return False

    def get_contacts(self, chat_id, category):
        """
        Fetches and sends contact details for a specific category from the loaded DataFrame.
        """
        try:
            # Filter contacts DataFrame by category (case-insensitive)
            # Ensure 'Category' column exists and is handled as string before lowercasing
            if "Category" not in self.contacts_df.columns:
                print("❌ ERROR: 'Category' column not found in contacts_df. Cannot filter.")
                self.send_message(chat_id, f"❌ Internal error: Contact data is missing the 'Category' column.")
                return

            # Strip spaces from the values in the 'Category' column before comparing
            filtered = self.contacts_df[self.contacts_df["Category"].astype(str).str.strip().str.lower() == category.lower()]
            
            if filtered.empty:
                self.send_message(chat_id, f"🚫 No contacts found for `{category.capitalize()}`. Please check the command or spelling!")
                return

            message_parts = [f"📌 **{category.capitalize()} Contacts:**\n\n"] # Added extra newline for spacing
            for _, row in filtered.iterrows():
                name = row.get('Name', 'N/A')
                # Ensure 'Number' column exists before trying to get it
                number = row.get('Number', 'N/A') 
                
                # If 'Number' column was not found, 'number' will be 'N/A'
                if number == 'N/A':
                    print(f"WARNING: 'Number' column missing for a row in category '{category}'. Contact name: {name}")

                formatted_number = self.normalize_phone(number)
                message_parts.append(f"• **{name}**\n  📞 `{formatted_number}`\n\n") # Added extra newline for spacing
            
            self.send_message(chat_id, "".join(message_parts).strip())
        except Exception as e:
            print(f"❌ Error in get_contacts for '{category}': {e}")
            import traceback
            print(traceback.format_exc()) # Print full traceback for deeper debugging
            self.send_message(chat_id, f"❌ An internal error occurred while fetching details for `{category.capitalize()}`.")

    def send_document(self, chat_id, file_key, caption=None): # <--- NEW METHOD FOR SENDING DOCUMENTS
        """
        Downloads a file from S3 and sends it as a document via Telegram.
        """
        local_file_path = f"/tmp/{os.path.basename(file_key)}"
        try:
            print(f"Attempting to download {file_key} from S3 bucket {self.S3_BUCKET_NAME} to {local_file_path}")
            self.s3_client.download_file(self.S3_BUCKET_NAME, file_key, local_file_path)
            print(f"Successfully downloaded {file_key}.")

            url = f"{self.BASE_URL}sendDocument"
            
            # Open the file in binary read mode
            with open(local_file_path, 'rb') as f:
                files = {'document': f}
                data = {'chat_id': chat_id}
                if caption:
                    data['caption'] = caption

                response = requests.post(url, data=data, files=files)
                response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
                print(f"✅ Sent document {file_key} to {chat_id}. Status Code: {response.status_code}")
        except self.s3_client.exceptions.NoSuchBucket:
            print(f"❌ Error: S3 Bucket '{self.S3_BUCKET_NAME}' does not exist or is not accessible.")
            self.send_message(chat_id, "❌ Error: The file storage bucket could not be found.")
        except self.s3_client.exceptions.NoSuchKey:
            print(f"❌ Error: S3 object '{file_key}' not found in bucket '{self.S3_BUCKET_NAME}'.")
            self.send_message(chat_id, f"❌ Error: The requested document '{file_key}' was not found.")
        except requests.exceptions.RequestException as e:
            print(f"❌ Error sending document {file_key} to {chat_id}: {e}")
            self.send_message(chat_id, "❌ An error occurred while sending the document via Telegram.")
        except Exception as e:
            print(f"❌ General error in send_document for '{file_key}': {e}")
            import traceback
            print(traceback.format_exc())
            self.send_message(chat_id, "❌ An unexpected error occurred while processing your document request.")
        finally:
            # Clean up the downloaded file from /tmp to save space
            if os.path.exists(local_file_path):
                os.remove(local_file_path)
                print(f"Cleaned up {local_file_path}")