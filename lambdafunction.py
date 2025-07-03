import json
import os
from detailsharsham import TelegramBot # Import the bot logic class

# Initialize bot instance globally to persist across warm invocations.
# This avoids re-initializing S3/DynamoDB clients and re-loading Excel files
# on every invocation if the Lambda container is reused.
TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")

# Basic check for token at global scope
if not TOKEN:
    print("FATAL ERROR: TELEGRAM_BOT_TOKEN environment variable not set. Bot will not function.")
    # In a production scenario, you might want to raise an exception here
    # or handle this more robustly during deployment.

try:
    bot = TelegramBot(token=TOKEN)
except ValueError as ve:
    print(f"FATAL ERROR during bot initialization: {ve}")
    # Re-raise or handle if token is missing
    raise

def lambda_handler(event, context):
    """
    Process webhook updates from Telegram and handle user interactions.
    This is the main entry point for the AWS Lambda function.
    """
    print(f"📩 Incoming Event: {json.dumps(event, indent=2)}")

    try:
        # --- Basic Input Validation ---
        if "body" not in event:
            print("❌ Error: Invalid request format - 'body' not in event.")
            return {"statusCode": 400, "body": json.dumps({"error": "Invalid request format: 'body' key missing."})}

        # Attempt to parse the JSON body
        try:
            body = json.loads(event["body"])
        except json.JSONDecodeError:
            print("❌ Error: Could not decode JSON body.")
            return {"statusCode": 400, "body": json.dumps({"error": "Invalid JSON in request body."})}

        message = body.get("message", {})

        # Extract chat_id - crucial for all bot interactions
        chat_id_raw = message.get("chat", {}).get("id")
        if chat_id_raw is None:
            print("❌ Error: Could not get chat_id from message. Skipping processing.")
            return {"statusCode": 400, "body": json.dumps({"error": "Chat ID not found in message."})}
        
        chat_id = int(chat_id_raw) # Convert to integer as DynamoDB PK is Number type

        user_message_text = message.get("text", "").strip().lower() # Get message text, clean it

        # --- CENTRALIZED AUTHENTICATION/VERIFICATION LOGIC ---
        # All incoming messages go through this verification gate first.
        if not bot.is_user_verified(chat_id):
            print(f"User {chat_id} is NOT verified. Initiating/continuing verification flow.")
            
            # Case 1: User sends their contact info for verification
            if "contact" in message and message["contact"].get("user_id") == chat_id:
                user_phone_number = message["contact"]["phone_number"]
                print(f"Attempting to verify contact for {chat_id} with number {user_phone_number}.")
                if bot.verify_contact(chat_id, user_phone_number):
                    bot.send_message(chat_id, "✅ You are now verified! You can now use the bot and commands like `/housekeeping`.")
                    # After successful verification, we return. The user can then send commands.
                    return {"statusCode": 200, "body": json.dumps({"message": "User verified successfully."})}
                else:
                    bot.send_message(chat_id, "🚫 Verification failed. Your number is not recognized by our system.")
                    return {"statusCode": 200, "body": json.dumps({"message": "Verification failed: unrecognized number."})}
            else:
                # Case 2: User is not verified and did not send contact info (e.g., first message, or command without verification)
                print(f"User {chat_id} not verified and no contact provided. Requesting contact.")
                bot.request_contact(chat_id)
                return {"statusCode": 200, "body": json.dumps({"message": "Requesting contact info for unverified user."})}

        # --- Verified User Command Handling ---
        # If the code reaches this point, the user IS verified.

        # Handle the /start command specifically for verified users
        if user_message_text == "/start":
            bot.send_message(chat_id, "✅ Welcome back! You are already verified. Use `/help` to see available commands.")
            return {"statusCode": 200, "body": json.dumps({"message": "Verified user sent /start."})}

        # If no specific text message (e.g., photo, sticker from verified user)
        if not user_message_text:
            bot.send_message(chat_id, "Please send a text command. You can use `/help` to see the options.")
            return {"statusCode": 200, "body": json.dumps({"message": "Non-text message received from verified user."})}

        # Normalize the command (remove leading '/' if present)
        command = user_message_text[1:] if user_message_text.startswith('/') else user_message_text

        # List of recognized commands/categories
        # It's good practice to keep document commands separate from contact categories
        contact_categories = ["auto", "housekeeping", "milkman", "authorities", "electrician", "plumber","services","paperboy","shops"] 
        document_commands = ["wastemanagementpdf"] # <--- NEW: Document command

        if command in contact_categories:
            print(f"Verified user {chat_id} requested contact category: {command}")
            bot.get_contacts(chat_id, command)
        elif command == "wastemanagementpdf": # <--- NEW: Handle the PDF command
            print(f"Verified user {chat_id} requested waste management PDF.")
            bot.send_document(chat_id, bot.WASTE_MANAGEMENT_PDF_KEY, "Here is your Waste Management Guide.")
        elif command == "help":
            print(f"Verified user {chat_id} requested help.")
            # Update the help text to include new commands
            help_text = (
                "📌 **Available Commands:**\n"
                "- `/housekeeping`\n"
                "- `/auto`\n"
                "- `/milkman`\n"
                "- `/authorities`\n"
                "- `/electrician`\n"
                "- `/plumber`\n"
                "- `/services`\n"
                "- `/shops`\n"
                "- `/paperboy`\n"
                "- `/wastemanagementpdf` (for the PDF guide)\n\n" # <--- NEW: Help text for PDF
                "You can type the command with or without the `/`."
            )
            bot.send_message(chat_id, help_text)
        else:
            print(f"Verified user {chat_id} sent unrecognized command: '{user_message_text}'")
            bot.send_message(chat_id, f"🤖 Unrecognized command: `{user_message_text}`. Please use `/help` to see the available options.")

        return {"statusCode": 200, "body": json.dumps({"message": "Command processed for verified user."})}

    except Exception as e:
        print(f"❌ Unhandled Error in lambda_handler: {e}")
        import traceback
        print(traceback.format_exc()) # Print the full stack trace for detailed debugging
        
        try:
            # Attempt to send a polite error message back to the user
            error_chat_id = None
            if "body" in event:
                try:
                    body = json.loads(event["body"])
                    error_chat_id = body.get("message", {}).get("chat", {}).get("id")
                except json.JSONDecodeError:
                    pass # Body might not be valid JSON, ignore
            
            if error_chat_id:
                bot.send_message(int(error_chat_id), "Sorry, an unexpected error occurred. The issue has been logged. Please try again later.")
        except Exception as inner_e:
            print(f"⚠️ Failed to send error message to user: {inner_e}")
            
        return {"statusCode": 500, "body": json.dumps({"error": "Internal Server Error"})}