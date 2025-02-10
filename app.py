import base64
import logging
import random
import sys
from datetime import datetime
from flask import Flask, jsonify, request, url_for
import imaplib
import email
from email.header import decode_header
from pymongo import MongoClient
import multiprocessing
from redis import Redis
from rq import Queue
from dotenv import load_dotenv, dotenv_values
from email.utils import parsedate_to_datetime
import urllib.parse
import boto3
import os
import urllib
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
app = Flask(__name__)
load_dotenv()

env = dotenv_values(".env")

# MongoDB Configuration
MONGO_URI = env.get("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client["email_db"]
mailgun_emails = db["mailgun_emails"]
mailgun_email_participant = db["mailgun_email_participant"]

# Redis Configuration
redis_conn = Redis(host='localhost', port=6379, db=0)
queue = Queue(connection=redis_conn)

# DigitalOcean Spaces Configuration
DO_BUCKET = env.get("DO_BUCKET")
DO_DEFAULT_REGION = env.get("DO_DEFAULT_REGION")
DO_ACCESS_KEY_ID = env.get("DO_ACCESS_KEY_ID")
DO_SECRET_ACCESS_KEY = env.get("DO_SECRET_ACCESS_KEY")
DO_ENDPOINT = f"https://{DO_BUCKET}.{DO_DEFAULT_REGION}.digitaloceanspaces.com"
DO_CRM_PATH = DO_ENDPOINT  # Base path for uploaded files
DO_FOLDER = "email_attachments"





# # Configure logging
# logging.basicConfig(filename='app.log', level=logging.INFO,
#                     format='%(asctime)s - %(levelname)s - %(message)s')
# logging.basicConfig(level=logging.INFO)
def upload_to_do_spaces(filename, content, userName):
    try:
        # Initialize S3 client
        session = boto3.session.Session()

        client = session.client('s3',
                                region_name=DO_DEFAULT_REGION,
                                endpoint_url=DO_ENDPOINT,
                                aws_access_key_id=DO_ACCESS_KEY_ID,
                                aws_secret_access_key=DO_SECRET_ACCESS_KEY)
        username, domain = userName.split('@')
        sanitized_username = urllib.parse.quote(username, safe="")
        sanitized_domain = urllib.parse.quote(domain, safe="")
        randomperfix = str(random.randint(1, 10000000))
        key = f"emails/{sanitized_domain}/{sanitized_username}/attachments/{randomperfix}_{filename}"

        # Ensure content is not empty
        if not content:
            print(f"Error: Empty content provided for file {filename}")
            return None
        temple = "tempr/" + str(random.randint(1, 10000000)) + filename
        with open(temple, 'wb') as file:
            file.write(content)
            file.close()
        try:
            client.upload_file(temple,DO_FOLDER, key)
            os.remove(temple)
        except Exception as e:
            print(e)
            return None


        return f"{DO_ENDPOINT}/{DO_FOLDER}/{key}"

    except NoCredentialsError:
        print("Error: Credentials are missing. Check your access keys.")
        return None
    except PartialCredentialsError:
        print("Error: Incomplete credentials provided. Please check both access key and secret key.")
        return None
    except ClientError as e:
        # Catching AWS client errors (e.g., invalid permissions or bucket issues)
        print(f"ClientError: {e}")
        return None
    except Exception as e:
        # General exception handling for any unforeseen errors
        print(f"An error occurred while uploading {filename} to DigitalOcean Spaces: {e}")
        return None


# Save attachment function (no changes)
def save_attachment(part, imap_user):
    filename = part.get_filename()
    if filename:
        decoded_filename = decode_header(filename)[0][0]
        if isinstance(decoded_filename, bytes):
            filename = decoded_filename.decode(errors="ignore")
        elif isinstance(decoded_filename, str):
            filename = decoded_filename
        file_content = part.get_payload(decode=True)
        if not file_content:
            return None
        attachment_url = upload_to_do_spaces(filename, file_content, imap_user)
        return attachment_url
    return None


# Update email fetching function to handle the new date format
def fetch_emails(page, per_page, imap_server, imap_user, imap_password, last_email_date=None):
    mail = imaplib.IMAP4_SSL(imap_server)
    mail.login(imap_user, imap_password)
    mail.select("inbox")
    print('last email date: ', last_email_date)
    if last_email_date:
        # Use the ISO 8601 format for the last_email_date
        search_date = last_email_date.strftime("%d-%b-%Y")  # E.g., 04-Feb-2025
        _, messages = mail.search(None, f"SINCE {search_date}")
        # _, messages = mail.search(None, "ALL")
    else:
        _, messages = mail.search(None, "ALL")

    email_ids = messages[0].split()
    email_ids.reverse()

    if page < 0:
        page = 1
    start = (page - 1) * per_page
    end = start + per_page
    selected_email_ids = email_ids[start:end]
    emails = []

    for email_id in selected_email_ids:
        _, msg_data = mail.fetch(email_id, "(RFC822)")
        raw_email = msg_data[0][1]
        msg = email.message_from_bytes(raw_email)

        message_id = msg["Message-ID"]

        if mailgun_emails.find_one({"message_id": message_id}):
            print(f"Skipping duplicate email: {message_id}")
            continue

        subject, encoding = decode_header(msg["Subject"])[0]
        if isinstance(subject, bytes):
            subject = subject.decode(encoding if encoding else "utf-8")

        sender = msg["From"]
        date = msg["Date"]
        to_email = msg["To"]
        cc = msg.get("Cc", "")
        bcc = msg.get("Bcc", "")
        in_reply_to = msg.get("In-Reply-To")
        references = msg.get("References")
        thread_id = references if references else message_id

        attachments = []
        body_plain = ""
        body_html = ""

        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                if content_type == "text/plain":
                    body_plain = part.get_payload(decode=True).decode(errors="ignore")
                elif content_type == "text/html":
                    body_html = part.get_payload(decode=True).decode(errors="ignore")
                elif part.get_filename():
                    attachment_url = save_attachment(part, imap_user)
                    if attachment_url:
                        attachments.append(attachment_url)
        else:
            body_plain = msg.get_payload(decode=True).decode(errors="ignore")

        # Parse the email date
        try:
            # Parse email date using the ISO 8601 format
            date_format = "%a, %d %b %Y %H:%M:%S %z"
            date = datetime.strptime(date, date_format)
        except (TypeError, ValueError) as e:
            print(f"Error parsing date for email {message_id}: {e}")
            date = datetime.now()  # Fallback to current time if parsing fails

        is_first = 0 if in_reply_to or references else 1
        email_data = {
            "to": to_email,
            "email_account_id": None,
            "message_id": message_id,
            "email_date": date.isoformat(),  # Save the date as ISO 8601 string
            "from": sender,
            "subject": subject,
            "body_plain": body_plain,
            "attachment_count": len(attachments),
            "attachments": attachments,
            "created_at": None,
            "updated_at": None,
            "body_html": body_html,
            "content_length": len(body_plain) + len(body_html),
            "recipient": to_email,
            "user_agent": None,
            "received": None,
            "cc": cc,
            "bcc": bcc,
            "to_display": to_email,
            "stripped_text": body_plain,
            "status": "received",
            "lead_id": None,
            "thread_id": thread_id,
            "in_reply_to": in_reply_to,
            "is_first": is_first,
            "received_on": imap_user
        }

        mailgun_emails.insert_one(email_data)
        participant_data = {
            "message_id": message_id,
            "email_account_id": None,
            "email": sender,
            "box_type": "inbox",
            "message_uid": email_id,
            "process_id": None,
            "participant_id": None,
            "search_box": "Inbox",
            "read_by": None,
            "read_at": None,
            "stripped_text": body_plain,
            "subject": subject,
            "email_date": date.isoformat(),  # Save the date as ISO 8601 string
            "sent_by": None,
            "in_reply_to": in_reply_to,
            "thread_id": thread_id,
            "is_first": is_first,
            "date_time": date,
            "updated_at": None,
            "created_at": None,
            "received_on": imap_user
        }
        mailgun_email_participant.insert_one(participant_data)

    mail.logout()
    print(len(email_ids))
    return len(email_ids) > end


# Same job function
def fetch_emails_job(page, per_page, imap_server, imap_user, imap_password, last_email_date=None):

    has_more = fetch_emails(page, per_page, imap_server, imap_user, imap_password, last_email_date)

    if has_more:
        print(f"Dispatching next page: {page + 1}")
        queue.enqueue(fetch_emails_job, page + 1, per_page, imap_server, imap_user, imap_password, last_email_date)

    return {"status": "completed", "page": page}


# Fetch emails endpoint (adjusted)
@app.route("/fetch-emails", methods=["GET"])
def fetch_emails_endpoint():
    page = int(request.args.get("page", -1))
    per_page = 10

    # Get IMAP credentials from request parameters
    imap_server = request.args.get("imap_server")
    imap_user = request.args.get("imap_user")
    imap_password = request.args.get("imap_password")
    imap_password = base64.b64decode(imap_password).decode("utf-8")

    last_email_date = None
    if page == -1:
        last_email = mailgun_emails.find_one(
            {"received_on": imap_user},
            sort=[("email_date", -1)]
        )
        print(last_email,'last email')

        if last_email:
            email_date = last_email.get("email_date")
            if isinstance(email_date, str):

                last_email_date = datetime.fromisoformat(email_date)
            elif isinstance(email_date, datetime):
                last_email_date = email_date
            else:
                last_email_date = datetime.now()  # Fallback if no valid date is found

    if not imap_server or not imap_user or not imap_password:
        return jsonify({"error": "Missing IMAP credentials"}), 400

    job = queue.enqueue("myapp.fetch_emails_job", page, per_page, imap_server, imap_user, imap_password, last_email_date)
    return jsonify({"message": "Email fetching started", "job_id": job.id})


@app.route("/job-status/<job_id>", methods=["GET"])
def job_status(job_id):
    job = queue.fetch_job(job_id)
    if job:
        return jsonify({"status": job.get_status(), "result": job.result})
    return jsonify({"status": "not found"})


if __name__ == "__main__":
    multiprocessing.set_start_method("spawn", force=True)
    app.run(debug=True)