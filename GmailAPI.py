from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import logging


# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']


class GmailAPI():
    def __init__(self):
        """Shows basic usage of the Gmail API.
        Lists the user's Gmail labels.
        """
        creds = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                logging.info("Refreshing OAuth Token, Check browser...")
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.pickle', 'wb') as token:
                pickle.dump(creds, token)

        self.service = build('gmail', 'v1', credentials=creds)

    def get_email_list(self, options, pageToken=None):
        logging.info("Getting emails, Options: %s", options)

        response = self.service.users().messages().list(**options).execute()
        emails = response['messages']

        # if response.get('nextPageToken'):
        #     options['pageToken'] = response['nextPageToken']
        #     next_page_response = self.get_email_list(options)
        #     emails.extend(next_page_response)

        return emails

    def get_emails(self, email_ids):
        logging.info("Downloading emails: %s", len(email_ids))
        response_array = []
        exception_array = []

        def callback_callable(request_idx, response, exception):
            if exception:
                exception_array.append(request_idx) if request_idx not in exception_array else None
            else:
                response_array.append(response)

        batch = self.service.new_batch_http_request(callback=callback_callable)

        [batch.add(self.service.users().messages().get(
            userId='me', id=e_id), request_id=e_id) for e_id in email_ids]

        batch.execute()

        if exception_array:
            logging.warning("Exception Handling for: %s", exception_array)
            response_array.extend(self.get_emails(exception_array))

        return response_array
