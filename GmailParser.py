import os
import logging
from GmailAPI import GmailAPI


data_columns = [
    "id",
    "date",
    "price",
    "car_type",
    "start_time",
    "end_time",
    "start_location",
    "end_location",
    "trip_time",
    "time_cost",
    "distance",
    "rider",
    "license_plate",
    "subtotal",
    "discounts",
    "canceled_trip",
    "trip_fare",
    "base_fare",
    "rounding",
    "credits",
    "change",
]


class GmailParser:
    def __init__(self):
        self.gmail_api = GmailAPI()
        self.parsed_emails = self.get_parsed_emails()

    def get_parsed_emails(self):
        if os.path.exists('parsed_emails.txt'):
            ids = [line.strip() for line in open("parsed_emails.txt").readlines()
                   if line.strip()]
            self.remember_email_id_file = open('parsed_emails.txt', 'a')
            return ids
        else:
            self.remember_email_id_file = open('parsed_emails.txt', 'w')
            return []

    def save_parsed_emails(self):
        with open("parsed_emails.txt", 'r') as target:
            target.write("\n".join(self.parsed_emails))

    def remember_email_id(self, email_id):
        self.remember_email_id_file.write(email_id + "\n")
        self.parsed_emails.append(email_id)

    def find_new_email_ids(self, emails):
        return list((set([e['id'] for e in emails]) - set(self.parsed_emails)))


