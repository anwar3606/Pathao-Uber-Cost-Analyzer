import re
import os
import csv
import json
import base64
import logging
import datetime
from GmailAPI import GmailAPI
from bs4 import BeautifulSoup


data_columns = [
    "id",
    "date",
    "price",
    "car_type",
    "start_time",
    "end_time",
    "start_location",
    "end_location",
    "time_time",
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

    def find_new_email_ids(self, emails):
        return list((set([e['id'] for e in emails]) - set(self.parsed_emails)))

    def get_emails_content(self, email_ids):
        already_parsed_emails = get_parsed_emails()

    def parser_uber_template_old(self, soup):
        data = {}
        data['price'] = soup.select_one(
            '.totalPrice.topPrice.tal.black').text.replace("Tk", "")
        data['distance'], data['time'], data['car_type'] = [
            td.text for td in soup.select('.tripInfo.tal')]
        (data['start_time'], data['start_location']), (data['end_time'], data['end_location']) = [
            x.text.split('|') for x in soup.select('.address.gray.vam.tal')]

        trip_data = [x.text for x in soup.select('.fareText')]
        for i in range(0, len(trip_data), 2):
            try:
                trip_data[i+2]
            except IndexError:
                continue
            key = re.findall(r'([A-Za-z]+( [A-Za-z]+)*)', trip_data[i])[0][0]
            key = key.lower().replace(" ", "_").strip()
            if key == "time":
                key = "trip_time"
            data[key] = trip_data[i+1]

        data['rider'] = soup.select_one(
            '.driverText.gray.tal').text.replace("You rode with ", '')
        return data

    def parser_uber_template_new(self, soup):
        data = {}
        page_text = soup.select_one('table').text
        page_text = page_text.replace('\xa0', '')

        if page_text.find("Here's the receipt for your canceled trip.") > 0:
            return data

        data['price'] = re.findall(
            r'(\d+\.\d+)', soup.select('.Uber18_p3.black.total_head')[1].text)[0]
        data['car_type'] = soup.select_one('.Uber18_text_p1.white').text

        elements_to_be_find = [
            r"(Trip Fare)\s+(.*?)\s",
            r"(Base fare)\s+(.*?)\s",
            r"(Distance)\s+(.*?)\s",
            r"(Time)\s+(.*?)\s",
            r"(Subtotal)\s+(.*?)\s",
            r"(Promotions)\s+(.*?)\s",
            r"(Credits)\s+(.*?)\s",
        ]

        for element in elements_to_be_find:
            try:
                res = re.findall(element, page_text)[0]
            except Exception:
                continue
            key = res[0].lower().replace(" ", "_")
            if key == "time":
                key = "trip_time"
            data[key] = res[1]

        res = re.findall(
            "(License Plate):\s(.*?)\s\s.*?(\w+)\s+(.*?)\|\s+(.*?)\s\s", page_text)[0]
        data['license_plate'], _, data['distance'], data['time'] = res[1:]

        res = re.findall("(\d\d:\d\d[ap]m)\s\s(.*?)\s\s", page_text)
        (data['start_time'], data['start_location']
         ), (data['end_time'], data['end_location']) = res

        data['rider'] = re.findall(r'You rode with (.*?)\s\s', page_text)[0]
        return data

    def parse_uber_emails(self, emails):
        for email in emails:
            data = {}
            data['id'] = email['id']
            try:
                email_body = email['payload']['parts'][0]['body']['data']
            except KeyError:
                logging.warning("No data found in email: %s", email['id'])
                yield data

            email_html = base64.urlsafe_b64decode(
                email_body).decode('ascii', 'ignore')

            data['date'] = datetime.datetime.fromtimestamp(
                int(email['internalDate'])/1e3).strftime("%c")

            soup = BeautifulSoup(email_html, 'html.parser')
            try:
                data.update(self.parser_uber_template_old(soup))
            except Exception:
                try:
                    data.update(self.parser_uber_template_new(soup))
                except Exception:
                    open("error/"+email['id']+".html", 'w',
                         encoding='utf-8').write(str(soup))

            yield data

    def cleanup_data(self, data):
        new_data = {}
        for key, value in data.items():
            if "canceled_trip" in key:
                new_data['canceled_trip'] = value

            elif key in ["promotions", "promotion"]:
                new_data['discounts'] = value

            else:
                if key in data_columns:
                    new_data[key] = value.strip()
                else:
                    logging.error("Invalid Data Found! %s:%s", key, value)

        for key, value in new_data.items():
            if key in [
                "price",
                "subtotal",
                "discounts",
                "canceled_trip"
                "trip_fare",
                "base_fare",
                "rounding",
                "credits",
                "change",
            ]:
                amount = re.findall(r'(\d+\.\d+)', value)[0]
                new_data[key] = amount

        return new_data

    def get_uber_emails(self):
        logging.info("Getting uber emails...")
        options = {
            "userId": 'me',
            "q": 'uber.bangladesh@uber.com',
        }

        emails = list(self.gmail_api.get_email_list(options=options))
        filtered_email_ids = self.find_new_email_ids(emails)
        emails_content = self.gmail_api.get_emails(filtered_email_ids)

        uber_trip_data = self.parse_uber_emails(emails_content)

        if os.path.exists('uber.csv'):
            write_header = False
        else:
            write_header = True

        with open("uber.csv", 'a', newline="") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=data_columns)
            if write_header:
                writer.writeheader()
            print("")
            for idx, data in enumerate(uber_trip_data):
                print("\rParsed:", idx+1)
                if len(data) > 1:
                    writer.writerow(self.cleanup_data(data))

                if data:
                    self.remember_email_id(data['id'])

        logging.info("Finished downloading emails, Total: %s", len(emails))


if __name__ == "__main__":

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    formater = logging.Formatter("[%(levelname)s] %(message)s")
    stream_handler.setFormatter(formater)
    file_handler = logging.FileHandler("parser_log.log")
    formater = logging.Formatter(
        "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s")
    file_handler.setFormatter(formater)
    file_handler.setLevel(logging.INFO)

    logging.basicConfig(
        level=logging.INFO,
        # format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
        handlers=[
            file_handler,
            stream_handler
        ])
    logging.getLogger("googleapiclient").propagate = False
    logging.getLogger("google").propagate = False

    parser = GmailParser()
    # emails = parser.get_pathao_emails()
    emails = parser.get_uber_emails()

    # open("16a9fd2f82d1f2ba.html", 'w').write(base64.urlsafe_b64decode(['payload']['parts'][0]['body']['data']).decode('ascii', 'ignore'))

    # email = parser.gmail_api.get_emails(['15e79bcf8cf58b3f'])
    # uber_trip_data = list(parser.parse_uber_emails(email))
    print("Finished")
