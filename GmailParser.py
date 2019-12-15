import re
import os
import base64
import logging
import datetime
from GmailAPI import GmailAPI
from bs4 import BeautifulSoup
import csv


class GmailParser:
    def __init__(self):
        self.gmail_api = GmailAPI()
        self.parsed_emails = self.get_parsed_emails()

    def get_parsed_emails(self):
        if os.path.exists('parsed_emails.txt'):
            return [line.strip() for line in open("parsed_emails.txt").readlines()
                    if line.strip()]
        else:
            return []

    def save_parsed_emails(self):
        with open("parsed_emails.txt", 'r') as target:
            target.write("\n".join(self.parsed_emails))

    def find_new_email_ids(self, emails):
        return list((set([e['id'] for e in emails]) - set(self.parsed_emails)))

    def get_emails_content(self, email_ids):
        already_parsed_emails = get_parsed_emails()


    def parser_uber_template_old(self, soup):
        data = {}
        data['price'] = soup.select_one(
            '.totalPrice.topPrice.tal.black').text.replace("Tk", "")
        data['distance_traveled'], data['trip_time'], data['car_type'] = [
            td.text for td in soup.select('.tripInfo.tal')]
        (data['start_time'], data['start_location']), (data['end_time'], data['end_location']) = [
            x.text.split('|') for x in soup.select('.address.gray.vam.tal')]

        trip_data = [x.text for x in soup.select('.fareText')]
        for i in range(0, len(trip_data), 2):
            try:
                trip_data[i+2]
            except IndexError:
                continue
            data[re.findall(r'([A-Za-z]+( [A-Za-z]+)*)',
                            trip_data[i])[0][0]] = trip_data[i+1]


        data['rider'] = soup.select_one('.driverText.gray.tal').text.replace("You rode with ",'')
        return data

    def parser_uber_template_new(self, soup):
        data = {}
        data['price'] = re.findall(r'(\d+\.\d+)',soup.select('.Uber18_p3.black.total_head')[1].text)[0]
        data['car_type'] = soup.select_one('.Uber18_text_p1.white').text
        page_text = soup.select_one('table').text

        elements_to_be_find = [
            r"(Trip Fare)\s+(.*?)\s",
            r"(Base fare)\s+(.*?)\s",
            r"(Distance)\s+(.*?)\s",
            r"(Time)\s+(.*?)\s",
            r"(Subtotal)\s+(.*?)\s",
            r"(Promotions)\s+(.*?)\s",
            r"(Credits)\s+(.*?)\s",
            # r"(You rode with)\s(.*)\s\s",
            #License Plate: LA116523 \xa0 Moto  0.01 km | 4 min
            # r"(License Plate):(.*?)\s"
            # re.findall("(License Plate):\s(.*?)\s.*?(\w+)\s+(.*?)\|\s+(.*?)\s\s", my_string)
            # [('License Plate', 'HA466840', 'Moto', '8.74 km ', '26 min')]

            # re.findall("(\d\d:\d\d[ap]m)\s\s(.*?)\s\s", my_string)
            # [('02:41pm', 'Kobi Faruk Sarani, ...angladesh'), ('03:08pm', 'Rubaayat Rahman Bha...angladesh')]
            
            #01:02pm  Progoti Sarani - Debogram Rd, Dhaka, Bangladesh  01:06pm  Progoti Sarani - Debogram Rd, Dhaka, Bangladesh  Invite 
        ]

        for element in elements_to_be_find:
            try:
                res = re.findall(element, page_text)[0]
            except Exception:
                continue
            data[res[0]] = res[1]

        
        res = re.findall("(License Plate):\s(.*?)\s.*?(\w+)\s+(.*?)\|\s+(.*?)\s\s", page_text)[0]
        data['license_plate'], data['car_type'], data['distance'], data['time'] = res[1:]

        res = re.findall("(\d\d:\d\d[ap]m)\s\s(.*?)\s\s", page_text)
        (data['start_time'], data['start_location']), (data['end_time'], data['end_location']) = res

        return data

    def parse_uber_emails(self, emails):
        for email in emails:
            data = {}
            data['id'] = email['id']
            try:
                email_body = email['payload']['parts'][0]['body']['data']
            except KeyError:
                logging.warn("No data found in email: %s", email['id'])
                yield data


            email_html = base64.urlsafe_b64decode(email_body).decode('ascii', 'ignore')

            data['date'] = datetime.datetime.fromtimestamp(
                int(email['internalDate'])/1e3)

            soup = BeautifulSoup(email_html, 'html.parser')
            try:
                data.update(self.parser_uber_template_old(soup))
            except Exception:
                data.update(self.parser_uber_template_new(soup))
            
            yield data
            print("Parsing")

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

        with open("uber.csv", 'a') as csv_file:
            writer = csv.DictWriter(csv_file)
            for data in uber_trip_data:
                writer.writerow(data)

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

    print("Finished")
