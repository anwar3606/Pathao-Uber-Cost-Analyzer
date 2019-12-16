from GmailParser import GmailParser
import re
import csv
import base64
import datetime
import logging
import os
from bs4 import BeautifulSoup

PATHAO_FILE_NAME = "PathaoTrips.csv"

data_columns = [
    "id",
    "date",
    "price",
    "car_type",
    "start_location",
    "end_location",
    "rider",
    "license_plate",
    "subtotal",
    "discounts",
    "surge"
]

class PathaoParser(GmailParser):

    def __init__(self):

        super().__init__()

    def parser_pathao_template_old(self, soup):
        data = {}
        data ['start_location'] = soup.find(id='pick-up-location').text
        data['end_location'] = soup.find(id='pick-down-location').text

        fare_text = soup.select('#email-content>tr')[1].text
        data['price'] = re.findall(r'Total Fare\s*(.*?)\n', fare_text)[0]
        data['subtotal'] = re.findall(r'Subtotal\n(.*?)\n', fare_text)[0]
        
        try:
            data['discounts'] = re.findall(r'Promo\n(.*?)\n', fare_text)[0]
        except Exception:
            pass

        try:
            data['surge'] = re.findall(r'Surge\n(.*?)\n', fare_text)[0]
        except Exception:
            pass

        
        data['rider'] = soup.select('#email-content>tr')[2].findAll('td')[3].contents[1].text
        data['license_plate'] = soup.select('#email-content>tr')[2].findAll('td')[3].contents[4].strip()

        return data

    def parser_pathao_template_new(self, soup):
        data = {}

        data['price'] = soup.select_one('.payment-area').text
        data['start_location'], data['end_location'] = [x.text for x in soup.select('.address')]

        fare_text = soup.select_one('.fare-area').text        
        data['subtotal'] = re.findall(r'Subtotal\s+(.*?)\n', fare_text)[0]
        
        try:
            data['discounts'] = re.findall(r'Promo\s+(.*?)\n', fare_text)[0]
        except Exception:
            pass

        try:
            data['surge'] = re.findall(r'Surge\s+(.*?)\n', fare_text)[0]
        except Exception:
            pass

        try:
            data['car_type'] = soup.select_one('.bike').text
        except Exception:
            pass

        data['rider'] = soup.select_one('.name').text
        data['license_plate'] = soup.select_one('.number').text

        return data

    def parse_pathao_emails(self, emails):
        for email in emails:
            data = {}
            data['id'] = email['id']
            try:
                email_body = email['payload']['body']['data']
            except KeyError:
                logging.warning("No data found in email: %s", email['id'])
                yield data
                continue

            if "Pathao Food" in email['snippet']:
                logging.warning("Ignoring pathao food: %s", email['id'])
                continue


            email_html = base64.urlsafe_b64decode(
                email_body).decode('ascii', 'ignore')

            data['date'] = datetime.datetime.fromtimestamp(
                int(email['internalDate'])/1e3).strftime("%c")

            soup = BeautifulSoup(email_html, 'html.parser')
            try:
                data.update(self.parser_pathao_template_old(soup))
            except Exception:
                try:
                    data.update(self.parser_pathao_template_new(soup))
                except Exception:
                    open("error/"+email['id']+".html", 'w',
                         encoding='utf-8').write(str(soup))

            yield data

    def cleanup_data(self, data):
        new_data = {}
        for key, value in data.items():
            if key in ["price", "subtotal"]:
                new_data[key] = re.findall(r'\d+', value)[0]
            else: new_data[key.strip()] = value.strip()
        
        return new_data

    def get_pathao_emails(self, date=None):
        logging.info("Getting pathao emails...")

        options = {
            "userId": 'me',
            "q": 'from:(no-reply@pathao.com)',
        }
        if date:
            options['q'] += " before:" + date

        emails = list(self.gmail_api.get_email_list(options=options))
        filtered_email_ids = self.find_new_email_ids(emails)
        emails_content = self.gmail_api.get_emails(filtered_email_ids)

        pathao_trip_data = self.parse_pathao_emails(emails_content)

        if os.path.exists(PATHAO_FILE_NAME):
            write_header = False
        else:
            write_header = True

        with open(PATHAO_FILE_NAME, 'a', newline="") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=data_columns)
            if write_header:
                writer.writeheader()
            print("")
            for idx, data in enumerate(pathao_trip_data):
                print("\rParsed:", idx+1, end="")
                if len(data) > 1:
                    writer.writerow(self.cleanup_data(data))

                if data:
                    self.remember_email_id(data['id'])


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

    parser = PathaoParser()
    # emails = parser.get_pathao_emails()
    emails = parser.get_pathao_emails()

    # open("16a9fd2f82d1f2ba.html", 'w').write(base64.urlsafe_b64decode(['payload']['parts'][0]['body']['data']).decode('ascii', 'ignore'))

    # email = parser.gmail_api.get_emails(['163459e93aa2ed0c'])
    # pathao_trip_data = list(parser.parse_pathao_emails(email))
    # parser.cleanup_data(pathao_trip_data[0])
    print("Finished")
