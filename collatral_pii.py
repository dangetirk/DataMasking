import pandas as pd
from faker import Faker
import os
import random
import configparser
import sys
import logging
from datetime import datetime
from datetime import date

# Create a logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create console handler and set level to info
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
logger.addHandler(handler)

def generate_sort_code():
    """Generate a random bank sort code."""
    return "{}{}{}".format(random.randint(10, 99), random.randint(10, 99), random.randint(10, 99))

def calculate_age(birth_date):
    today = date.today()
    return today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))

def generate_fake_age(fake, min_age=20, max_age=90):
    dob = fake.date_of_birth(minimum_age=min_age, maximum_age=max_age)
    return calculate_age(dob)

def generate_fake_data(row):
    fake = Faker('en_GB')
    fake_data_functions = {
        "first_name": fake.first_name,
        "last_name": fake.last_name,
        "name": fake.name,
        "age": lambda: generate_fake_age(fake),
        "random_int": fake.random_int,
        "city": fake.city,
        "state": lambda: random.choice(["ENG", "SCO", "WAL", "NIR"]),
        "address_line1": fake.street_address,
        "address_line2": fake.secondary_address,
        "zipcode": fake.postcode,
        "comments": fake.sentence,
        "phone_number": fake.phone_number,
        "email": fake.email,
        "company": fake.company,
        "job": fake.job,
        "date_of_birth": fake.date_of_birth,
        "organization": fake.company,
        "sort_code": generate_sort_code,
        "text": fake.word
    }

    for column in row.index:
        if column in fake_data_functions:
            row[column] = fake_data_functions[column]()
        else:
            logger.warning(f"Invalid fake data type for column {column}!")

    return row

def process_file(input_file_path, output_file):
    try:
        df = pd.read_csv(input_file_path, sep='|', dtype=str, low_memory=False, encoding='latin1')
        df = df.apply(generate_fake_data, axis=1)
        df.to_csv(output_file, index=False, sep='|', quoting=1, quotechar='"', escapechar='\\')
        logger.info(f"Masked file saved to {output_file}")
    except Exception as e:
        logger.error(f"Error processing file {input_file_path}: {str(e)}")

def process_config_entries(config_entries, input_dir, mask_folder):
    path = os.getcwd()
    for entry in config_entries:
        src_file = entry['src_file']
        src_folder = entry['src_folder']

        input_file_path = os.path.join(input_dir, src_folder, src_file)
        output_dir = os.path.join(path, mask_folder, src_folder)
        os.makedirs(output_dir, exist_ok=True)

        if not os.path.isfile(input_file_path):
            logger.warning(f"The file '{input_file_path}' does not exist. Skipping this file.")
            continue

        output_file = os.path.join(output_dir, src_file.replace('.csv', '_mask.csv'))
        process_file(input_file_path, output_file)

if len(sys.argv) < 2:
    print("Please provide a path to the configuration file as an argument.")
    sys.exit(1)

config_file = sys.argv[1]
config = read_config_file(config_file)
base_path = config.get('PATHS', 'base_path')
input_folder = config.get('PATHS', 'input_folder')
mask_folder = config.get('PATHS', 'mask_folder')
input_dir = os.path.join(base_path, input_folder)

config_entries = [
    dict(config[section])
    for section in config.sections() if section != 'PATHS'
]

# Create log file with timestamp
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
log_dir = 'log'
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f'logfile_{timestamp}.log')

# Create file handler and set level to info
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.INFO)
logger.addHandler(file_handler)

process_config_entries(config_entries, input_dir, mask_folder)
