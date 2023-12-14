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

# Function to replace column values with fake data
def replace_columns_with_fake_data(dataframe, column_mappings, quoted_columns):
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

    for column, fake_data_type in column_mappings.items():
        if fake_data_type in fake_data_functions:
            column_values = dataframe[column]
            if column_values.dtype == object and column in quoted_columns:
                dataframe[column] = column_values.apply(
                    lambda x: f'"{fake_data_functions[fake_data_type]()}"'
                )
            else:
                dataframe[column] = column_values.apply(
                    lambda x: fake_data_functions[fake_data_type]()
                )
        else:
            logger.warning(f"Invalid fake data type for column {column}!")

    return dataframe

# Read the configuration from a file
def read_config_file(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)
    return config

# Process the configuration entries
def process_config_entries(config_entries, input_dir, mask_folder):
    path = os.getcwd()
    for entry in config_entries:
        src_file = entry['src_file']
        column_mappings = entry['columns']
        quoted_columns = entry.get('quoted_columns', '').split(',')
        src_folder = entry['src_folder']

        output_dir = os.path.join(path, mask_folder, src_folder)
        os.makedirs(output_dir, exist_ok=True)

        input_file_path = os.path.join(input_dir, src_folder, src_file)

        # Error handling if the input file doesn't exist
        if not os.path.isfile(input_file_path):
            logger.warning(f"The file '{input_file_path}' does not exist. Skipping this file.")
            continue

        output_file = os.path.join(output_dir, src_file.replace('.csv', '_mask.csv'))

        # Error handling if the source folder doesn't exist
        src_folder_path = os.path.join(input_dir, src_folder)
        if not os.path.isdir(src_folder_path):
            logger.error(f"The source folder '{src_folder_path}' does not exist. Exiting.")
            sys.exit(1)

        # Process the file in chunks to handle large files
        chunk_size = 10000  # Adjust the chunk size as needed
        chunks = pd.read_csv(input_file_path, sep='|', dtype=str, low_memory=False, encoding='latin1', chunksize=chunk_size)

        for chunk in chunks:
            chunk = replace_columns_with_fake_data(chunk, column_mappings, quoted_columns)
            chunk.to_csv(output_file, mode='a', index=False, sep='|', quoting=1, quotechar='"', escapechar='\\')

        logger.info(f"Masked file saved to {output_file}")

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
