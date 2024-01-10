import pandas as pd
from faker import Faker
import os
import random
import configparser
import sys
import logging
from datetime import datetime
import io
import re
import csv

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

def singleint():
    """Generate a single integer."""
    return "{}".format(random.randint(3, 9))   

def calculate_age(birth_date):
    today = datetime.today()
    return today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))

def generate_fake_age(fake, min_age=20, max_age=90):
    dob = fake.date_of_birth(minimum_age=min_age, maximum_age=max_age)
    return calculate_age(dob)

def read_config_file(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)
    return config

def mask_postcode(postcode):
    """Mask the postcode, retaining characters before the first numeric and masking the rest."""
    masked_postcode = []
    numeric_found = False
    for char in postcode:
        if char.isdigit() and not numeric_found:
            numeric_found = True
            masked_postcode.append('X')
        elif numeric_found:
            masked_postcode.append('X')
        else:
            masked_postcode.append(char)
    return ''.join(masked_postcode)

import re

def replace_first_name(name):
    """Replace first name based on specific patterns."""
    if 'whereare' in name:
        return 'KING'
    elif 'sastry' in name:
        return 'MILLER'
    elif 'jusf' in name:
        return 'SCOTT'
    elif re.search(r'jaju.*rriv', name):
        return 'CHLOE'
    else:
        return name  # Return the original name if no pattern matches

# Testing the function with a sample name
print(replace_first_name("ratjajusarrivehowto"))  # Expected output: "CHLOE"


def replace_columns_with_fake_data(dataframe, column_mappings):
    fake = Faker('en_GB')
    fake_data_functions = {
        "first_name": replace_first_name,  # Custom function for first_name
        #"last_name": fake.last_name,
        "name": fake.name,
        "age": lambda: generate_fake_age(fake),
        "random_int": fake.random_int,
        "city": fake.city,
        "state": lambda: random.choice(["ENG", "SCO", "WAL", "NIR"]),
        "address_line1": fake.street_address,
        "address_line2": fake.secondary_address,
        "postcode": mask_postcode,  # Custom function for postcode
        "comments": fake.sentence,
        "phone_number": fake.phone_number,
        "email": fake.email,
        "company": fake.company,
        "job": fake.job,
        "date_of_birth": fake.date_of_birth,
        "organization": fake.company,
        "sort_code": generate_sort_code,
        "singlechar": singleint,
        "alphanumeric": lambda: fake.bothify(text='??????'),
        "text": fake.word
    }

    for column, fake_data_type in column_mappings.items():
        column_index = int(column) - 1
        if fake_data_type in fake_data_functions:
            dataframe.iloc[:, column_index] = dataframe.iloc[:, column_index].apply(
                lambda x: fake_data_functions[fake_data_type](x) if pd.notnull(x) and x.strip() != '' else None         
            )
        else:
            logger.warning(f"Invalid fake data type for column {column}!")

    return dataframe

def process_config_entries(config_entries, input_dir, mask_folder):
    path = os.getcwd()
    for entry in config_entries:
        src_file = entry['src_file']
        column_mappings = entry['columns']
        src_folder = entry['src_folder']
        output_dir = os.path.join(path, mask_folder, src_folder)
        os.makedirs(output_dir, exist_ok=True)
        input_file_path = os.path.join(input_dir, src_folder, src_file)

        if not os.path.isfile(input_file_path):
            logger.warning(f"The file '{input_file_path}' does not exist. Skipping this file.")
            continue

        output_file = os.path.join(output_dir, src_file.replace('.csv', '_mask.csv'))
        with open(input_file_path, 'r', newline='') as file:
            reader = csv.reader(file, delimiter='|', quoting=csv.QUOTE_NONE, escapechar='\\')
            lines = list(reader)

        header = '|'.join(lines[0]) + '\n'
        footer = '|'.join(lines[-1]) + '\n'
        body_lines = lines[1:-1]
        body = pd.DataFrame(body_lines, dtype=str)
        column_mappings = dict(column.split('=') for column in column_mappings.split(','))
        body = replace_columns_with_fake_data(body, column_mappings)
        body = body.applymap(lambda x: x.replace("|", ",").replace("\n", " ") if isinstance(x, str) else x)
        csv_body = body.to_csv(index=False, sep='|', header=False, quoting=csv.QUOTE_NONE, quotechar="", escapechar="\\")

        with open(output_file, 'w', newline='\n') as file:
            file.write(header)
            file.write(csv_body)
            file.write(footer)

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

timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
log_dir = 'log'
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f'logfile_{timestamp}.log')
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.INFO)
logger.addHandler(file_handler)

process_config_entries(config_entries, input_dir, mask_folder)
