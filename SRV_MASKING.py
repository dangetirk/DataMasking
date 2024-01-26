import pandas as pd
from faker import Faker
import os
import random
import configparser
import sys
import io
import csv
import logging
from datetime import datetime
import re

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

# Initialize counters and summary lists
source_file_count = 0
masked_file_count = 0
total_columns_masked = 0
total_records_masked = 0
summary_data = []
masked_file_paths = []

def generate_sort_code():
    """Generate a random bank sort code."""
    return "{}{}{}".format(random.randint(10, 99), random.randint(10, 99), random.randint(10, 99))

def singleint():
    """Generate a random bank sort code."""
    return "{}".format(random.randint(3, 9))

def calculate_age(birth_date):
    today = datetime.today()
    return today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))

def generate_fake_age(fake, min_age=20, max_age=90):
    dob = fake.date_of_birth(minimum_age=min_age, maximum_age=max_age)
    return calculate_age(dob)

def mask_postcode(postcode):
    if ' ' in postcode:
        parts = postcode.split(' ')
        first_part = parts[0]
        second_part = 'X' * len(parts[1])
        return f"{first_part} {second_part}"
    else:
        return f"{postcode[:2]}{'Z' * (len(postcode) - 2)}"

def replace_first_name(fake, name):
    if re.search(r"(((?=.*omn)(?=.*gtee)(?=.*set )(?=.*off ))|(?=.*o.g.s.a )|(?=ogsa)|((?=.*omnibus)(?=.*letter)(?=.*set ))|((?=.*omnibus)(?=.*lso))|((?=.*joint guarantee)(?=.*lso))|((?=.*g'tee &)(?=.*lso))|((?=.*og incorporates lso))|((?=.*also )(?=.*set_off)))", name, re.I):
        return 'OGSA'  
    elif re.search(r"((?=\bog\b )|((?=.*omn)(?=.*gtee))|((?=.*omni g'tee))|(?=.*note:og ))", name, re.I):
        return 'OMNIBUS'  
    elif re.search(r"(((?=.*cross)(?=.*guar))|((?=.*cross corp))|((?=.*cross)(?=.*g)(?=.*tee))|(?=.*cross ))", name, re.I):
        return 'CROSS'    
    else:
        return fake.word()

def read_config_file(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)
    return config

def replace_columns_with_fake_data(dataframe, column_mappings):
    fake = Faker('en_GB')
    fake_data_functions = {
        "first_name": replace_first_name,
        "last_name": fake.last_name,
        "name": fake.name,
        "age": lambda: generate_fake_age(fake),
        "random_int": fake.random_int,
        "city": fake.city,
        "state": lambda: random.choice(["ENG", "SCO", "WAL", "NIR"]),
        "address_line1": fake.street_address,
        "address_line2": fake.secondary_address,
        "postcode": mask_postcode,
        "zipcode": fake.postcode,
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
            if fake_data_type == "postcode":
                dataframe.iloc[:, column_index] = dataframe.iloc[:, column_index].apply(
                    lambda x: fake_data_functions[fake_data_type](x) if pd.notna(x) and str(x).strip() != '' else None
                )
            elif fake_data_type == "first_name":
                dataframe.iloc[:, column_index] = dataframe.iloc[:, column_index].apply(
                    lambda x: fake_data_functions[fake_data_type](fake, x) if pd.notna(x) and str(x).strip() != '' else None
                )
            else:
                dataframe.iloc[:, column_index] = dataframe.iloc[:, column_index].apply(
                    lambda x: fake_data_functions[fake_data_type]() if pd.notna(x) and str(x).strip() != '' else None
                )
        else:
            logger.warning(f"Invalid fake data type for column {column}!")

    return dataframe

def process_config_entries(config_entries, input_dir, mask_folder, output_dir):
    global source_file_count, total_columns_masked, total_records_masked

    for entry in config_entries:
        src_file = entry['src_file']
        column_mappings = entry['columns']
        os.makedirs(output_dir, exist_ok=True)
        input_file_path = os.path.join(input_dir, src_file)
        if not os.path.isfile(input_file_path):
            logger.warning(f"The file '{input_file_path}' does not exist. Skipping this file.")
            continue
        output_file = os.path.join(output_dir, src_file)
        with open(input_file_path, 'r', newline='') as file:
            reader = csv.reader(file, delimiter='|', quoting=csv.QUOTE_NONE, escapechar='\\')
            lines = list(reader)
        header = '|'.join(lines[0]) + '\n'
        footer = '|'.join(lines[-1]) + '\n'
        body_lines = lines[1:-1]
        body = pd.DataFrame(body_lines, dtype=str)
        column_mappings = dict(column.split('=') for column in column_mappings.split(','))
        columns_masked = len(column_mappings)  # Count columns to be masked
        body = replace_columns_with_fake_data(body, column_mappings)
        body = body.applymap(lambda x: x.replace("|", ",").replace("\n", " ") if isinstance(x, str) else x)
        csv_body = body.to_csv(index=False, sep='|', header=False, quoting=csv.QUOTE_NONE, quotechar="", escapechar="\\")
        with open(output_file, 'w', newline='\n') as file:
            file.write(header)
            file.write(csv_body)
            file.write(footer)

        # Record input file record count and columns masked
        input_record_count = len(body_lines)
        source_file_count += 1
        total_columns_masked += columns_masked

        # Count the number of records in the generated masked file
        with open(output_file, 'r', newline='') as file:
            masked_lines = list(csv.reader(file, delimiter='|', quoting=csv.QUOTE_NONE, escapechar='\\'))
        masked_record_count = len(masked_lines) - 2  # Subtract header and footer lines
        total_records_masked += masked_record_count
        masked_file_paths.append(output_file)

        # Create summary data for the source file
        summary_data.append({
            'Source File': src_file,
            'Input Records': input_record_count,
            'Columns Masked': columns_masked,
            'Masked Record Count': masked_record_count,
            'Masked File Path': output_file
        })

if len(sys.argv) < 2:
    print("Please provide a path to the configuration file as an argument.")
    sys.exit(1)

config_file = sys.argv[1]
config = read_config_file(config_file)
base_path = config.get('PATHS', 'base_path')
mask_folder = config.get('PATHS', 'mask_folder')
input_dir = base_path
output_dir = mask_folder  # Use 'mask_folder' as the output directory

config_entries = [
    dict(config[section])
    for section in config.sections() if section != 'PATHS'
]

process_config_entries(config_entries, input_dir, mask_folder, output_dir)

# After processing all entries, print summary
if summary_data:
    print("\nSummary Report:")
    summary_df = pd.DataFrame(summary_data)
    summary_df.index += 1  # Start the index from 1
    print(summary_df.to_string(index=False))
