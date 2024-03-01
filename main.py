# Consolidation and Cleaning of Contact Data from Multiple Excel Sheets
#
# This program takes a set of Excel files in the current directory, each containing contact information,
# and performs data consolidation, cleanup, and transformation to generate a unified output Excel file.
# It addresses variations in column names, handles email headers, combines similar columns,
# formats phone numbers, and maps state names to their abbreviations. The program also tracks events
# attended by each contact and creates a consolidated output file containing cleaned and merged data.
#
# The input Excel files should have data organized in rows, with potential variations in column headers.
# The program assumes that the input Excel files are located in the same directory as this script.
#
# Dependencies: pandas, glob, re, os
# Output: A cleaned and consolidated Excel file named "output.xlsx" with contact information.
#
# Author: Lauren Mora, Omar Hakawati, David-Bryne Adedeji
# Date: 08/28/2023

import pandas as pd
import glob
import re
import os

# Get a list of input files in the current directory with the .xlsx extension
input_files = glob.glob("*.xlsx")

# Define the output file name
output_file = "output.xlsx"

# List of headers that might contain email addresses
email_headers = ["Email", "email", "Company Email"]

# Dictionary to map similar column names to a common name
similar_columns = {
    "Job Title": ["Role", "Job Title", "Title"],
    "Company": ["Company", "Company Name", "Company name"],
    "Email": ["Email", "Company Email"],
    "State/Region": ["State/Region", "State"],
    "Phone Number": ["Phone", "Phone Number"],
    "Attended": ["Attended", "Attendance"],
}

# Dictionary to map state names to their abbreviations
state_abbreviations = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA",
    "colorado": "CO", "connecticut": "CT", "delaware": "DE", "florida": "FL", "georgia": "GA",
    "hawaii": "HI", "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD", "massachusetts": "MA",
    "michigan": "MI", "minnesota": "MN", "mississippi": "MS", "missouri": "MO", "montana": "MT",
    "nebraska": "NE", "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM",
    "new york": "NY", "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT",
    "virginia": "VA", "washington": "WA", "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY"
}


# Function to split a city and state/region from an entry
def split_city_state(entry):
    # If there's a comma in the entry, split it into city and state/region
    if ',' in entry:
        c, s = map(str.strip, entry.split(',', 1))
        return c, s
    # If no comma, return the entry as city and None for state/region
    return entry, None


# Function to capitalize the first letter of each word in a string
def capitalize_first_letters(value):
    if isinstance(value, str):
        return ' '.join(word.capitalize() for word in value.split())
    return value


# Function to normalize a header by stripping whitespace and converting to lowercase
def normalize_header(hdr):
    return hdr.strip().lower()


# Function to split last name from full name
def split_last_name(r, first_name_col, last_name_col):
    similar_first_name_col = get_similar_column_name(r.index, first_name_col)

    # If last name is present but first name is missing, split the last name
    if similar_first_name_col:
        if pd.isnull(r[similar_first_name_col]):
            ln = r[last_name_col]
            if pd.notnull(ln):
                parts = ln.split(' ', 1)
                if len(parts) > 1:
                    r[similar_first_name_col] = parts[0]
                    r[last_name_col] = parts[1]
    # Return the modified row
    return r


# Function to find a header that matches a target header in a list of columns
def find_matching_header(columns, target_header):
    normalized_target = normalize_header(target_header)

    # Iterate through columns and compare with the normalized target header
    for cl in columns:
        if normalized_target == normalize_header(cl):
            return cl
    return None


# Function to find a column with a name similar to the target column
def get_similar_column_name(columns, target_column):
    normalized_target = normalize_header(target_column)

    # Iterate through columns and look for a normalized match
    for c in columns:
        if normalized_target in normalize_header(c):
            return c
    return None


# Function to format phone numbers
def format_phone_number(phone):
    # Ensure phone is a string
    phone_str = str(phone)

    # Remove non-numeric characters from the phone number
    digits = re.sub(r"\D", "", phone_str)

    # If country code present, extract it, else assume it's local
    if len(digits) > 10:
        country_code = "+" + digits[:-10]  # Extract country code
        formatted_number = "({}) {}-{}".format(digits[-10:-7], digits[-7:-4], digits[-4:])
        return country_code + " " + formatted_number
    else:
        return "({}) {}-{}".format(digits[:3], digits[3:6], digits[6:])


# Main function body
def main():
    # Dictionary to store email addresses and associated data from different sheets
    email_data_map = {}

    # Iterate through each input file
    for file in input_files:
        # Read an Excel file into a DataFrame
        df = pd.read_excel(file)

        # Get the current sheet name without extension
        sheet_name = os.path.splitext(file)[0]

        # Iterate through possible email headers
        for email_header in email_headers:
            # Find a column with a matching email header
            email_col_name = find_matching_header(df.columns, email_header)

            if email_col_name:
                # Get the column with email addresses
                email_col = df[email_col_name]

                # Iterate through rows and store data associated with email addresses
                for idx, email in enumerate(email_col):
                    if pd.notnull(email):
                        if email not in email_data_map:
                            email_data_map[email] = []

                        # Store row data and sheet name associated with the email
                        email_data_map[email].append((df.iloc[idx], sheet_name))

    # List to store the final output data
    output_data = []

    # Iterate through email addresses and their associated data
    for email, rows_and_sheets in email_data_map.items():
        # Dictionary to store merged data for each email
        merged_row = {}

        # Set to store unique event names attended by the contact
        events_attended = set()

        # Iterate through rows and sheets associated with the email
        for row, sheet_name in rows_and_sheets:
            row = split_last_name(row, "First Name", "Last Name")

            # Add sheet name to the set
            events_attended.add(sheet_name)

            for col in row.index:
                if col != "#" and pd.notnull(row[col]):
                    if col in similar_columns:
                        for similar_col in similar_columns[col]:
                            if similar_col in row.index:
                                if similar_col == "Phone Number":
                                    merged_row[similar_col] = format_phone_number(row[similar_col])
                                elif similar_col == "State/Region" or similar_col == "State":
                                    state_region_value = row[similar_col].lower().strip()
                                    if state_region_value in state_abbreviations:
                                        merged_row[similar_col] = state_abbreviations[state_region_value]
                                    else:
                                        city, state = split_city_state(state_region_value)
                                        if state:
                                            merged_row["City"] = capitalize_first_letters(city)  # Add City column
                                            merged_row[similar_col] = state.strip().upper()
                                        else:
                                            merged_row[similar_col] = state_region_value.strip().upper()
                                else:
                                    merged_row[similar_col] = capitalize_first_letters(row[similar_col])
                    else:
                        if col == "Phone Number":
                            merged_row[col] = format_phone_number(row[col])
                        elif col == "State/Region" or col == "State":
                            state_region_value = row[col].lower().strip()
                            if state_region_value in state_abbreviations:
                                merged_row[col] = state_abbreviations[state_region_value]
                            else:
                                city, state = split_city_state(row[col])
                                if state:
                                    merged_row["City"] = capitalize_first_letters(city)  # Add City column
                                    merged_row[col] = state.strip().upper()
                                else:
                                    merged_row[col] = state_region_value.strip().upper()  # Convert to uppercase
                        else:
                            if col == "Email":  # Handle Email column separately
                                merged_row[col] = row[col]
                            else:
                                merged_row[col] = capitalize_first_letters(row[col])

        # Convert the set of event names to a comma-separated string
        events_attended_str = ', '.join(events_attended)

        # Add the events attended data to the merged_row dictionary
        merged_row["Events Attended"] = events_attended_str

        # Append the merged_row dictionary to the output_data list
        output_data.append(merged_row)

    # Create a DataFrame from the output_data list
    output_df = pd.DataFrame(output_data)

    # Convert "State/Region" column and specified columns to uppercase
    state_region_headers = ["State/Region", "State"]
    for header in state_region_headers:
        if header in output_df.columns:
            output_df[header] = output_df[header].str.upper()

    # Dictionary to store combined columns
    columns_dict = {}

    # Iterate through columns in the output DataFrame
    for col in output_df.columns:
        # Exclude the '#' (number) column
        exclude_column = col == "#"
        if not exclude_column:
            similar_col_name = None

            # Check if the column name is in the similar_columns dictionary
            for key, similar_names in similar_columns.items():
                if col in similar_names:
                    similar_col_name = key
                    break

            if similar_col_name:
                # If the column name has a similar key, combine data
                matched_col = get_similar_column_name(columns_dict.keys(), similar_col_name)
                if matched_col:
                    columns_dict[matched_col] = columns_dict[matched_col].combine_first(output_df[col])
                else:
                    columns_dict[similar_col_name] = output_df[col]
            else:
                normalized_col = normalize_header(col)
                # Combine data for columns with similar normalized names
                matched_col = get_similar_column_name(columns_dict.keys(), normalized_col)
                if matched_col:
                    columns_dict[matched_col] = columns_dict[matched_col].combine_first(output_df[col])
                else:
                    columns_dict[col] = output_df[col]

    # Create a DataFrame with the combined columns
    output_df = pd.DataFrame(columns_dict)

    # Dictionary to store combined rows
    combined_rows = {}

    # Iterate through rows in the output DataFrame
    for index, row in output_df.iterrows():
        first_name = row["First Name"]
        last_name = row["Last Name"]

        # Check if the first and last name combination exists in the dictionary
        if (first_name, last_name) in combined_rows:
            combined_row = combined_rows[(first_name, last_name)]

            # Update columns from the current row to the combined row
            for col in row.index:
                if col not in ["First Name", "Last Name"]:
                    # If the column already has data, append the new data
                    if pd.notnull(row[col]) and pd.notnull(combined_row[col]):
                        combined_row[col] = f"{combined_row[col]}, {row[col]}"
                    # If the column in the combined row is empty, update with new data
                    elif pd.isnull(combined_row[col]):
                        combined_row[col] = row[col]
        else:
            combined_rows[(first_name, last_name)] = row.copy()

    # Create a list to store the final combined rows
    combined_output = list(combined_rows.values())

    # Create a new DataFrame with the combined rows
    combined_output_df = pd.DataFrame(combined_output)

    # Save the modified DataFrame to the output file
    combined_output_df.to_excel(output_file, index=False)
