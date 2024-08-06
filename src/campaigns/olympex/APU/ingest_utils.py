import re
import pandas as pd
import numpy as np

def color_encode(data, min_value, max_value, opa=255, RGBA=False):
    # Define the number of colors in the gradient
    num_colors = 100  # Number of colors in the gradient

    # Generate the gradient from blue to red
    colors = np.linspace([0, 0, 1], [1, 0, 0], num_colors, dtype=np.float32)

    # Add opacity if RGBA is True
    if RGBA:
        colors = np.column_stack((colors, np.full(num_colors, opa, dtype=np.uint8)))

    # Convert min_value and max_value to numerical values
    min_value = float(min_value)
    max_value = float(max_value)

    # Normalize the parameter values
    values = data['rain rate'].astype(float).values
    normalized_values = (values - min_value) / (max_value - min_value)

    # Clip normalized values to ensure they are within [0, 1]
    normalized_values = np.clip(normalized_values, 0, 1)

    # Map the parameter values to colors in the gradient
    color_indices = (normalized_values * (num_colors - 1)).astype(int)
    encoded_colors = colors[color_indices]

    return encoded_colors

def clean_line(line):
    line = re.sub(r'\s+',' ', line)
    line = line.strip()
    line = line.split(" ")
    return line

def get_col_index_map():
    # represents the column number for each key, inside the input csv type file.
    return {
        "Year": 0,
        "Day": 1,
        "Hour": 2,
        "Min": 3,
        "drops": 4,
        "conc": 5,
        "water content": 6,
        "rain rate": 7,
        "reflectivity": 8,
        "mass-weight d.d": 9,
        "max d.d": 10,
    }

def generator_to_df(file):
    modified_lines = []
    for line in file:
        # if(type(line) == str):
        # modified_line = clean_line(line)
        # else:
        modified_line = clean_line(line.decode())
        modified_lines.append(modified_line)
    return modified_lines

# Function to read the CSV file and get latitude and longitude by site ID
def get_lat_lon_by_site_id(site_id):
    csv_file_path = 'src/campaigns/olympex/APU/sites.csv'
    df = pd.read_csv(csv_file_path)
    site_row = df[df['Site ID'] == site_id]
    
    # Check if the site ID was found and return the latitude and longitude
    if not site_row.empty:
        latitude = site_row.iloc[0]['Latitude (deg)']
        longitude = site_row.iloc[0]['Longitude (deg)']
        return latitude, longitude
    return None, None