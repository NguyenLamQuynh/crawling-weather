import requests
from datetime import datetime, timedelta
import concurrent.futures
import pandas as pd

def fetch_data(YOUR_API_KEY, city):
    # Get the date for 2 days ago
    twodaysago = datetime.now() - timedelta(days=2)
    start_date = twodaysago.strftime('%Y-%m-%dT00:00:00')
    end_date = twodaysago.strftime('%Y-%m-%dT23:59:59')

    # Base URL for the Visual Crossing API
    base_url = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"

    # Construct the URL
    url = f"{base_url}/{city}/{start_date}/{end_date}"

    # Define the parameters
    params = {
        'unitGroup': 'metric',  # Use 'metric' for Celsius
        'key': YOUR_API_KEY,         # Your API key here
        'include': 'hours',     # Include hourly data
        'contentType': 'json'   # Get data in JSON format
    }

    # Make the API request
    response = requests.get(url, params=params)
    print(url)
    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f"Error: Unable to fetch data. Status code: {response.status_code}")
        return None

def extract_weather_data_location(city):
    # Replace with your Visual Crossing API key

    # Fetch weather data
    weather_data = fetch_data(api_key, city)
    
    records=[]
    # Display the data
    if weather_data:
        for day in weather_data['days']:

            date_obj = datetime.strptime(day['datetime'], "%Y-%m-%d")
            date_format=date_obj.strftime("%d/%m/%Y")

            for hour in day['hours']:
                records.append({
                "datetime": f"{date_format} {hour['datetime']}",
                "Temperature(°C)": hour['temp'],
                "Condition": hour['conditions'],
                "Humidity": hour['humidity'],
                "Wind Speed(km/h)": hour['windspeed'],
                "precip": hour['precip'],
                "icon": hour['icon'],
                "location": weather_data["address"]
                })
    return records


def extract_all_district(list_district):
    records=[]

    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_district= {executor.submit(extract_weather_data_location, district): district for district in list_district}
        for future in future_to_district:
            district= future_to_district[future]
            try:
                records.extend(future.result())
            except Exception as exc:
                print(f"{district} generated an exception: {exc}")

    # for district in list_district:
    #     records.extend(extract_weather_data_location(district))
    return records



if __name__ == "__main__":
    api_key = "PGFTWKEV5MRZUKWTFH93UPBXT"

    with open("location.txt",'r', encoding='utf-8') as f:
        list_district = [line.strip() for line in f.readlines()]

    data = extract_all_district(list_district)
    df  = pd.DataFrame(data)
    df.to_csv("weather_two_days_ago.csv", index=False)
