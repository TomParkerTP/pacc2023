import httpx
import csv
from prefect import flow, task
from datetime import datetime, timedelta

WEATHER_MEASURES = "temperature_2m,rain,relativehumidity_2m,cloudcover"

@task
def fetch_hourly_weather(lat: float, lon: float):
    """Query the open-meteo API for the forecast"""
    base_url = "https://api.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly=WEATHER_MEASURES,),
    )
    return weather.json()["hourly"]

@task
def save_weather(weather: dict):
    """Save the weather data to a csv file"""
    # csv headers
    contents = f"time,{WEATHER_MEASURES}\n"
    # csv contents
    try:
        for i in range(len(weather["time"])):
            contents += weather["time"][i]
            for measure in WEATHER_MEASURES.split(","):
                contents += "," + str(weather[measure][i])

            contents += "\n"
        with open("C:\\Users\\s24822\\Code\\Prefect\\weather.csv", "w+") as w:
            w.write(contents)
        return "Successfully wrote csv"
    except Exception as e:
        return f"Failed to write csv: {e}"
    
@task
def log_result(weather: str, lat: float, lon: float):
    """Create a markdown file with the results of the next hour forecast"""
    log = f"# Weather in {lat}, {lon}\n\nThe forecast for the next hour as of {datetime.now()} is...\n\n"
    # Find the next hour
    try:
        next_hour = weather["time"].index(
            (datetime.now() + timedelta(hours=1)).strftime("%Y-%m-%dT%H:00")
        )
    except ValueError:
        # Default to the first hour
        next_hour = 0
    # Log the results
    for measure in WEATHER_MEASURES.split(","):
        log += f"- {measure}: {weather[measure][next_hour]}\n"

    # Save the data
    with open("most_recent_results.md", "w") as f:
        f.write(log)

@flow(retries=0)
def fetch_weather_metrics(lat: float, lon: float):
    """Main pipeline"""
    weather = fetch_hourly_weather(lat, lon)
    result = save_weather(weather)
    log_result(weather, lat, lon)
    return result
    
if __name__ == "__main__":
    fetch_weather_metrics(51.507351,-0.127758)