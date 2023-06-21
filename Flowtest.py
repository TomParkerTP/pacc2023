import httpx
import csv
from prefect import flow, task

@task
def get_temperature(lat: float, lon: float):
    base_url = "https://api.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="temperature_2m"),
    )

    most_recent_temp = float(weather.json()["hourly"]["temperature_2m"][0])
    print(f"Most recent temp C: {most_recent_temp} degrees")
    return most_recent_temp

@task
def get_rain(lat: float, lon: float):
    base_url = "https://api.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="rain"),
    )
    rain_status = float(weather.json()["hourly"]["rain"][0])
    print(f"Rain status: {rain_status}")
    return rain_status

@task
def get_visibility(lat: float, lon: float):
    base_url = "https://api.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="visibility"),
    )
    visibility_status = float(weather.json()["hourly"]["visibility"][0])
    print(f"Visibility status: {visibility_status}")
    return visibility_status

@task
def save_weather(temp: float, rain: float, vis: float):
    with open("weather.csv", "w+") as w:
        writer = csv.writer(w)
        writer.writerow([temp, rain, vis])
    return "Successfully wrote temp"

@flow(retries=3)
def fetch_weather_metrics(lat: float, lon: float):
    get_temperature(lat=lat, lon=lon)
    get_rain(lat=lat, lon=lon)
    get_visibility(lat=lat, lon=lon)
    temp = get_temperature(lat, lon)
    rain = get_rain(lat, lon)
    vis = get_visibility(lat, lon)
    result = save_weather(temp, rain, vis)
    return result
    
if __name__ == "__main__":
    fetch_weather_metrics(51.507351,-0.127758)