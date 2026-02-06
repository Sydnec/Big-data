import requests
import json

def test_open_meteo_api():
    url = 'https://api.open-meteo.com/v1/forecast'

    # Toulouse
    params = {
        'latitude': 43.6047,
        'longitude': 1.4442,
        'hourly': 'temperature_2m,precipitation,wind_speed_10m,wind_direction_10m,relative_humidity_2m,pressure_msl,soil_temperature_0_to_7cm,soil_moisture_0_to_7cm,cloud_cover',
        'timezone': 'Europe/Paris'
    }

    try:
        print(f"\nSending request to: {url}")
        print(f"Parameters: {json.dumps(params, indent=2)}")

        response = requests.get(url, params=params, timeout=30)

        print(f"\nStatus Code: {response.status_code}")

        if response.status_code == 200:
            data = response.json()

            print("\nOpen Meteo API is accessible")
            print("=" * 60)
            print("\nAPI Response Summary:")
            print(f"  Latitude: {data.get('latitude')}")
            print(f"  Longitude: {data.get('longitude')}")
            print(f"  Timezone: {data.get('timezone')}")
            print(f"  Elevation: {data.get('elevation')} m")

            if 'hourly' in data:
                print(f"\nHourly Data Variables ({len(data['hourly'])} total):")
                for key in data['hourly'].keys():
                    if key == 'time':
                        print(f"  - {key}: {len(data['hourly'][key])} timestamps")
                    else:
                        print(f"  - {key}")

                # Show sample data
                print(f"\nSample Data (first 3 hours):")
                for i in range(min(3, len(data['hourly']['time']))):
                    print(f"\n  Time: {data['hourly']['time'][i]}")
                    print(f"    Temperature: {data['hourly']['temperature_2m'][i]}°C")
                    print(f"    Precipitation: {data['hourly']['precipitation'][i]} mm")
                    print(f"    Wind Speed: {data['hourly']['wind_speed_10m'][i]} km/h")

            return True

        else:
            print(f"\nAPI returned error status: {response.status_code}")
            print(f"Response: {response.text}")
            return False

    except requests.exceptions.Timeout:
        print("\nAPI request timed out")
        return False
    except requests.exceptions.RequestException as e:
        print(f"\nAPI request error: {str(e)}")
        return False
    except Exception as e:
        print(f"\nUnexpected error: {str(e)}")
        return False


if __name__ == '__main__':
    import sys
    success = test_open_meteo_api()
    sys.exit(0 if success else 1)
