import requests

api_key = 'ef8a3722135be9a302f5ea61c8a732ae'

city = 'Dublin'
country_code = 'ie'

# create the API request URL
url = f'https://api.openweathermap.org/data/2.5/weather?q={city},{country_code}&appid={api_key}'

# send the API request and get the response
response = requests.get(url)

# extract the weather information from the response
weather_data = response.json()
print(weather_data)