# Importing all required libraries to run this code
import requests
import json
import datetime
import time
import yaml

from datetime import datetime
from configparser import ConfigParser

# Printing initial message
print('Asteroid processing service')

# Initiating and reading config values
print('Loading configuration from file')

# Opens and reads data from external config file
# Test for errors with try
try:
	config = ConfigParser()
	config.read('config.ini')

	# Reads NASA API key and URL
	nasa_api_key = config.get('nasa', 'api_key')
	nasa_api_url = config.get('nasa', 'api_url')


# Handle the errors with except
except:
	logger.exception('')
print('DONE')

# Getting todays date
dt = datetime.now()
request_date = str(dt.year) + "-" + str(dt.month).zfill(2) + "-" + str(dt.day).zfill(2)  
print("Generated today's date: " + str(request_date))

# Requesting info from NASA API
print("Request url: " + str(nasa_api_url + "rest/v1/feed?start_date=" + request_date + "&end_date=" + request_date + "&api_key=" + nasa_api_key))
r = requests.get(nasa_api_url + "rest/v1/feed?start_date=" + request_date + "&end_date=" + request_date + "&api_key=" + nasa_api_key)

# Printing NASA request response data
print("Response status code: " + str(r.status_code))
print("Response headers: " + str(r.headers))
print("Response content: " + str(r.text))

# If statement based on http status code (200 is successful)
if r.status_code == 200:

	# Parsing data from json format into two arrays below
	json_data = json.loads(r.text)

	# Creating arrays for safe and hazardous asteroids
	ast_safe = []
	ast_hazardous = []

	# Testing whether such json element exists in the given data
	if 'element_count' in json_data:
		ast_count = int(json_data['element_count'])
		print("Asteroid count today: " + str(ast_count))

		# Testing if there are any asteroids
		if ast_count > 0:

			# If there are any asteroids, then read the data from the data provided by NASA (URL, name, approach time, size)
			for val in json_data['near_earth_objects'][request_date]:
				# If statement to get asteroid characteristics
				if 'name' and 'nasa_jpl_url' and 'estimated_diameter' and 'is_potentially_hazardous_asteroid' and 'close_approach_data' in val:
					# Get asteroid name
					tmp_ast_name = val['name']
					# Get URL from description
					tmp_ast_nasa_jpl_url = val['nasa_jpl_url']
					# Get asteroid diameter if the required information is available (min and max asteroid diameter and km)
					if 'kilometers' in val['estimated_diameter']:
						# Testing and defining min and max asteroid diameter if it's available
						if 'estimated_diameter_min' and 'estimated_diameter_max' in val['estimated_diameter']['kilometers']:
							tmp_ast_diam_min = round(val['estimated_diameter']['kilometers']['estimated_diameter_min'], 3)
							tmp_ast_diam_max = round(val['estimated_diameter']['kilometers']['estimated_diameter_max'], 3)
						# Set a default negative value of min and max asteroid diameter if it is not available
						else:
							tmp_ast_diam_min = -2
							tmp_ast_diam_max = -2
					# Set a default negative value of the asteroid diameter if it is not available
					else:
						tmp_ast_diam_min = -1
						tmp_ast_diam_max = -1

					# Setting a new text variable
					tmp_ast_hazardous = val['is_potentially_hazardous_asteroid']

					# If statement if it's a close approach 
					if len(val['close_approach_data']) > 0:
						# If these three values exist in data, then setting values for them
						if 'epoch_date_close_approach' and 'relative_velocity' and 'miss_distance' in val['close_approach_data'][0]:
							tmp_ast_close_appr_ts = int(val['close_approach_data'][0]['epoch_date_close_approach']/1000)
							tmp_ast_close_appr_dt_utc = datetime.utcfromtimestamp(tmp_ast_close_appr_ts).strftime('%Y-%m-%d %H:%M:%S')
							tmp_ast_close_appr_dt = datetime.fromtimestamp(tmp_ast_close_appr_ts).strftime('%Y-%m-%d %H:%M:%S')

							# Checks if speed is available, otherwise sets it to negative value
							if 'kilometers_per_hour' in val['close_approach_data'][0]['relative_velocity']:
								tmp_ast_speed = int(float(val['close_approach_data'][0]['relative_velocity']['kilometers_per_hour']))
							else:
								tmp_ast_speed = -1

							# Tests if km data is available
							if 'kilometers' in val['close_approach_data'][0]['miss_distance']:
								tmp_ast_miss_dist = round(float(val['close_approach_data'][0]['miss_distance']['kilometers']), 3)
							else:
								tmp_ast_miss_dist = -1
						# If those three values mentioned above do not exist in data, then set default values shown below
						else:
							tmp_ast_close_appr_ts = -1
							tmp_ast_close_appr_dt_utc = "1969-12-31 23:59:59"
							tmp_ast_close_appr_dt = "1969-12-31 23:59:59"
					
					# Else statement if it's not a close approach
					else:
						print("No close approach data in message")
						tmp_ast_close_appr_ts = 0
						tmp_ast_close_appr_dt_utc = "1970-01-01 00:00:00"
						tmp_ast_close_appr_dt = "1970-01-01 00:00:00"
						tmp_ast_speed = -1
						tmp_ast_miss_dist = -1

					# Printing information on asteroid characteristics
					print("------------------------------------------------------- >>")
					print("Asteroid name: " + str(tmp_ast_name) + " | INFO: " + str(tmp_ast_nasa_jpl_url) + " | Diameter: " + str(tmp_ast_diam_min) + " - " + str(tmp_ast_diam_max) + " km | Hazardous: " + str(tmp_ast_hazardous))
					print("Close approach TS: " + str(tmp_ast_close_appr_ts) + " | Date/time UTC TZ: " + str(tmp_ast_close_appr_dt_utc) + " | Local TZ: " + str(tmp_ast_close_appr_dt))
					print("Speed: " + str(tmp_ast_speed) + " km/h" + " | MISS distance: " + str(tmp_ast_miss_dist) + " km")
					
					# Adding asteroid data to the corresponding array
					if tmp_ast_hazardous == True:
						# Adding asteroid to hazardous if it is not safe
						ast_hazardous.append([tmp_ast_name, tmp_ast_nasa_jpl_url, tmp_ast_diam_min, tmp_ast_diam_max, tmp_ast_close_appr_ts, tmp_ast_close_appr_dt_utc, tmp_ast_close_appr_dt, tmp_ast_speed, tmp_ast_miss_dist])
					else:
						# Adding asteroid to safe if it is not hazardous
						ast_safe.append([tmp_ast_name, tmp_ast_nasa_jpl_url, tmp_ast_diam_min, tmp_ast_diam_max, tmp_ast_close_appr_ts, tmp_ast_close_appr_dt_utc, tmp_ast_close_appr_dt, tmp_ast_speed, tmp_ast_miss_dist])

		# If there are no asteroids during this day, then printing the following message
		else:
			print("No asteroids are going to hit earth today")

	# Printing a message on the number of safe and hazardous asteroids
	print("Hazardous asteorids: " + str(len(ast_hazardous)) + " | Safe asteroids: " + str(len(ast_safe)))

	# If statement to perform specific actions if there are and there are not asteroids passing today
	if len(ast_hazardous) > 0:

		# Sorting asteroids by time from starting from the most recent
		ast_hazardous.sort(key = lambda x: x[4], reverse=False)

		# Returning a list of asteroids to fly near earth today
		print("Today's possible apocalypse (asteroid impact on earth) times:")
		for asteroid in ast_hazardous:
			print(str(asteroid[6]) + " " + str(asteroid[0]) + " " + " | more info: " + str(asteroid[1]))

		# Sorting asteroids by distance from earth
		ast_hazardous.sort(key = lambda x: x[8], reverse=False)
		# Returning information about the closest asteroid to fly near earth
		print("Closest passing distance is for: " + str(ast_hazardous[0][0]) + " at: " + str(int(ast_hazardous[0][8])) + " km | more info: " + str(ast_hazardous[0][1]))

	# Printing a following message, if there are not asteroids today
	else:
		print("No asteroids close passing earth today")

# Printing an error message due to not response from API with a response code and content
else:
	print("Unable to get response from API. Response code: " + str(r.status_code) + " | content: " + str(r.text))
