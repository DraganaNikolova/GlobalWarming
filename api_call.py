import system
import string
import json

def getData(city, day):
	
	'''This function gets a JSON object from a weather forecast API and returns a dictionary for getting
	average, minimum, maximum temperature for particular day and city. Day needs to be in format yyyy-mm-dd
	It is beeing called from a Ignition Timer Gateway Event Script every 24 hours around midnight'''

	
	namedQuery = "GetKet"	
	parameters={}
	key = system.db.runNamedQuery(project="GlobalWarming", path=namedQuery, parameters=parameters).getValueAt(0,0)
		
	url = "http://api.weatherapi.com/v1/history.json?key=" + key + "&q=" + city + "&dt=" + day
	
	try:
		data = system.net.httpGet(url=url)
		jsonData = system.util.jsonDecode(data)
		return get_temperature(city, day, jsonData)		
	except:
		system.util.getLogger("Gateway Timer Script").info("City Passed " + city)
		pass
	
	
def get_temperature(city, day, jsonData):
	
	''' This function goes through the dictionary and gets the values from the right keys: avgtemp_c, mintemp_c, mintemp_c
	It inserts the values in the database.'''

	temperature = 0
	minimum = 0
	maximum = 0
	for key, value in jsonData.items():
		if type(value) is dict and key == 'forecast':
			for key2, value2 in value.items():		
				if key2 == 'forecastday':
					for key3, value3 in value2[0].items():
						if type(value3) is dict and key3 == 'day':
							for key4, value4 in value3.items():
								if key4 == 'avgtemp_c':
									temperature = value4
								elif key4 == 'mintemp_c':
									minimum = value4
								elif key4 == 'mintemp_c':
									maximum = value4

	# Insert temperature into database
	namedQuery = "InsertTemperature"
	parameters={"Day":day, "City":city, "Temperature":temperature, 'Minimum':minimum, "Maximum":maximum}
	system.db.runNamedQuery(project="GlobalWarming", path=namedQuery, parameters=parameters)
	
	system.util.getLogger("Gateway Timer Script").info("Temperature, Minimum, Maximum " + str(temperature) + ', ' + str(minimum) + ', ' +str(maximum) )



   

	