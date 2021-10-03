import pandas as pd
import matplotlib.pyplot as plt
from netCDF4 import Dataset
from cftime import num2date
import os
import numpy as np
from datetime import datetime, timedelta, date


def plot_temperatures_by_country(values, country, start, end):
    """
    Returns a plot for temperature values for a country
    from a start point to an end point
    """

    filtered = values.loc[(values['Country'] == country) &
                          (values['dt'] >= start) &
                          (values['dt'] <= end)]

    # x axis values
    x1 = filtered['dt']
    # corresponding y axis values
    y1 = filtered['AverageTemperature']

    # plotting the points
    plt.plot(x1, y1, label = "line 1")

    filtered = values.loc[(values['Country'] == country) &
                          (values['dt'] >= '1973-01-01') &
                          (values['dt'] <= '1974-01-01')]

    # x axis values
    x2 = filtered['dt']
    # corresponding y axis values
    y2 = filtered['AverageTemperature']

    # plotting the points
    plt.plot(x2, y2, label="line 2")

    # naming the x axis
    plt.xlabel('x - axis - date')
    # naming the y axis
    plt.ylabel('y - axis - temperature')

    plt.title('Temperatures from ' + start + ' to ' + end + ' for ' + country)

    # function to show the plot
    plt.show()


def temperatures_by_city_till2013():
    """
    Info for dataset, temperatures by city part 1 - from 1743 to 2013
    """

    # Columns: dt,AverageTemperature,AverageTemperatureUncertainty,City,Country,Latitude,Longitude
    temperatures = pd.read_csv("GlobalLandTemperatures/GlobalLandTemperaturesByCity.csv")

    # 8 599 212 rows
    print(len(temperatures))

    countries = temperatures['Country'].unique()
    print(len(countries))
    print(sorted(countries))


def temperatures_by_country_till2013():
    """
    Info for dataset, temperatures by country part 1 - from 1743 to 2013
    """

    # Columns: dt, AverageTemperature, AverageTemperatureUncertainty, Country
    temperatures = pd.read_csv("GlobalLandTemperatures/GlobalLandTemperaturesByCountry.csv")

    # 577 462 rows
    print(len(temperatures))

    countries = temperatures['Country'].unique()
    print(len(countries))
    print(sorted(countries))


def plot_co2_by_country(values, country, start, end):
    """
    Returns a plot for co2 values for a country
    from a start point to an end point
    """

    filtered = values.loc[(values['Country'] == country) &
                          (values['Year'] >= start) &
                          (values['Year'] <= end)]

    # x axis values
    x1 = filtered['Year']
    # corresponding y axis values
    y1 = filtered['CO2']

    # plotting the points
    plt.plot(x1, y1, label = "line 1")

    # naming the x axis
    plt.xlabel('x - axis - year')
    # naming the y axis
    plt.ylabel('y - axis - co2')

    # giving a title to my graph
    plt.title('CO2 from ' + start + ' to ' + end + ' for ' + country)

    # function to show the plot
    plt.show()


def co2_by_country_till2019():
    """
    Info for dataset, co2 by country part 1 - from 1751 to 2017
    """
    co2_messy = pd.read_csv("CO2/emission data.csv")

    co2 = pd.melt(co2_messy, id_vars=["Country"], var_name="Year", value_name="CO2")

    df = pd.DataFrame()
    df['Country'] = co2['Country']
    df['Year'] = co2['Year']
    df['CO2'] = co2['CO2']

    df.to_csv(r'C:\Users\stoja\Desktop\EmissionCO2.csv', index=False)


def get_lat_lon():
    """
    Returns arrays for latitudes, longitudes, cities and countries
    from dataset, temperatures by country part 1, from 1743 to 2013
    """

    # Columns: dt,AverageTemperature,AverageTemperatureUncertainty,City,Country,Latitude,Longitude
    temperatures = pd.read_csv("GlobalLandTemperatures/GlobalLandTemperaturesByCity.csv")

    Latitude = temperatures['Latitude']
    Longitude = temperatures['Longitude']
    City = temperatures['City']
    Country = temperatures['Country']

    lat_array = []
    long_array = []
    cities_array = []
    countries_array = []
    tuples = []
    for i, j, city, country in zip(Latitude, Longitude, City, Country):
        if (i, j) not in tuples:
            tuples.append((i, j))
            lat_array.append(float(i[:-1]))
            long_array.append(float(j[:-1]))
            cities_array.append(city)
            countries_array.append(country)

    return lat_array, long_array, cities_array, countries_array


def make_dataset_temperatures(filename, points):
    """
     From netCDF4 file to CSV file
     """

    ds = Dataset(filename)

    lats, lons, cities, countries = get_lat_lon()

    # total lat,lon pairs: 1366
    print('The number of rows is ' + str(len(lats)*points))
    lon = ds.variables['longitude']
    lat = ds.variables['latitude']
    time = ds.variables['date_number']

    lon_array = lon[:]
    lat_array = lat[:]
    time_array = time[:]

    temperature = ds.variables['temperature']

    dates = []
    for time in time_array[:]:
        year = int(time)
        rem = time - year
        base = datetime(year, 1, 1)
        dates.append((base + timedelta(seconds=(base.replace(year=base.year + 1) - base).total_seconds() * rem)).date())

    # second approach
    # for t in time_array[:]:
    #     dates.append(num2date(t, units=time.units))

    dateResult = []
    temperatureResult = []
    latitudeResult = []
    longitudeResult = []
    cityResult = []
    countryResult = []

    for latitude, longitude, city, country in zip(lats, lons, cities, countries):

        # We want to find data for latitude, longitude
        # We first need to find the indexes
        i = np.abs(lon_array - longitude).argmin()
        j = np.abs(lat_array - latitude).argmin()

        for d in dates:
            dateResult.append(d)

        resultTemperature = temperature[:, j, i]
        for t in resultTemperature:
            temperatureResult.append(t)

        resultLatitues = np.full(
            shape=points,
            fill_value=latitude,
            dtype=np.float
        )
        for l in resultLatitues:
            latitudeResult.append(l)

        resultLongitudes = np.full(
            shape=points,
            fill_value=longitude,
            dtype=np.float
        )
        for l in resultLongitudes:
            longitudeResult.append(l)

        resultCities = np.full(
            shape=points,
            fill_value=city
        )
        for c in resultCities:
            cityResult.append(c)

        resultCountries = np.full(
            shape=points,
            fill_value=country
        )
        for c in resultCountries:
            countryResult.append(c)

        print('iteration no:' + str(i))

    df = pd.DataFrame()
    df['date'] = dateResult
    df['temperature'] = temperatureResult
    df['latitude'] = latitudeResult
    df['longitude'] = longitudeResult
    df['city'] = cityResult
    df['country'] = countryResult

    df.to_csv(r'C:\Users\stoja\Desktop\Temperatures.csv', index=False)
    return df


def model():

    # Info for netCDF4 file
    # 1416
    ds = Dataset('air.mon.mean.v501.nc')
    print(ds)
    time = ds.variables['time']
    print(time.units)
    time_array = time[:]
    for t in time_array[:]:
        print(num2date(t, units=time.units))


if __name__ == '__main__':
    print('Start')

    # Making the CO2 dataset
    co2_by_country_till2019()

    # Making the temperatures dataset
    df1 = make_dataset_temperatures('air.mon.mean.v501.nc', 1416)
    print(df1.head())

    # Making the temperatures anomalies dataset
    df2 = make_dataset_temperatures('Complete_TAVG_Daily_LatLong1_2010.nc', 3652)
    print(df2.head())
