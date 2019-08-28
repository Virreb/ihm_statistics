import pandas as pd
import datetime

# Load rain data
rain_df = pd.read_csv('data/rain_lysekil.csv', delimiter=';', header=0)
rain_df['date'] = rain_df['Representativt dygn']
rain_df['rain'] = rain_df['Nederbördsmängd']
rain_df = rain_df.loc[:, ['date', 'rain']]
rain_df.set_index('date', inplace=True)

# Load average temp data
average_temp_df = pd.read_csv('data/temp_average_vaderoarna.csv', delimiter=';', header=0)
average_temp_df['date'] = average_temp_df['Representativt dygn']
average_temp_df['temp_average'] = average_temp_df['Lufttemperatur']
average_temp_df = average_temp_df.loc[:, ['date', 'temp_average']]
average_temp_df.set_index('date', inplace=True)

# Load min and max temperatures
min_max_temp_df = pd.read_csv('data/temp_min_max_vaderoarna.csv', delimiter=';', header=0)
min_max_temp_df['date'] = min_max_temp_df['Representativt dygn']
min_max_temp_df['temp_max'] = min_max_temp_df['Lufttemperatur_max']
min_max_temp_df['temp_min'] = min_max_temp_df['Lufttemperatur_min']
min_max_temp_df = min_max_temp_df.loc[:, ['date', 'temp_min', 'temp_max']]
min_max_temp_df.set_index('date', inplace=True)

# Load min and max temperatures
wind_dir_speed_average_df = pd.read_csv('data/wind_dir_speed_average_vaderoarna.csv', delimiter=';', header=0)
wind_dir_speed_average_df['date'] = wind_dir_speed_average_df['Datum']
wind_dir_speed_average_df['wind_dir'] = wind_dir_speed_average_df['Vindriktning']
wind_dir_speed_average_df['wind_average'] = wind_dir_speed_average_df['Vindhastighet']
wind_dir_speed_average_df = wind_dir_speed_average_df.groupby('date')['wind_dir', 'wind_average'].mean()

# Load min and max temperatures
wind_max_df = pd.read_csv('data/wind_max_vaderoarna.csv', delimiter=';', header=0)
wind_max_df['date'] = wind_max_df['Datum']
wind_max_df['wind_max'] = wind_max_df['Byvind']
wind_max_df = wind_max_df.groupby('date')['wind_max'].max()

# Join to weather dataframe
weather_df = rain_df\
    .join(average_temp_df, how='inner')\
    .join(min_max_temp_df, how='inner')\
    .join(wind_dir_speed_average_df, how='inner')\
    .join(wind_max_df, how='inner')

# print(weather_df.head())
# df['date'] = df.index
# df.reset_index(inplace=True)
# df['date'] = pd.to_datetime(df['date'])

# Load sales data
store_df = pd.read_excel('data/butik.xlsx')
store_df.rename(
    columns={
        'Datum': 'date',
        'Bensin': 'store_gasoline_revenue',
        'Diesel': 'store_diesel_revenue',
        'Kajakuthyrning': 'rental_kayak',
        'Skoteruthyrning': 'rental_jet_ski',
        'Båtuthyrning': 'rental_boat',
        'Gasol': 'store_gas',
        'Tillbehör': 'store_accessories',
        'Kiosk': 'store_kiosk',
        'Service': 'store_service',
        'Totalt': 'store_total'
    },
    inplace=True)

store_df.set_index('date', inplace=True)
store_df['store_open'] = 1

# Load prices
prices_df = pd.read_excel('data/priser.xlsx')
prices_df['Datum'] = prices_df['Datum'].dt.date     # possibility of new price more than 1 times a day
prices_df = pd.pivot_table(prices_df, values='Pris', columns='Produkt', index='Datum')
prices_df.fillna(method='ffill', inplace=True)
prices_df.reset_index(inplace=True)
# print(prices_df.head())
# prices_df.drop(columns='Produkt', inplace=True)
prices_df.rename(
    columns={
        'Datum': 'date',
        '95-oktan': 'price_gasoline',
        'Diesel': 'price_diesel',
    },
    inplace=True)
prices_df.set_index('date', inplace=True)

# Create one price row per day instead
prices_per_day_df = pd.DataFrame()
prices_per_day_df['date'] = pd.date_range(start='2010-08-16', end='2019-06-01', freq='D')
prices_per_day_df.set_index('date', inplace=True)
prices_per_day_df = prices_per_day_df.join(prices_df)
prices_per_day_df.fillna(method='ffill', inplace=True)

# Use price to calculate number of liters instead of revenue
store_df = store_df.join(prices_per_day_df, how='left')
store_df['store_diesel_volume'] = store_df['store_diesel_revenue']/store_df['price_diesel']
store_df['store_gasoline_volume'] = store_df['store_gasoline_revenue']/store_df['price_gasoline']
store_df.drop(columns=['price_diesel', 'price_gasoline'], inplace=True)     # join on later
# print(store_df.head())

# Load card machine data
cm_df = pd.read_excel('data/kortautomat.xlsx')
cm_df.rename(
    columns={
        'Tidpunkt': 'date',
        'Volym': 'volume',
        'Belopp': 'revenue',
    },
    inplace=True)
mask = (cm_df['Produkt'] != '[Ingen produkt]') & (cm_df['date'] >= datetime.datetime(year=2013, month=1, day=1))
cm_df = cm_df.loc[mask, :]
cm_df.replace(to_replace='95-oktan', value='cm_gasoline', inplace=True)
cm_df.replace(to_replace='Diesel', value='cm_diesel', inplace=True)
cm_df = pd.pivot_table(cm_df, values=['volume', 'revenue'], columns='Produkt', index='date')
cm_df.reset_index(inplace=True)
cm_df.columns = ['_'.join(reversed(tup)).lstrip('_') for tup in cm_df.columns.to_flat_index()]
cm_df['date'] = cm_df['date'].dt.date
cm_df = cm_df.groupby('date').sum()

# join store and card machine to sales
sales_df = cm_df.join(store_df, how='outer')

# join weather to sales aswel
# df = sales_df.join(weather_df, how='left')
df = weather_df.join(sales_df, how='left').join(prices_per_day_df, how='left')
df.reset_index(inplace=True)
df['date'] = pd.to_datetime(df['date'])
mask = df['date'] >= datetime.datetime(year=2013, month=5, day=4)
df = df.loc[mask, :]
cm_cols = ['cm_gasoline_revenue', 'cm_gasoline_volume', 'cm_diesel_revenue', 'cm_diesel_volume']
df[cm_cols] = df[cm_cols].fillna(0)
df = df.round(2)
print(df.head())
print(df.columns)
print(df.info())


# df.to_excel('data.xlsx')
df['store_open'] = df['store_open'].fillna('0')
df['year'] = df['date'].dt.year
df['month'] = df['date'].dt.month
df = df.loc[:, [
    'date', 'year', 'month', 'store_gasoline_revenue', 'store_diesel_revenue',
    'store_gas', 'store_accessories', 'store_kiosk', 'store_service', 'rental_boat',
    'rental_kayak', 'rental_jet_ski', 'store_total', 'cm_gasoline_revenue', 'cm_diesel_revenue',  'store_open',
    'store_gasoline_volume', 'store_diesel_volume', 'cm_diesel_volume',
    'cm_gasoline_volume', 'price_gasoline', 'price_diesel', 'rain', 'temp_average', 'temp_min', 'temp_max',
    'wind_dir', 'wind_average', 'wind_max']
     ]
print(df.info())
df.to_csv('data.csv', sep=';', header=True, index=False)
