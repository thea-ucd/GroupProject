import requests
import mysql.connector
import sys
import config

try:
    # Setup the database connection
    mydb = mysql.connector.connect(
        host=config.host,
        user=config.user,
        passwd=config.passwd,
        database=config.db_name,
        auth_plugin='mysql_native_password'
    )

    # Get the data from the API
    url = "https://api.jcdecaux.com/vls/v1/stations?contract=Dublin&apiKey=" + config.APIKEY
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception if the API request fails
    data = response.json()

    # Create the insert statement for the new data
    sql_insert = "INSERT INTO station (address, banking, bike_stands, bonus, contract_name, name, number, " \
                 "position_lat, " \
                 "position_lng, status  " \
                 "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    # Create a cursor
    mycursor = mydb.cursor()

    # Remove foreign key check and truncate existing values
    sql_fk_check = "SET FOREIGN_KEY_CHECKS=0; TRUNCATE dublin_bikes_static; SET FOREIGN_KEY_CHECKS=1;"
    mycursor.execute(sql_fk_check)

    # Iterate through the data response object and perform inserts
    for elem in data:
        val = (elem["address"], elem["banking"], elem["bike_stands"], elem["bonus"],
               elem["contract_name"]["lat"], elem["position"]["lng"], elem["banking"], elem["bonus"])
        mycursor.execute(sql_insert, val)
    mydb.commit()

    # Close the cursor and the connection
    mycursor.close()
    mydb.close()

except mysql.connector.Error as err:
    print("Unable to connect to database: {}".format(err))
    sys.exit(1)

except requests.exceptions.RequestException as e:
    print("API request failed: {}".format(e))
    sys.exit(1)
