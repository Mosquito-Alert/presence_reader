# presence_reader

This project uses a config.py file. This file is quite self-explanatory, but it should contain the credentials to connect to a postgresql database and the full path to the shapefile dbf file.

The main.py script reads the shapefile and scans for presence of albopictus and japonicus. Then it updates the table main_natcodepresence in the madatosapp.mosquitoalert.com with the presence data found in the shapefile.

This update process takes place before the load_observation_data.py of the madatosapp.mosquitoalert.com is executed.
