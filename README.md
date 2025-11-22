# TEMPERATURE SENSOR

This docker implements a temperature simulator of a room with a heatpump. 
It will report simulated temperature readings with the ratio 1:60 where 1 second in the simulation is 1 minute in reality.
If the heatpump is off, the temperature will decrease. If it is on it will start to increase

### MESSAGE FORMAT
The sensor reports {"timestamp": "<yyyy-mm-ddTHH:MM:SS.sss>", "value": <float>} indicating the simulated time and the value
The sensor listens messages with the format {"status":<status>} expecting values of *1-on*, *0-off*

### CONFIGURATION

You have to specify the following environment variables to configurate the sensor:
    - MQTT_HOST=<host of the user's mosquito in the local network>
    - SENSOR_TOPIC_DATA=<topic where to report sensor readings>
    - SENSOR_TOPIC_STATUS=<topic to send status messages>

### DOCKER COMPOSE

- Create different docker composes to have different sensor at user's homes.
- Launch the docker compose with `--project-name` to have the same docker compose running with different name  
`docker-compose -f <file_yaml> --project-name <specific_name> up`
- Launch the docker compose with `--build` to build the containers specified with "build" instead of image
`docker-compose -f <file_yaml> --project-name <specific_name> up --build`
