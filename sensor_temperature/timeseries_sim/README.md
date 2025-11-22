# SIMULATION OF TIMESERIES

This library provides a simulation tool to get data of Temperature and Presense sensor:

You must initiate the simulator with a base timestamp and the simulator will provide the temperature and presence values,
also returning the simulated timestamp, where 1 second in the real world will be 1 second in the simulation.

Simulator classes:
TemperatureSimulator(<current datetime>)
    - get_temperature(<current datetime>, <heatpump status>)
PresenceSimulator(<current datetime>)
    - get_presence(<current datetime>)

Usage:
```python
from timeseries_get import TemperatureSimulator, PresenceSimulator
from datetime import datetime
# iniciate the simulators with the current time
ts = TemperatureSimulator(datetime.now())
ps = PresenceSimulator(datetime.now())

# get the simulated time and value from the object
print(ts.get_temperature(datetime.now(), 1))
print(ps.get_presence(datetime.now()))

```
