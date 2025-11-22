import time
from datetime import datetime
import random


class TemperatureSimulator(object):
    outlier_ratio = 0.2

    def __init__(self, current_time):
        self.start_time = current_time
        self.room_temp = 20

    @staticmethod
    def sim_temperature(hour, month, status, room_temp):
        base_tmp = 20
        if month == 12 or 1 <= month < 3:
            base_tmp = 8
        elif 3 <= month < 7:
            base_tmp = 16
        elif 7 <= month < 10:
            base_tmp = 30
        elif 10 <= month < 12:
            base_tmp = 16

        if 21 <= hour <= 23 or 0 <= hour < 9:
            base_tmp = base_tmp + random.uniform(-2, 2)
        if 9 <= hour <= 13:
            base_tmp = base_tmp + 5 + random.uniform(-2, 2)
        if 13 <= hour < 18:
            base_tmp = base_tmp + 10 + random.uniform(-2, 2)
        if 18 <= hour < 21:
            base_tmp = base_tmp + 5 + random.uniform(-2, 2)
        if status == 1:
            if room_temp:
                return min(35, room_temp + (base_tmp / 60))
            else:
                return base_tmp
        else:
            return max(10, room_temp - (base_tmp / 60))

    def get_temperature(self, current_date, dev_status):
        sim_diff = current_date.timestamp() - self.start_time.timestamp()
        sim_time = datetime.fromtimestamp(self.start_time.timestamp() + (sim_diff * 60))
        if random.uniform(0, 1) < TemperatureSimulator.outlier_ratio:
            return sim_time, 100
        self.room_temp = TemperatureSimulator.sim_temperature(sim_time.hour, sim_time.month, dev_status, self.room_temp)
        return sim_time, self.room_temp


class PresenceSimulator(object):
    outlier_ratio = 0.2

    def __init__(self, current_time):
        self.start_time = current_time

    @staticmethod
    def sim_presence(hour, day_of_week):
        if 0 <= hour < 7:
            return 0
        elif 7 <= hour < 8:
            return 1
        elif 8 <= hour < 12:
            if day_of_week == 6 or day_of_week == 0:
                return 1
            else:
                return 0
        elif 12 <= hour < 17:
            if day_of_week in [6, 0, 3]:
                return 0
            else:
                return 1
        elif 17 <= hour <= 23:
            return 1

    def get_presence(self, current_date):
        sim_diff = current_date.timestamp() - self.start_time.timestamp()
        sim_time = datetime.fromtimestamp(self.start_time.timestamp() + (sim_diff * 60))
        if random.uniform(0, 1) < PresenceSimulator.outlier_ratio:
            return sim_time, 100
        return sim_time, PresenceSimulator.sim_presence(sim_time.hour, int(sim_time.strftime("%w")))


if __name__ == '__main__':
    ts = TemperatureSimulator(datetime.now())
    ps = PresenceSimulator(datetime.now())
    for i in range(300):
        print(ts.get_temperature(datetime.now(), 1))
        print(ps.get_presence(datetime.now()))
        time.sleep(1)
