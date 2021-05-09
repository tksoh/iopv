from datetime import datetime
import time
import re

def string_to_list(strg):
    # convert string ranges (e.g. "1,2,3-5,6") to list
    res = sum(((list(range(*[int(b) + c
                for c, b in enumerate(a.split('-'))]))
                if '-' in a else [int(a)]) for a in strg.split(',')), [])
    return res

def parse_field(field):
    if field == '*':
        return []
    else:
        return string_to_list(field)

class Crontab:
    def __init__(self, spec, job):
        self.spec = spec
        self.job = job
        self.alive = True

        fields = re.split('[\s\|]+', spec)
        if len(fields) != 5:
            raise ValueError

        self.minutes = parse_field(fields[0])
        self.hours = parse_field(fields[1])
        self.mdays = parse_field(fields[2])
        self.months = parse_field(fields[3])
        self.wdays = parse_field(fields[4])

    def run(self, showtime=False):
        self.alive = True

        while self.alive:
            tm = datetime.now()

            if tm.second >= 1:
                pass
            elif ((not self.minutes or tm.minute in self.minutes) and
                    (not self.hours or tm.hour in self.hours) and
                    (not self.mdays or tm.day in self.mdays) and
                    (not self.months or tm.month in self.months) and
                    (not self.wdays or tm.isoweekday() in self.wdays)):
                if showtime:
                    date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    print(f"\n[{date}]")
                self.job()

            time.sleep(1)

    def stop(self):
        self.alive = False

if __name__ == "__main__":
    def test_job():
        print("testing...")

    cron = Crontab("0-5,18-20,22,52-59 | 9-12,14-18 | * | * | 1-6", test_job)
    cron.run(showtime=True)
