import os
import json
from json.decoder import JSONDecodeError

class Sample(object):
    def __init__(self, time, value):
        self.time = time
        self.value = value
    
    def __eq__(self, other):
        return self.time == other.time and self.value == other.value


class TSDB(object):
    def __init__(self, fileName='TSDB.json'):
        self.fileName = fileName

        if not os.path.isfile(fileName) or not os.access(fileName, os.R_OK):
            print("Creating TSDB for write and read...")
            with open(fileName, 'w+') as db:
                db.write(json.dumps({}))

    
    def persist(self, samples):
        with open(self.fileName, 'r+') as db:
            data = json.load(db)
            
            for sample in samples:
                if sample.time in data:
                    data[sample.time].append(sample.value)
                else:
                    data[sample.time] = [sample.value]

            db.seek(0)
            json.dump(data, db)


    def query(self, startTime, endTime):
        if not isinstance(startTime, int) or not isinstance(endTime, int):
            return

        if os.path.isfile(self.fileName) and os.access(self.fileName, os.R_OK):
            # Load data from database to be read
            with open(self.fileName, 'r+') as db:
                data = json.load(db)
            
            result = []
            for time in range(startTime, endTime):
                if str(time) in data:
                    # Grab only enough such that we return a maximum of 100 samples
                    if len(result) + len(data[str(time)]) >= 100:
                        foundSamples = [Sample(time, value) for value in data[str(time)][:100]]
                        return result + foundSamples

                    # Add samples to result that were found in database
                    foundSamples = [Sample(time, value) for value in data[str(time)]]
                    result += foundSamples
            return result
        else:
            print("Query failed because db doesn't exist")
    
    def clear(self):
        with open(self.fileName, 'w+') as db:
            db.write(json.dumps({}))

