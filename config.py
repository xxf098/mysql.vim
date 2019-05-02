import os, json, traceback

class Config:
    def __init__ (self, path):
        self.path = path
    def load (self):
        try:
            with open(self.path) as config_file:
                data = json.load(config_file)
                if isinstance(data, list):
                    enableData = [x for x in data if x.get('enable', None) == True]
                    data = enableData[0] if len(enableData) > 0 else data[0]
                    data.pop('enable', None)
                return data
        except Exception as e:
            print(traceback.print_exc())
            exit(1)
