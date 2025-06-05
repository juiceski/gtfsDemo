#ETL Skeleton for now
class etl():
    def __init__(self, source, destination):
        self.source = source           
        self.destination = destination
    
    def extract(self):
        pass

    def transform(self):
        pass

    def load(self):
        pass

    def run(self):
        self.extract()
        self.transform()
        self.load()

if __name__ == "__main__": 
    ETL = etl()
    ETL.run()