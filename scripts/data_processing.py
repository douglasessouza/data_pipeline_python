import json
import csv

class Data: 

    def __init__(self, path, type_data):
        self.path = path 
        self.type_data = type_data
        self.data = self.data_reading()
        self.column_names = self.get_columns()
        self.number_lines = self.size_data()

    #Creating a function to read the json file 
    def json_reading(self):    
        json_data = []
        with open(self.path, 'r') as file:
            json_data = json.load(file)
        return json_data

    #Creating a function to read the csv file
    def csv_reading(self):
        csv_data = []
        with open(self.path, 'r') as file: 
            spamreader = csv.DictReader(file, delimiter=',')
            for row in spamreader:
                csv_data.append(row)
        return csv_data

    #Handling with any data
    def data_reading(self):
        datas = []  

        if self.type_data == 'csv':
            datas = self.csv_reading()

        elif self.type_data == 'json':
            datas = self.json_reading()
        
        elif self.type_data == 'list':
            datas = self.path
            self.path = 'list in memory'

        return datas
    
    #Getting columns names
    def get_columns(self):
        return list(self.data[-1].keys())
    
    #Renaming the csv columns 
    def rename_columns(self, key_mapping): 
        new_data = []

        for old_dict in self.data:
            dict_temp = {}
            for old_key, value in old_dict.items():
                dict_temp[key_mapping[old_key]] = value
            new_data.append(dict_temp)
            
        self.data = new_data
        self.column_names = self.get_columns()

    #Data size
    def size_data(self):
        return len(self.data)
    
    #Join
    def join(dataA, dataB):
        combined_list = []
        combined_list.extend(dataA.data)
        combined_list.extend(dataB.data)
        
        return Data(combined_list, 'list')
    
    #Writing the data, adding NuN values for the difference between columns names (Data da Venda)
    def data_table_trasnformation (self):

        data_table_combined = [self.column_names]

        for row in self.data: 
            line = []
            for column in self.column_names:
                line.append(row.get(column, 'NuN'))
            data_table_combined.append(line)

        return data_table_combined
    
    def saving_data(self, path):

        data_table_combined = self.data_table_trasnformation()

        with open(path, 'w') as file: 
            writer = csv.writer(file)
            writer.writerows(data_table_combined)