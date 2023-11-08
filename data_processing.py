import csv, os
__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

def loadTable(fileName):
    tab = [];
    with open(os.path.join(__location__, fileName)) as f:
        rows = csv.DictReader(f);
        for r in rows:
            tab.append(dict(r));
    return tab;

class DB:
    def __init__(self):
        self.database = []

    def insert(self, table):
        self.database.append(table)

    def search(self, table_name):
        for table in self.database:
            if table.table_name == table_name:
                return table
        return None
    
import copy
class Table:
    def __init__(self, table_name, table):
        self.table_name = table_name
        self.table = table
    
    def join(self, other_table, common_key):
        joined_table = Table(self.table_name + '_joins_' + other_table.table_name, [])
        for item1 in self.table:
            for item2 in other_table.table:
                if item1[common_key] == item2[common_key]:
                    dict1 = copy.deepcopy(item1)
                    dict2 = copy.deepcopy(item2)
                    dict1.update(dict2)
                    joined_table.table.append(dict1)
        return joined_table
    
    def filter(self, condition):
        filtered_table = Table(self.table_name + '_filtered', [])
        for item1 in self.table:
            if condition(item1):
                filtered_table.table.append(item1)
        return filtered_table
    
    def aggregate(self, function, aggregation_key):
        temps = []
        for item1 in self.table:
            temps.append(float(item1[aggregation_key]))
        return function(temps)
    
    def select(self, attributes_list):
        temps = []
        for item1 in self.table:
            dict_temp = {}
            for key in item1:
                if key in attributes_list:
                    dict_temp[key] = item1[key]
            temps.append(dict_temp)
        return temps

    def __str__(self):
        return self.table_name + ':' + str(self.table)

citiesTable = Table('cities', loadTable("Cities.csv"))
countriesTable = Table('countries', loadTable("Countries.csv"))
titanicTable = Table('titanic', loadTable("Titanic.csv"))
plrsTable = Table('players', loadTable("Players.csv"))
teamsTable = Table('teams', loadTable("Teams.csv"))
my_DB = DB()
my_DB.insert(citiesTable)
my_DB.insert(countriesTable)
my_DB.insert(titanicTable)
my_DB.insert(plrsTable)
my_DB.insert(teamsTable)
my_citiesTable = my_DB.search('cities')

print("Test filter: only filtering out cities in Italy") 
my_citiesTable_filtered = my_citiesTable.filter(lambda x: x['country'] == 'Italy')
print(my_citiesTable_filtered)
print()

print("Test select: only displaying two fields, city and latitude, for cities in Italy")
my_citiesTable_selected = my_citiesTable_filtered.select(['city', 'latitude'])
print(my_citiesTable_selected)
print()

print("Calculting the average temperature without using aggregate for cities in Italy")
temps = []
for item in my_citiesTable_filtered.table:
    temps.append(float(item['temperature']))
print(sum(temps)/len(temps))
print()

print("Calculting the average temperature using aggregate for cities in Italy")
print(my_citiesTable_filtered.aggregate(lambda x: sum(x)/len(x), 'temperature'))
print()

print("Test join: finding cities in non-EU countries whose temperatures are below 5.0")
my_countriesTable = my_DB.search('countries')
my_table3 = my_citiesTable.join(my_countriesTable, 'country')
my_table3_filtered = my_table3.filter(lambda x: x['EU'] == 'no').filter(lambda x: float(x['temperature']) < 5.0)
print(my_table3_filtered.table)
print()
print("Selecting just three fields, city, country, and temperature")
print(my_table3_filtered.select(['city', 'country', 'temperature']))
print()

print("Print the min and max temperatures for cities in EU that do not have coastlines")
my_table3_filtered = my_table3.filter(lambda x: x['EU'] == 'yes').filter(lambda x: x['coastline'] == 'no')
print("Min temp:", my_table3_filtered.aggregate(lambda x: min(x), 'temperature'))
print("Max temp:", my_table3_filtered.aggregate(lambda x: max(x), 'temperature'))
print()

print("Print the min and max latitude for cities in every country")
for item in my_countriesTable.table:
    my_citiesTable_filtered = my_citiesTable.filter(lambda x: x['country'] == item['country'])
    if len(my_citiesTable_filtered.table) >= 1:
        print(item['country'], my_citiesTable_filtered.aggregate(lambda x: min(x), 'latitude'), my_citiesTable_filtered.aggregate(lambda x: max(x), 'latitude'))
print()
