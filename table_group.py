from prettytable import PrettyTable

table = PrettyTable()
table.field_names = ["Name", "Age", "Country"]
table.add_row(["John", 28, "USA"])
table.add_row(["Anna", 24, "UK"])
print(table)