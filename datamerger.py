import os
import pandas as pd
 
# Check data schema from the MX deposit
class Schemabuilder:
    def __init__(self, schema_folder, data_folder):
        self.schema_folder = schema_folder
        self.data_folder = data_folder
        self.schema = self.load_schema()
 
    def load_schema(self):
        schema = {}
        for file in os.listdir(self.schema_folder):
            if file.endswith('.csv'):
                file_path = os.path.join(self.schema_folder, file)
                df = pd.read_csv(file_path)
                schema[file] = df.columns.tolist()
        return schema
   
    def find_common_key(self):
        schema = self.load_schema()    
        lists = list(schema.values())
         # Find the intersection of all lists
        common_items = set(lists[0]).intersection(*lists)
        return list(common_items)[0]
 
    def merge_data(self):
        common_key= self.find_common_key()
        merged_data = pd.DataFrame()
        for file in os.listdir(self.data_folder):
            if file.endswith('.xlsx'):
                file_path = os.path.join(self.data_folder, file)
                xls = pd.ExcelFile(file_path)
                for sheet_name in xls.sheet_names:
                    sheet_data = pd.read_excel(xls, sheet_name=sheet_name)
                    if merged_data.empty:
                        merged_data = sheet_data
                    else:
                        merged_data = pd.merge(merged_data, sheet_data, on=common_key, how='outer')
        return merged_data
 
    def save_merged_data(self, output_file, common_key):
        merged_data = self.merge_data(common_key)
        merged_data.to_excel(output_file, index=False)
 
 
