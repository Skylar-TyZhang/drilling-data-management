import os
import pandas as pd
 
# Check data schema from the MX deposit
class SchemaBuilder:
    def __init__(self, schema_folder, data_folder):
        self.schema_folder = schema_folder
        self.data_folder = data_folder
        self.schema = self.load_schema()
        self.common_key = self.find_common_key()
 
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
        return list(common_items)[0] if common_items else None
 
    def merge_data(self):
        common_key= self.find_common_key()
        if not common_key:
            raise ValueError("No common key found in the schema files.")
        
        merged_data = pd.DataFrame()
        for filefolder in os.listdir(self.data_folder):
            file_path = os.path.join(self.data_folder,filefolder)
            if os.path.isdir(file_path):  # Check if it's a directory
                for table in os.listdir(file_path):
                    if table.endswith('.xlsx'):
                        table_path = os.path.join(file_path, table)
                        xls = pd.ExcelFile(table_path)
                        for sheet_name in xls.sheet_names:
                            sheet_data = pd.read_excel(xls, sheet_name=sheet_name)
                            if merged_data.empty:
                                merged_data = sheet_data
                            else:
                                merged_data = pd.merge(merged_data, sheet_data, on=common_key, how='outer')
                                
                # Write the merged data to a CSV file in the original Excel folder
                output_file_path = os.path.join(file_path, 'merged_data.csv')
                merged_data.to_csv(output_file_path, index=False)
                print(f"Merged data written to {output_file_path}")
        return merged_data
            
 
    def save_merged_data(self, output_file, common_key):
        merged_data = self.merge_data(common_key)
        merged_data.to_excel(output_file, index=False)
 
 
