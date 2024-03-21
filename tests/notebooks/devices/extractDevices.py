class ExtractDevices:
    def __init__(self,credencials_path):
        from c8y_api import CumulocityApi
        import json
        from os.path import join

        # file with credentials
        config_path = join(credencials_path)
        with open(config_path) as config_file:
            login = json.load(config_file)

        # instâncias globais
        self.c8y = CumulocityApi(
            base_url= login['url'],
            tenant_id= login['tenant'],
            username= login['username'],
            password= login['password'],
        )
        
    def extrair_devices(self) -> dict:
        from c8y_api.model import DeviceInventory
        import pandas as pd
        dev = DeviceInventory(self.c8y) # EXTRACT

        self.schema = { 
            'id': [],
            'name': [],
            'owner':[]
        } 

        for d in dev.get_all(): # TRANSFORM
            if len(list(d.get_supported_measurements())) > 0:
                self.schema['name'].append(d.name)
                self.schema['id'].append(d.id)
                self.schema['owner'].append(d.owner)
        
        return self.schema
    
    def salvar_csv(self,schema:dict, path_to_save): # LOAD
        from csv import writer
        from os.path import join
        from os import getcwd

        data_path = join(path_to_save)
        with open(data_path,'w', newline='') as data_file:
            w = writer(data_file)
            w.writerow(k for k in schema.keys())

            for row in zip(*schema.values()):
                w.writerow(row)

        return (print(f'devices salvos na pasta {data_path}'))

        
        
if __name__ == '__main__':
    ExtractDevices()
            