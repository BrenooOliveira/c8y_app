class ExtractDevices:
    def __init__(self,url,tenant,username,password):
        from c8y_api import CumulocityApi

        # instâncias globais
        self.c8y = CumulocityApi(
            base_url= url,
            tenant_id= tenant,
            username= username,
            password=password,
        )
        
    def extrair_devices(self) -> dict:
        from c8y_api.model import DeviceInventory
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
        with open(data_path,'w', newline='',encoding='utf-8') as data_file:
            w = writer(data_file)
            w.writerow(k for k in schema.keys())

            for row in zip(*schema.values()):
                w.writerow(row)

        return (print(f'devices salvos na pasta {data_path}'))

        
        
if __name__ == '__main__':
    # utilizando a classe como para ela realizar o trabalho que realizará quando importada.
    # extrai devices -> salva CSV
    
    from dotenv import load_dotenv
    from os import getenv
    load_dotenv()
    
    # vars de ambiente
    URL=getenv('URL')
    TENANT=getenv('TENANT')
    USERNAME=getenv('USERNAME')
    PASSWORD=getenv('PASSWORD')
    
    devices = ExtractDevices(URL,TENANT,USERNAME,PASSWORD)
    print(devices.extrair_devices())
    '''
    devices = ExtractDevices(URL,TENANT,USERNAME,PASSWORD)
    devices.salvar_csv(devices.extrair_devices(),'C:/Users/breno.oliveira/Documents/ProgramasPython/myk4.0_dataApp/datas/devices.csv')
    '''
            