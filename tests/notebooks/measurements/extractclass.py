from datetime import timedelta
import pandas as pd
from numpy import nan

class ExtractMeasurements:
    def __init__(self,c8y,source):
        '''
        ::PARAMETERS::
        c8y -> instância 'CumulocityApi' da lin c8y-api
        source -> id do device dentro da instância do Cumulocity IoT
        ::DESCRIPTION::
        inicia os dois objetos para utilizarmos nas demais funções
        as variaveis de timedelta são iniciadas aqui pois elas irão controlar o periodo da requisição na API

        '''

        #instância necessárias para realizar as operaçoes na API
        self.c8y = c8y
        self.source = source

        self.delta_twohours = timedelta(hours=2) # 2 hours older
        self.delta_onehour = timedelta(hours=1) # 1 hour older

        #EXTRACT
        self.one_hour_measu = self.c8y.measurements.select(source=self.source,max_age=self.delta_onehour) #measurements de uma hora atrás
        self.two_hour_measu = self.c8y.measurements.select(source=self.source,max_age=self.delta_twohours, min_age=self.delta_onehour) #measurements de duas horas atras

    #EXTRACT
    def verify_device_type(self): 
        '''
        ::PARAMETER:: 
        - Source >> device do cumulocity
        - c8y >> instância cumulocity

        ::DESCRIPTION::
        - essa funcao funciona como uma ponta paraas classes 'extract_..._devices' 
        - ela avalia o output do device e direciona para a classse correta de extacao
        
        ::RETURN::
        - nao retorna nada pois eh uma funcao auxiliar que direciona para outras funcoes
        '''

        #capturar apenas um fragmento do retorno da API e então condicionar. 
        for i in self.one_hour_measu:
            ...
        device_type = len(i.fragments.keys()) 

        if device_type == 1: 
            print(f'o device {self.source} é NOVO')
            return self.extract_new_devices() # os new_devices vêm separados. Cada fragmento é um tipo de measu 
        else:
            print(f'o device {self.source} NÃO é novo')
            return self.extract_old_devices() # os old devices vêm no formato compactado. Ou seja, ele retorna > 1 pois todas as measu vêm num só pacote
        
        ...

    # EXTRACT
    def extract_new_devices(self) -> list: 
        '''

        ::PARAMETERS::
            Não possui.
        ::DESCRIPTION::
            há diferentes devices com formas de aquisição de dados diferentes pois a forma de subir para a nuvem do Cumulocity IoT foi diferente.
            Esse objeto contempla a extração de dados de devices que foram alocados no Cumulocity via conector do eWon. Tal forma de subir os dados é tendência por sua simplicidade e alta capacidade de controle de time interval.
        ::RETURN::
            retorna uma lista com dois dataframes.
            o df[0] é o dataframe com dados de UMA hora atrás
            o df[1] é o dataframe com dados de DUAS hora atrás
        '''
        #TRANSFORM
        def tratamento(_hour_measu):
            '''
            ::PARAMETER:: 
                _hour_measu -> generator gerado por CumulocityApi.measurements.select()
            :::DESCRIPTION::
                formata os dados requisitados num dataframe inserindo para cada dado de nome novo uma nova coluna no dataframe
                Alguns dados podem ter o intervalo de aquisição diferente, para isso, inserimos a var 'comp_min' para equilibrar os arrays e inserir num dataframe de linhas iguais.
            ::RETURN:::
                retorna o dataframe dinamicamente formatado
            '''
            df = {}
            for i,v in enumerate(_hour_measu): 
                fragments = v.fragments # caracteristicas do dados puxado. Daí tiramos as propriedes que queremos
                col_name = list(fragments.keys())[0] # nome da nossa coluna no df
                aux = list(fragments[col_name].keys())[0] # parametro utilizado para entrar no aninhamento
                value = [fragments[col_name][aux]['value']] #valor do parametro atual -> inserimos ele numa lista para poder appendar no script abaixo
                #value = [list(fragments[col_name][aux].values())[1]] #valor do parametro atual -> inserimos ele numa lista para poder appendar no script abaixo
                # no primeiro loop, precisamos definir as colunas do dataframe, nos próximos basta appendarmos.
                try:
                    df[col_name].append(value[0])
                except KeyError as error:
                    df[col_name] = value

                # alguns parametros possuem time interval de envio de dados diferentes. Isso faz com que seja necessario equilibrarmos ele inserindo 'nan' nos dados faltantes
                comp_max = max(map(len,df.values())) # puxamos numero máximo de linhas do futuro dataframe
                for i,v in enumerate(df.items()):
                    fill_rows = comp_max - len(v[1]) # preencheremos de forma a ficar equilibrada, portanto, é necessário subt o comp_max pelo o comprimento da coluna no loop
                    df[v[0]].extend(nan for rows in range(0,fill_rows)) # preenchemos os valores faltantes com 'nan'
                    
            df = pd.DataFrame(df)
            return df
        
        # retorna uma lista com os dfs de uma hora atrás e duas horas atrás da mesma source
        return [tratamento(self.one_hour_measu),tratamento(self.two_hour_measu)]


    def extract_old_devices(self):

        #TRANSFORM
        def tratamento(_hour_measu):
            df = {}
            for i,v in enumerate(_hour_measu):
                fragments = v.fragments.values() # taking the actual parameter
                for f in fragments:
                    col_name = list(f.keys())[0] # setting 
                    value = [list(f[col_name].values())[1]]
                    
                    try:
                        df[col_name].append(value[0])
                    except KeyError as error:
                        df[col_name] = value

                # alguns parametros possuem time interval de envio de dados diferentes. Isso faz com que seja necessario equilibrarmos ele inserindo 'nan' nos dados faltantes
                comp_max = max(map(len,df.values())) # puxamos numero máximo de linhas do futuro dataframe
                for i,v in enumerate(df.items()):
                    fill_rows = comp_max - len(v[1]) # preencheremos de forma a ficar equilibrada, portanto, é necessário subt o comp_max pelo o comprimento da coluna no loop
                    df[v[0]].extend(nan for rows in range(0,fill_rows)) # preenchemos os valores faltantes com 'nan'
                    
            df = pd.DataFrame(df)
            return df

        return [tratamento(self.one_hour_measu),tratamento(self.two_hour_measu)]        

    def extrac_N_device(self): # reservado para mais um tipo de device
       # TRANSFORM
        def tratamento(_hour_measu):
            return ...
        return tratamento(...)

    def load_parquet(self): # LOAD
        pass
        
        