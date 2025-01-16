import numpy as np
import pandas as pd
import calendar
from datetime import datetime
from sqlalchemy import create_engine
import matplotlib.pyplot as plt

# Dados iniciais
Curva_tipica = [
    #  CT-08 (Industrial)

         0.44828, 0.44448, 0.42443, 0.40285, 0.41609, 0.44226, 0.50296, 0.84177,
         1.41840, 1.61855, 1.72690, 1.62080, 1.61110, 1.60163, 1.64404, 1.75534,
         1.64330, 1.40273, 1.17373, 0.83101, 0.62876, 0.48362, 0.47292, 0.44404

        #  CT -025 (Comercia)
        #
        # 1.01773,1.03315,1.02677,1.12128,1.04888,1.07128,
        # 1.04770,1.07553,1.02855,1.00333,0.88507,0.87214,
        # 0.99776,0.92413,0.94319,1.00239,0.99546,1.01526,
        # 1.06561,1.02921,0.93614,1.02977,0.91279,0.91689

        # CT-053 (Residencial)
        # 0.69683,0.63609,0.64501,0.52993,0.46504,0.41355,
        # 0.58804,0.74342,0.49790,0.54218,0.77417,0.77644,
        # 1.09644,1.10144,0.81000,0.81092,0.69742,0.77785,
        # 1.31151,2.50768,2.81652,2.02857,1.92475,0.80833

]


Multiplicador = [1.03, 1.03, 1.03, 1.03, 1.03, 0.95, 0.9]

# Cálculo do vetor multiplicado
Vetor = []
for i in Multiplicador:
    for j in Curva_tipica:
        resultado = i * j
        Vetor.append(resultado)

# Configuração de conexão com SQLAlchemy
dados_conexao = "mssql+pyodbc://DESKTOP-UF2O4KN/dados_bi?driver=SQL+Server"
engine = create_engine(dados_conexao)

# Consulta SQL para obter datas distintas
comando_sql_dados = """
	SELECT  top (13) *
from(
select DISTINCT FORMAT(CONVERT(date, [Data_Atual]), 'yyyy-MM-dd') AS [Data_Atual]
FROM [ORBIS]
WHERE baixa_tensao_DGV_CLIENTE='7201768092' AND [baixa_tensao_Usina] = 'Fazenda Vale do Sol' and baixa_tensao_DTE_TE307T_TEXT40='Comercial' and baixa_tensao_DGV_INSTALACAO='3001450194') subquery
order by  [Data_Atual] asc

"""

# Executando a consulta e armazenando os resultados no DataFrame
df2 = pd.read_sql(comando_sql_dados, engine)
print("Conexão Bem Sucedida")

Data = df2['Data_Atual']
print(df2)

# Lista para acumular todos os resultados
resultados_totais = []

# Iterando sobre as datas obtidas
vetor_data = []
for i in Data:
    vetor_data.append(i)

# Loop principal para processar os dados de consumo
for i in vetor_data:
    comando_sql = f"""
SELECT 
        [baixa_tensao_Usina], 
        [baixa_tensao_DGV_CLIENTE] AS DGV_CLIENTE,
        [dias],
        CONVERT(float, [baixa_tensao_DGV_CONSUMO_01]) / (dias*24) AS Consumo,
        FORMAT(CONVERT(date, [Data_Atual]), 'yyyy-MM-dd') AS [Data_Atual],
        DAY([Data_Atual]) AS DIA,
        MONTH([Data_Atual]) AS MES,
		year([Data_Atual]) as ANO,
        [DATA_ANT],
        DAY([DATA_ANT]) AS Dia_Mes_Passado,
        MONTH([DATA_ANT]) AS Mes_Passado,
		YEAR([Data_Atual]) as ANO_do_mes_passado
    FROM [ORBIS]
    WHERE baixa_tensao_DGV_CLIENTE='7201768092' AND [baixa_tensao_Usina] = 'Fazenda Vale do Sol' and baixa_tensao_DTE_TE307T_TEXT40='Comercial' and baixa_tensao_DGV_INSTALACAO='3001450194'
    AND CONVERT(date, [Data_Atual]) = '{i}';
    """

    # Executando a consulta e armazenando os resultados no DataFrame
    df = pd.read_sql(comando_sql, engine)

    # Acessando os dados retornados
    Consumo = df['Consumo'][0]
    Dia_atual = df['DIA'][0]
    Dias = df['dias'][0]
    MES = df['MES'][0]
    ANO=df['ANO'][0]
    ANO_PASSADO=df['ANO_do_mes_passado'][0]
    Mes_Passado = df['Mes_Passado'][0]
    Dia_Mes_Passado = df['Dia_Mes_Passado'][0]

    dias_no_mes_anterior = calendar.monthrange(ANO, Mes_Passado)[1]

    dias_semana = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]
    multiplicadores = {"Segunda": 1.03, "Terça": 1.03, "Quarta": 1.03, "Quinta": 1.03, "Sexta": 1.03, "Sábado": 0.95, "Domingo": 0.9}

    # Processando dias do mês anterior
    Horas = 0

    # Processando dias do mês anterior


    Horas = 0


    for dia in range(Dia_Mes_Passado, dias_no_mes_anterior +1 ):

        if Mes_Passado == 12:
            ANO_PASSADO = 2023
        if Horas > 23:
            Horas = 0
        for j in Curva_tipica:
            data_completa = datetime(ANO, Mes_Passado, dia)
            dia_semana = dias_semana[data_completa.weekday()]
            consumo_ajustado = round(Consumo * multiplicadores[dia_semana] * j, 2)
            resultados_totais.append({
                'ds': f"{ANO_PASSADO}-{Mes_Passado}-{dia} {Horas}:00",
                'y': consumo_ajustado
            })
            Horas += 1


    # Processando dias do mês atual
    for dia in range(1, Dia_atual):
        if Mes_Passado == 12:
            ANO_PASSADO = 2023
        if Horas > 23:
            Horas = 0

        for j in Curva_tipica:
            data_completa = datetime(ANO, MES, dia)
            dia_semana = dias_semana[data_completa.weekday()]
            consumo_ajustado = round(Consumo * multiplicadores[dia_semana] * j, 2)
            resultados_totais.append({
                'ds': f"{ANO}-{MES}-{dia} {Horas}:00",
                'y': consumo_ajustado
            })
            Horas += 1

# Convertendo os resultados acumulados para um DataFrame final
vetor_ano_total = pd.DataFrame(resultados_totais)

print(vetor_ano_total)

caminho_arquivo = r'C:\Users\paulo\Downloads\Cliente_CO_7201768092.csv'
vetor_ano_total.to_csv(caminho_arquivo, index=False)
print(f'Dados exportados para o arquivo: {caminho_arquivo}')
