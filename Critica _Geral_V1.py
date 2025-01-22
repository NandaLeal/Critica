import pandas as pd
import dataframe_image as dfi
import numpy as np

#Base de Cadastro
cadastro_bm=pd.read_csv('0105070402-BM.csv',encoding="ISO-8859-1",sep=";",decimal=',')
cadastro_bm['Cód PDV'] = cadastro_bm['Cód PDV'].astype(str)
cadastro_bm['Cod']=['2-']+cadastro_bm['Cód PDV']
cadastro_moc=pd.read_csv('0105070402-MOC.csv',encoding="ISO-8859-1",sep=";",decimal=',')
cadastro_moc['Cód PDV'] = cadastro_moc['Cód PDV'].astype(str)
cadastro_moc['Cod']=['1-']+cadastro_moc['Cód PDV']
cadastro=pd.concat([cadastro_bm,cadastro_moc])
cpf_estouro=cadastro
#Base de Pedido
pedido_bm=pd.read_csv('03.01.11_BM.csv',encoding="ISO-8859-1",sep=";",decimal=',')
pedido_bm['Cod. Cliente'] = pedido_bm['Cod. Cliente'].astype(str)
pedido_bm['Cod']=['2-']+pedido_bm['Cod. Cliente']
pedido_moc=pd.read_csv('03.01.11_MOC.csv',encoding="ISO-8859-1",sep=";",decimal=',')
pedido_moc['Cod. Cliente'] = pedido_moc['Cod. Cliente'].astype(str)
pedido_moc['Cod']=['1-']+pedido_moc['Cod. Cliente']
pedido=pd.concat([pedido_moc,pedido_bm])
pedido_cesta=pedido[pedido["Tipo Movimento"] == 51]
pedido_giro=pedido
pedidox=pedido.drop_duplicates(subset=['Num Pedido'])
pedidox=pedidox[pedidox["Tipo Movimento"] == 51]
pedido_alto=pedido.drop_duplicates(subset=['Num Pedido'])
pedidos_estouros=pedido.drop_duplicates(subset=['Num Pedido'])
pedidos_estouros=pedidos_estouros[pedidos_estouros["Tipo Movimento"] == 51]
pedido=pedido.drop_duplicates(subset=['Num Pedido'])

#Dados Limite
dados_limite_bm=pd.read_csv('01.05.07.21_BM.csv',encoding="ISO-8859-1",sep=";",decimal=',')
dados_limite_bm['Cliente'] = dados_limite_bm['Cliente'].astype(str)
dados_limite_bm['Cod']=['2-']+dados_limite_bm['Cliente']
dados_limite_moc=pd.read_csv('01.05.07.21_MOC.csv',encoding="ISO-8859-1",sep=";",decimal=',')
dados_limite_moc['Cliente'] = dados_limite_moc['Cliente'].astype(str)
dados_limite_moc['Cod']=['1-']+dados_limite_moc['Cliente']
dados_limite=pd.concat([dados_limite_bm,dados_limite_moc])

#Faturamento_Periodo
faturamento_bmP=pd.read_csv("P03.02.37_BM.csv",encoding="ISO-8859-1",sep=";",decimal=',')
faturamento_bmP['Cliente'] = faturamento_bmP['Cliente'].astype(str)
faturamento_bmP['Cod']=['2-']+faturamento_bmP['Cliente']
faturamento_mocP=pd.read_csv("P03.02.37_MOC.csv",encoding="ISO-8859-1",sep=";",decimal=',')
faturamento_mocP['Cliente'] = faturamento_mocP['Cliente'].astype(str)
faturamento_mocP['Cod']=['1-']+faturamento_mocP['Cliente']
faturamentoP=pd.concat([faturamento_mocP,faturamento_bmP])

#Faturamento_Tri
faturamento_bmT=pd.read_csv("TRI_BM.csv",encoding="ISO-8859-1",sep=";",decimal=',')
faturamento_bmT['Cliente'] = faturamento_bmT['Cliente'].astype(str)
faturamento_bmT['Cod']=['2-']+faturamento_bmT['Cliente']
faturamento_mocT=pd.read_csv("TRI_MOC.csv",encoding="ISO-8859-1",sep=";",decimal=',')
faturamento_mocT['Cliente'] = faturamento_mocT['Cliente'].astype(str)
faturamento_mocT['Cod']=['1-']+faturamento_mocT['Cliente']
faturamentoT=pd.concat([faturamento_mocT,faturamento_bmT])
faturamentoT=faturamentoT[faturamentoT['Mot. Cancelamento'] == "   "]
faturamentoT['Total'] = faturamentoT['Total'].str.replace(".","")
faturamentoT['Total'] = faturamentoT['Total'].str.replace(",",".")
faturamentoT['Total'] = faturamentoT['Total'].astype(float)
faturamentoT.rename(columns={'Total': 'Faturamento Total'}, inplace = True)
faturamentoT=faturamentoT.groupby(['Cod'],as_index=False).agg({'Faturamento Total':sum}, group_keys =False)

#Faturamento_MES
faturamento_bm=pd.read_csv('03.02.37_BM.csv',encoding="ISO-8859-1",sep=";",decimal=',')
faturamento_bm['Cliente'] = faturamento_bm['Cliente'].astype(str)
faturamento_bm['Cod']=['2-']+faturamento_bm['Cliente']
faturamento_moc=pd.read_csv('03.02.37_MOC.csv',encoding="ISO-8859-1",sep=";",decimal=',')
faturamento_moc['Cliente'] = faturamento_moc['Cliente'].astype(str)
faturamento_moc['Cod']=['1-']+faturamento_moc['Cliente']
faturamento=pd.concat([faturamento_moc,faturamento_bm])
faturamento=faturamento[faturamento['Mot. Cancelamento'] == "   "]
faturamento['Total'] = faturamento['Total'].str.replace(".","")
faturamento['Total'] = faturamento['Total'].str.replace(",",".")
faturamento['Total'] = faturamento['Total'].astype(float)
faturamento.rename(columns={'Total': 'Faturamento Total'}, inplace = True)
faturamento=faturamento.groupby(['Cod'],as_index=False).agg({'Faturamento Total':sum}, group_keys =False)

#Base de Giro
giro_bm=pd.read_csv('02.02.53_BM.csv',encoding="ISO-8859-1",sep=";",decimal=',')
giro_bm['Cliente'] = giro_bm['Cliente'].astype(str)
giro_bm['Cod']=['2-']+giro_bm['Cliente']
giro_moc=pd.read_csv('02.02.53_MOC.csv',encoding="ISO-8859-1",sep=";",decimal=',')
giro_moc['Cliente'] = giro_moc['Cliente'].astype(str)
giro_moc['Cod']=['1-']+giro_moc['Cliente']
giro=pd.concat([giro_moc,giro_bm])
#Clientes Especiais
cliente=pd.read_csv('cliente.csv',encoding="utf-8",sep=";",decimal=',')
#Produto
produto=pd.read_csv('produto.csv',encoding="ISO-8859-1",sep=";",decimal=',')

#VALOR MÍNIMO
#Selecionar quais informações preciso
#Primeiro quero tdos os pedidos tipo venda
Pedido_venda=pedido[pedido["Tipo Movimento"] == 51]
#Depois separo por forma de pagamento
#Pedido_no_dinheiro=Pedido_venda[Pedido_venda["Forma Pgto"] == 'BL ']
Pedido_no_dinheiro=Pedido_venda
Pedido_condição=Pedido_no_dinheiro.groupby(["Cond Pgto"],as_index=False).agg({'Valor Pedido':sum}, group_keys =False)
Pedido_no_boleto=Pedido_no_dinheiro.groupby(["Cod","Empresa Origem","Cod. Cliente","Nome Cliente","Forma Pgto","Cod. Vendedor"],as_index=False).agg({'Valor Pedido':sum}, group_keys =False)
Pedido_no_boleto=Pedido_no_dinheiro.groupby(["Cod","Empresa Origem","Cod. Cliente","Nome Cliente","Cod. Vendedor"],as_index=False).agg({'Valor Pedido':sum}, group_keys =False)
#Pedido_no_boleto=Pedido_no_boleto[Pedido_no_boleto["Valor Pedido"] < 200]
Pedido_no_boleto=Pedido_no_boleto[Pedido_no_boleto["Valor Pedido"] < 200]
#novo
Pedido_venda=Pedido_no_boleto
#Pedido_no_dinheiro=Pedido_venda.loc[Pedido_venda["Forma Pgto"] != 'BL ']
Pedido_no_dinheiro=Pedido_no_dinheiro.groupby(["Cod","Empresa Origem","Cod. Cliente","Nome Cliente","Cod. Vendedor"],as_index=False).agg({'Valor Pedido':sum}, group_keys =False)
Pedido_no_dinheiro.insert(loc=5,column="Forma Pgto",value="DH",allow_duplicates= True)
Pedido_no_dinheiro=Pedido_no_dinheiro[Pedido_no_dinheiro["Valor Pedido"] < 150]
#Pedido_venda=pd.concat([Pedido_no_dinheiro,Pedido_no_boleto])
#Preciso saber quais são desse que tem bonificaçãoC:\Users\pedro.alves\PycharmProjects\pythonProject\Valor
Pedido_com_bonificação=pedido[pedido["Tipo Movimento"] == 52]
Pedido_com_bonificação=Pedido_com_bonificação.iloc[:,[38]]
Pedido_com_bonificação.insert(loc=1,column="Bonificação",value="Sim",allow_duplicates= True)
#Preciso saber quais são so clientes não tem compra
cliente_sem_comprar=cadastro
cliente_sem_comprar=cliente_sem_comprar[cliente_sem_comprar["Data da Última Compra"] =="          "]
cliente_sem_comprar=cliente_sem_comprar.loc[(cliente_sem_comprar["Status do PDV"] == "Ativo            ")|(cliente_sem_comprar["Status do PDV"]== "Bloqueado        ")]
cliente_sem_comprar=cliente_sem_comprar.iloc[:,[124]]
cliente_sem_comprar.insert(loc=1,column="Cliente Novo",value="Sim",allow_duplicates= True)
Valor_minimo=pd.merge(Pedido_venda,Pedido_com_bonificação,on="Cod",how='left')
Valor_minimo=pd.merge(Valor_minimo,cliente_sem_comprar,on="Cod",how='left')
Valor_minimo=pd.merge(Valor_minimo,faturamentoT,on="Cod",how='left')
Valor_minimo=Valor_minimo.drop_duplicates()
#CLIENTES BLOQUEADOS
clientes_bloqueados=pd.read_excel(r"\\192.168.1.1\arquivos\COMPARTILHADOS\FINANCEIRO\CLIENTES BLOQUEADOS\CLIENTES BLOQUEADOS.xlsx",sheet_name=2)
clientes_bloqueados=clientes_bloqueados.iloc[:,[10,0,3,6]]
clientes_bloqueados.rename(columns={'ChaveCliente': 'Cod'}, inplace = True)
clientes_bloqueados.rename(columns={'CONCATENAR': 'OBSERVAÇÃO'}, inplace = True)
#GIRO

giro=giro.loc[(giro['AG'] == "Cerveja Litrao      ")|(giro['AG'] == "Cerveja 1/2         ") | (giro['AG'] == "Cerveja 1/1         ")]
giro=giro.loc[(giro['Tipo Giro Mensal'] == "Zero Total  ")|(giro['Tipo Giro Mensal'] == "Não OK      ") | (giro['Tipo Giro Mensal'] == "Zero        ")]
giro.rename(columns={'Item Comodato': 'Vasilhame'}, inplace = True)
giro=giro.loc[(giro['Vasilhame'] == 188006)|(giro['Vasilhame'] == 198214)|(giro['Vasilhame'] == 27983)|(giro['Vasilhame'] == 785196)|(giro['Vasilhame'] == 786238)]
giro.info()

giro2=giro.iloc[:,[20,2,3,8]]
giro2['Meta']=giro2['Caixas Comodatadas']*2
giro2['Vol. Real Cx Mensal']=giro2['Vol. Real Cx Mensal'].str.strip()
giro2['Vol. Real Cx Mensal']=giro2['Vol. Real Cx Mensal'].str.replace(".","")
giro2['Vol. Real Cx Mensal']=giro2['Vol. Real Cx Mensal'].astype(int)
giro2['GAP']=giro2['Meta']-giro2['Vol. Real Cx Mensal']
giro2=giro2.iloc[:,[0,1,5]]
giro2=giro2.drop_duplicates()
giro=giro.iloc[:,[20,2,3]]

produto.rename(columns={'Código': 'Cod. Produto'}, inplace = True)
produto['Vasilhame'] = produto['Vasilhame'].astype(str)
produto['Vasilhame'] = produto['Vasilhame'].str.replace("188006","Litrão")
produto['Vasilhame'] = produto['Vasilhame'].str.replace("198214","Litrinho")
produto['Vasilhame'] = produto['Vasilhame'].str.replace("27983","Litro")
produto['Vasilhame'] = produto['Vasilhame'].str.replace("785196","Litro")
produto['Vasilhame'] = produto['Vasilhame'].str.replace("786238","Litro")
produto=produto.iloc[:,[0,1,8]]
giro_comprar=pedido_giro.iloc[:,[38]]
pedido_giro=pedido_giro.iloc[:,[38,26,27,28,]]
comprou_no_dia=pd.merge(giro,giro_comprar,how='inner',on='Cod')
giro.to_excel('giro.xlsx')
giro_do_dia=pd.merge(pedido_giro,produto,how='left')
giro_do_dia=giro_do_dia.groupby(['Cod',"Vasilhame"],as_index=False).agg({'Qtde':sum}, group_keys =False)
vasilhame_litrinhos = giro_do_dia.loc[(giro_do_dia['Vasilhame'] == "Litrinho")]
vasilhame_litrao = giro_do_dia.loc[(giro_do_dia['Vasilhame'] == "Litrão")]
vasilhame_litro = giro_do_dia.loc[(giro_do_dia['Vasilhame'] == "Litro")]
cliente_litra=comprou_no_dia.loc[(comprou_no_dia['AG'] == "Cerveja Litrao      ")]
cliente_litrinho=comprou_no_dia.loc[(comprou_no_dia['AG'] == "Cerveja 1/2         ")]
cliente_litro=comprou_no_dia.loc[(comprou_no_dia['AG'] == "Cerveja 1/1         ")]
cliente_litra=pd.merge(cliente_litra,vasilhame_litrao,how='left',on='Cod')
cliente_litrinho=pd.merge(cliente_litrinho,vasilhame_litrinhos,how='left',on='Cod')
cliente_litro=pd.merge(cliente_litro,vasilhame_litro,how='left',on='Cod')
giro_do_dia=pd.concat([cliente_litra,cliente_litrinho])
giro_do_dia=pd.concat([giro_do_dia,cliente_litro])
cpf=cadastro[cadastro["Dia de Visita do VDE"] != "                            "]
cpf=cadastro[cadastro["Dia de Visita do VDI"] != "                            "]
cpf=cpf.iloc[:,[124,50,59]]
dias_iguai=cpf["Dia de Visita do VDI"] == cpf["Dia de Visita do VDE"]
cpf['Dias_Iguais']=np.where(dias_iguai,1,2)
cpf['Dia de Visita do VDE'] = cpf['Dia de Visita do VDE'].str.replace("DOM/",'0')
cpf['Dia de Visita do VDI'] = cpf['Dia de Visita do VDI'].str.replace("DOM/",'0')
cpf['Dia de Visita do VDE'] = cpf['Dia de Visita do VDE'].str.replace("SAB/",'0')
cpf['Dia de Visita do VDI'] = cpf['Dia de Visita do VDI'].str.replace("SAB/",'0')
semana=['QUI/','TER/','SEG/','QUA/','SEX/']
for dia in semana:
    cpf['Dia de Visita do VDE'] = cpf['Dia de Visita do VDE'].str.replace(dia, '1')
    cpf['Dia de Visita do VDI'] = cpf['Dia de Visita do VDI'].str.replace(dia, '1')
cpf['Dia de Visita do VDE'] = cpf['Dia de Visita do VDE'].str.replace('110', '2')
cpf['Dia de Visita do VDI'] = cpf['Dia de Visita do VDI'].str.replace('110', '2')
cpf['Dia de Visita do VDE'] = cpf['Dia de Visita do VDE'].str.replace('111', '3')
cpf['Dia de Visita do VDI'] = cpf['Dia de Visita do VDI'].str.replace('111', '3')
cpf['Dia de Visita do VDE'] = cpf['Dia de Visita do VDE'].str.replace('11', '2')
cpf['Dia de Visita do VDI'] = cpf['Dia de Visita do VDI'].str.replace('11', '2')
cpf['Dia de Visita do VDI'] = cpf['Dia de Visita do VDI'].str.replace('10', '1')
cpf['Dia de Visita do VDE'] = cpf['Dia de Visita do VDE'].str.replace('10', '1')
cpf['Dia de Visita do VDE'] = cpf['Dia de Visita do VDE'].replace(r'^\s*$', '0', regex=True)
cpf['Dia de Visita do VDI'] = cpf['Dia de Visita do VDI'].replace(r'^\s*$', '0', regex=True)
cpf['Dia de Visita do VDE'] = cpf['Dia de Visita do VDE'].astype(int)
cpf['Dia de Visita do VDI'] = cpf['Dia de Visita do VDI'].astype(int)
visita1=cpf[cpf["Dias_Iguais"]==1]
visita1['Visitas']=visita1['Dias_Iguais']
visita2=cpf[cpf["Dias_Iguais"]==2]
visita2['Visitas']= visita2[['Dia de Visita do VDE','Dia de Visita do VDI']].sum(axis=1)
visita=pd.concat([visita1,visita2])
visita["Visitas"]=visita["Visitas"]*4
visita=visita.iloc[:,[0,4]]
giro_do_dia=pd.merge(giro_do_dia,visita,how='left',on='Cod')
giro_do_dia['Caixas Comodatadas']=giro_do_dia['Caixas Comodatadas']*2
giro_do_dia['Meta']=np.divide(giro_do_dia['Caixas Comodatadas'],giro_do_dia['Visitas'])
giro_do_dia['Meta'] = giro_do_dia['Meta']
retorno= giro_do_dia["Qtde"] >= giro_do_dia["Meta"]
giro_do_dia['Retorno']= np.where(retorno,"Entregou","Não Entregou")
giro_do_dia.rename(columns={'Caixas Comodatadas': 'Meta_Giro'}, inplace = True)
giro_do_dia=giro_do_dia.iloc[:,[0,1,4]]
#giro2.to_excel('giro.xlsx')
giro_dados=cadastro.iloc[:,[124,5,46,48]]
giro_do_dia=pd.merge(giro_do_dia,giro2,on=['Cod','AG'])
giro_do_dia=pd.merge(giro_do_dia,giro_dados,on=['Cod'])
giro_do_dia.info()
giro_do_dia=giro_do_dia.iloc[:,[0,4,6,5,1,2,3]]
giro_do_dia=giro_do_dia.drop_duplicates()

#LIMITE CPF
faturamentoP['Total'] = faturamentoP['Total'].str.replace(".","")
faturamentoP['Total'] = faturamentoP['Total'].str.replace(",",".")
faturamentoP['Total'] = faturamentoP['Total'].astype(float)
faturamentoP=faturamentoP.groupby(['Cod',"Emissao   "],as_index=False).agg({'Total':sum}, group_keys =False)
faturamento_mediana=faturamentoP.groupby(['Cod'],as_index=False).median('Total')
faturamento_mediana['Total'] = faturamento_mediana['Total'].astype(int)
faturamento_mediana.rename(columns={'Total': 'Media por Pedido'}, inplace = True)

cpf=cadastro.iloc[:,[124,5,44,46]]
cpf=cpf[cpf["Tipo de Pessoa"]=="Fisica  "]
pedido_limite=pedido.groupby(['Cod'],as_index=False).agg({'Valor Pedido':sum}, group_keys =False)
faturamento=faturamento
faturado=pd.merge(cpf,faturamento,how='left')
pedido_limite=pd.merge(cpf,pedido_limite,how='left')
limite=pd.merge(pedido_limite,faturado,how='left')
limite['Total_Cliente']=limite['Faturamento Total']+limite['Valor Pedido']

#ESTOURO

pedidos_estouros=pedidos_estouros.groupby(['Cod',"Forma Pgto"],as_index=False).agg({'Valor Pedido':sum}, group_keys =False)
pedidos_estouros_total=pedidos_estouros.groupby(['Cod'],as_index=False).agg({'Valor Pedido':sum}, group_keys =False)
pedidos_estouros_total.rename(columns={'Valor Pedido': 'Pedido do dia'}, inplace = True)
pedidos_boleto=pedidos_estouros[pedidos_estouros["Forma Pgto"] == 'BL ']
pedidos_dinheiro=pedidos_estouros[pedidos_estouros["Forma Pgto"] != 'BL ']
pedidos_dinheiro.rename(columns={'Valor Pedido': 'Dinheiro'}, inplace = True)
pedidos_dinheiro=pedidos_dinheiro.iloc[:,[0,2]]
pedidos_boleto.rename(columns={'Valor Pedido': 'Boleto'}, inplace = True)
pedidos_boleto=pedidos_boleto.iloc[:,[0,2]]
pedidos_estouros_total=pd.merge(pedidos_estouros_total,pedidos_dinheiro,how='left')
pedidos_estouros_total=pd.merge(pedidos_estouros_total,pedidos_boleto,how='left')
pedidos_estouros_total.fillna(0,inplace = True)
faturamento_medio=faturamentoP.groupby(['Cod'],as_index=False).mean('Total')
faturamento_medio.rename(columns={'Total': 'Pedido Medio'}, inplace = True)
faturamento_maximo=faturamentoP.groupby(['Cod'],as_index=False).max()
faturamento_maximo.rename(columns={'Total': 'Maior Pedido'}, inplace = True)
faturamento_mediana=faturamentoP.groupby(['Cod'],as_index=False).median('Total')
faturamento_mediana.rename(columns={'Total': 'Pedido Mediano'}, inplace = True)
faturamentoP=faturamentoP.groupby(['Cod'],as_index=False).agg({'Total':sum}, group_keys =False)
faturamentoP.rename(columns={'Total': 'Faturamento Total'}, inplace = True)
faturamentoP=pd.merge(faturamentoP,faturamento_medio,on='Cod')
faturamentoP=pd.merge(faturamentoP,faturamento_maximo,on='Cod')
faturamentoP=pd.merge(faturamentoP,faturamento_mediana,on='Cod')
faturamentoP=pd.merge(pedidos_estouros_total,faturamentoP,on='Cod')
faturamentoP['Estouro']=faturamentoP['Pedido do dia']-faturamentoP['Maior Pedido']
faturamentoP['Estouro Medio']=faturamentoP['Pedido do dia']-faturamentoP['Pedido Mediano']
faturamentoP=faturamentoP[faturamentoP["Estouro"] > 100]
cadastro_estouro=cadastro.iloc[:,[124,5,10,14,46,48]]
faturamentoP=pd.merge(cadastro_estouro,faturamentoP,on='Cod')
faturamentoP=faturamentoP.iloc[:,[0,1,2,4,5,12,13,6,7,8,14,15]]
faturamentoP.iloc[:,[5,6,7,8,9,10,11]] = faturamentoP.iloc[:,[5,6,7,8,9,10,11]].astype(int)
faturamentoP=faturamentoP.sort_values(by='Estouro Medio', ascending=False )
faturamentoP.rename(columns={'Setor VDE': 'RN'}, inplace = True)
faturamentoP.rename(columns={'GV VDE': 'GV'}, inplace = True)
lista_de_setor=faturamentoP['GV'].drop_duplicates()
lista_de_setor=lista_de_setor.sort_values(ascending=True)
"""for RN in lista_de_setor:
    a=RN
    faturamento_RN = faturamentoP[faturamentoP['GV'] == RN]
    faturamento_RN = faturamento_RN.iloc[0:50]
    dfi.export(faturamento_RN,"Estouro sala {}.png".format(RN))"""



#RGB
rgb=pd.read_csv('RGB_ONE_WAY.csv',encoding="ISO-8859-1",sep=";",decimal=',')
#pedido_cesta=pedido_cesta[pedido_cesta["Ocorrencia 1"] !='-->Falta']
#pedido_cesta=pedido_cesta[pedido_cesta["Ocorrencia 2"] !='-->Falta']
pedido_cesta=pd.merge(pedido_cesta,rgb,on='Cod. Produto')
pedido_cesta=pedido_cesta.drop_duplicates()
pedido_cesta['Total']=pedido_cesta['Preco Unit.']*pedido_cesta['Qtde']
valor_sem_falta=pedido_cesta
pedido_cesta=pedido_cesta.groupby(['Cod',"CESTA"],as_index=False).agg({'Total':sum}, group_keys =False)
pedido_cesta['Total'] = pedido_cesta['Total'].astype(int)
RGB = pedido_cesta[pedido_cesta["CESTA"]=="RGB"]
RGB=RGB.iloc[:,[0,2]]
RGB.rename(columns={'Total': 'RGB'}, inplace = True)
ONE_WAY = pedido_cesta[pedido_cesta["CESTA"]=="ONE WAY"]
ONE_WAY=ONE_WAY.iloc[:,[0,2]]
ONE_WAY.rename(columns={'Total': 'ONE_WAY'}, inplace = True)
MARKET_PLACE =pedido_cesta[pedido_cesta["CESTA"]=="MARKETPLACE"]
MARKET_PLACE=MARKET_PLACE.iloc[:,[0,2]]
MARKET_PLACE.rename(columns={'Total': 'MARKET_PLACE'}, inplace = True)
pedido_cesta=pd.merge(RGB,ONE_WAY,on='Cod',how='outer')
pedido_cesta=pd.merge(pedido_cesta,MARKET_PLACE,on='Cod',how='outer')
pedido_cesta.fillna(0,inplace = True)
#VALOR MINIMO
valor_sem_falta=valor_sem_falta.groupby(['Cod'],as_index=False).agg({'Total':sum}, group_keys =False)
#valor_sem_falta=valor_sem_falta.iloc[:,[38,47]]
Valor_minimo=pd.merge(Valor_minimo,valor_sem_falta,on="Cod",how='left')
#pedido_cesta=pd.merge(RGB,valor_sem_falta,on='Cod',how='outer')


#MAIORES PEDIDOS
pedido_alto=pedido_alto.groupby(['Cod','Nome Cliente',"Cod. Vendedor","Forma Pgto","Cod. Area"],as_index=False).agg({'Valor Pedido':sum}, group_keys =False)
pedido_alto.rename(columns={'Cod. Area': 'GV'}, inplace = True)
pedido_alto=pedido_alto.sort_values(by='Valor Pedido', ascending=False )
pedido_alto=pd.merge(pedido_alto,faturamento_mediana,how='left',on='Cod')
pedido_alto=pd.merge(pedido_alto,pedido_cesta,how='left',on='Cod')
lista_de_setor=pedido_alto['GV'].drop_duplicates()
lista_de_setor=lista_de_setor.sort_values(ascending=True)

"""for RN in lista_de_setor:
    a=RN
    pedido_alto_GV = pedido_alto[pedido_alto['GV'] == RN ]
    pedido_alto_GV = pedido_alto_GV.iloc[0:21]
    dfi.export(pedido_alto_GV,"{}.png".format(RN))"""""


cliente=pd.read_csv('cliente.csv',encoding="utf-8",sep=";",decimal=',')
pedidox=pedidox.groupby(['Cod','Cod. Vendedor',],as_index=False).agg({'Valor Pedido':sum}, group_keys =False)
cliente['Cod. Cliente'] = cliente['Cod. Cliente'].astype(str)
cliente=pd.concat([cliente,clientes_bloqueados])
cliente=pd.merge(cliente,pedidox,on='Cod',how='inner')
cliente=cliente.iloc[:,[2,4,7,6,0,5]]


with pd.ExcelWriter("critica.xlsx") as writer:
    giro_do_dia.to_excel(writer, 'Giro', index=False)
    Valor_minimo.to_excel(writer, 'Valor_Minimo', index=False)
    limite.to_excel(writer, 'limite', index=False)
    cliente.to_excel(writer, 'cliente', index=False)
    pedido_alto.to_excel(writer, 'pedido_alto', index=False)
    faturamentoP.to_excel(writer, 'estouros', index=False)
