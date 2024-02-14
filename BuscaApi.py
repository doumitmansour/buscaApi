import requests
import json
#import pandas as pd
import pyodbc 
from datetime import datetime,date,timedelta
#import schedule
#import time
    
    ##Define as informações do banco de dados sql
server   = ''
database = ''
username = ''
password = ''
driver   = '{SQL Server Native Client 11.0}'



def insere_sql():
    contagem = 0 
    data_atual =  str(date.today() + timedelta(days=1))
    dia_anterior = str(date.today() - timedelta(days=28))
    
    api = requests.get('https://api.movidesk.com/public/v1/tickets?token=$select=category,serviceFirstLevel,serviceSecondLevel,serviceThirdLevel,status,createdDate,resolvedIn,closedIn,id&$expand=owner,statusHistories,ownerHistories($expand=owner),clients($expand=organization),customfieldvalues($filter=customfieldid%20eq%2026295;$select=customFieldId,customFieldRuleId;$expand=items($select=customFieldItem))&$filter=createdDate%20ge%20'+ dia_anterior +'T00:00:00.00z%20and%20createdDate%20le%20' + data_atual +'T00:00:00.00z')
    dados = json.loads(api.text)
    
    # insere os valores na lista realizando os tratamentos para campos com valores vazios  
    for dado in dados:
        statusHistories = dado['statusHistories']

        id = dado['id']

        if dado['status']:
            status = dado['status']
        else: 
            status = ''

        if dado['customFieldValues']:
            owner = dado['customFieldValues'] #['customFieldRuleId']
        else:
            owner = 'Inexistente'
            

        if dado['category']:
            category = dado['category']
        else:
            category = ''

        if dado['serviceFirstLevel']:
            servico = dado['serviceFirstLevel']
        else:
            servico = ''
        
        if dado['serviceSecondLevel']:
            servico_2 = dado['serviceSecondLevel']
        else:
            servico_2 = ''
        
        if dado['serviceThirdLevel']:
            servico_3 = dado['serviceThirdLevel']
        else:
            servico_3 = ''
    #Buscando e descompactando a lista para trazer o campo businessName dentro de organization dentro de cliente      
      
        clientes = dado['clients']
               
        for cliente in clientes:
            organizacao = cliente['organization']
   
        if len(str(organizacao)) <5:
            nome_cliente = 'Inexistente'
        else:
            nome_cliente = organizacao['businessName']                  
            
        if dado['ownerHistories']:
            ownerHistories = dado['ownerHistories']
            ownerHistories = str(ownerHistories).replace("'",'')
        else:
            ownerHistories = ''       
            
            
        for owners in owner:
            
            if len(str(owners)) <5:
                owner_finalizado = 'inexistente'
            else:
                owner_finalizado = owners['items'] 
         
        for owner_finalizados in owner_finalizado:
            if len(str(owner_finalizados)) <5:
                finalizado_por = 'inexistente'
            else:
                 finalizado_por = owner_finalizados['customFieldItem']  
         

 
        createdDate = dado['createdDate']
        resolvedIn = dado['resolvedIn']
        closedIn = dado['closedIn']
 
       
        somaAberto = 0
        somaEmAndamento = 0
        somaRespondido = 0
        somaResolvido = 0
        somaRespondidoN2 = 0
        somaEmAndamentoBI = 0
        somaEmAndamentoDev = 0
        somaEmAndamentoRussia = 0
        somaEngenharia = 0
        somaOperacao = 0
        somaPendenciaCliente = 0
        # horas uteis
        somaAberto_util = 0
        somaEmAndamento_util = 0
        somaRespondido_util = 0
        somaResolvido_util = 0
        somaRespondidoN2_util = 0
        somaEmAndamentoBI_util = 0
        somaEmAndamentoDev_util = 0
        somaEmAndamentoRussia_util = 0
        somaEngenharia_util = 0
        somaOperacao_util = 0
        somaPendenciaCliente_util = 0
        

        for statusHistorie in statusHistories:
            if statusHistorie['permanencyTimeFullTime'] == None:
                continue

            if statusHistorie['status'] == 'Em andamento':
                somaEmAndamento         += statusHistorie['permanencyTimeFullTime']
                somaEmAndamento_util    += statusHistorie['permanencyTimeWorkingTime']

            elif statusHistorie['status'] == 'Em andamento - B.I':
                somaEmAndamentoBI         += statusHistorie['permanencyTimeFullTime']
                somaEmAndamentoBI_util    += statusHistorie['permanencyTimeWorkingTime']

            elif statusHistorie['status']  == 'Em andamento - Dev':
                somaEmAndamentoDev         += statusHistorie['permanencyTimeFullTime']
                somaEmAndamentoDev_util    += statusHistorie['permanencyTimeWorkingTime']

            elif statusHistorie['status']  == 'Em andamento - Rússia':
                somaEmAndamentoRussia      += statusHistorie['permanencyTimeFullTime']
                somaEmAndamentoRussia_util += statusHistorie['permanencyTimeWorkingTime']

            elif statusHistorie['status'] == 'Em andamento - Engenharia':
                somaEngenharia            += statusHistorie['permanencyTimeFullTime']
                somaEngenharia_util       += statusHistorie['permanencyTimeWorkingTime']

            elif statusHistorie['status'] == 'Em andamento - Operação':
                somaOperacao              += statusHistorie['permanencyTimeFullTime']
                somaOperacao_util         += statusHistorie['permanencyTimeWorkingTime']
                

            elif statusHistorie['status'] == 'Respondido N2':
                somaRespondidoN2          += statusHistorie['permanencyTimeFullTime']
                somaRespondidoN2_util     += statusHistorie['permanencyTimeWorkingTime']
    
            elif statusHistorie['status'] == 'Aberto':
                somaAberto                += statusHistorie['permanencyTimeFullTime']
                somaAberto_util           += statusHistorie['permanencyTimeWorkingTime']

            elif statusHistorie['status'] == 'Respondido':
                somaRespondido            += statusHistorie['permanencyTimeFullTime']
                somaRespondido_util       += statusHistorie['permanencyTimeWorkingTime']

            elif statusHistorie['status'] == 'Resolvido':
                somaResolvido             += statusHistorie['permanencyTimeFullTime']
                somaResolvido_util        += statusHistorie['permanencyTimeWorkingTime']

            elif statusHistorie['status'] == 'Pendencia / Cliente':
                somaPendenciaCliente      += statusHistorie['permanencyTimeFullTime'] 
                somaPendenciaCliente_util += statusHistorie['permanencyTimeWorkingTime']     

        #Converter dataspi
        if createdDate:
            dtcri = datetime.strptime(createdDate[:23] ,'%Y-%m-%dT%H:%M:%S.%f')
            dataCriacao = dtcri.strftime("%Y-%m-%d %H:%M:%S")

        if resolvedIn:
            dtres = datetime.strptime(resolvedIn[:23] ,'%Y-%m-%dT%H:%M:%S.%f')
            dataResolucao = dtres.strftime("%Y-%m-%d %H:%M:%S")
        else:
            aux = dado['statusHistories'][-1]['changedDate']
            aux = datetime.strptime(aux[:23] ,'%Y-%m-%dT%H:%M:%S.%f')
            dataResolucao = aux.strftime("%Y-%m-%d %H:%M:%S")

        if closedIn:
            dtfec = datetime.strptime(closedIn[:23] ,'%Y-%m-%dT%H:%M:%S.%f')
            dataFechamento = dtfec.strftime("%Y-%m-%d %H:%M:%S")
        else:
            print('!')
            aux = dado['statusHistories'][-1]['changedDate']
            aux = datetime.strptime(aux[:23] ,'%Y-%m-%dT%H:%M:%S.%f')
            dataFechamento = aux.strftime("%Y-%m-%d %H:%M:%S")

    ## Conexão com o SQL SERVER 
        conec = pyodbc.connect('DRIVER='+driver+';SERVER=' + server+';DATABASE='+database+';UID='+username+';PWD=' + password)
        cursor = conec.cursor()

        cursor.execute("SELECT id FROM STAGE.DBO.INT_SLA_MOVIDESK WHERE id = " + str(id))
        data = cursor.fetchone()
        
        if data:
            # print("UPDATE STAGE.DBO.INT_SLA_MOVIDESK SET category = " + "'"+ category +"'"+ " WHERE id = " + str(id))
            cursor.execute(" BEGIN UPDATE STAGE.DBO.INT_SLA_MOVIDESK SET category  = " +"'"+ str(category) +"'"+ 
                                                                " , servico     = " +"'"+ str(servico) +"'"+
                                                                " , servico_2     = " +"'"+ str(servico_2) +"'"+
                                                                " , servico_3     = " +"'"+ str(servico_3) +"'"+   
                                                                " , status      = " +"'"+ str(status) +"'"+ 
                                                                " , responsavel = " +"'"+ str(finalizado_por) +"'"+
                                                                " , createdDate = " +"'"+ str(dataCriacao) +"'"+
                                                                " , resolvedIn  = " +"'"+ str(dataResolucao) +"'"+
                                                                " , closedIn    = " +"'"+ str(dataFechamento) +"'"+
                                                                " , nome_cliente       = " +"'"+ str(nome_cliente) +"'"+
                                                                " , Responsavel_1       = " +"'"+ str(ownerHistories) +"'"+
                                                                 
                                                                " , somaAberto            = " +"'"+ str(somaAberto) +"'"+
                                                                " , somaEmAndamento       = " +"'"+ str(somaEmAndamento) +"'"+
                                                                " , somaRespondido        = " +"'"+ str(somaRespondido) +"'"+
                                                                " , somaResolvido         = " +"'"+ str(somaResolvido) +"'"+
                                                                " , somaRespondidoN2      = " +"'"+ str(somaRespondidoN2) +"'"+
                                                                " , somaEmAndamentoBI     = " +"'"+ str(somaEmAndamentoBI) +"'"+
                                                                " , somaEmAndamentoDev    = " +"'"+ str(somaEmAndamentoDev) +"'"+
                                                                " , somaEmAndamentoRussia = " +"'"+ str(somaEmAndamentoRussia) +"'"+
                                                                " , somaEngenharia        = " +"'"+ str(somaEngenharia) +"'"+
                                                                " , somaOperacao          = " +"'"+ str(somaOperacao) +"'"+
                                                                " , somaPendenciaCliente  = " +"'"+ str(somaPendenciaCliente) +"'"+
                                                                
                                                                " , somaAberto_util            = " +"'"+ str(somaAberto_util) +"'"+
                                                                " , somaEmAndamento_util       = " +"'"+ str(somaEmAndamento_util) +"'"+
                                                                " , somaRespondido_util        = " +"'"+ str(somaRespondido_util) +"'"+
                                                                " , somaResolvido_util         = " +"'"+ str(somaResolvido_util) +"'"+
                                                                " , somaRespondidoN2_util      = " +"'"+ str(somaRespondidoN2_util) +"'"+
                                                                " , somaEmAndamentoBI_util     = " +"'"+ str(somaEmAndamentoBI_util) +"'"+
                                                                " , somaEmAndamentoDev_util    = " +"'"+ str(somaEmAndamentoDev_util) +"'"+
                                                                " , somaEmAndamentoRussia_util = " +"'"+ str(somaEmAndamentoRussia_util) +"'"+
                                                                " , somaEngenharia_util        = " +"'"+ str(somaEngenharia_util) +"'"+
                                                                " , somaOperacao_util          = " +"'"+ str(somaOperacao_util) +"'"+
                                                                " , somaPendenciaCliente_util  = " +"'"+ str(somaPendenciaCliente_util) +"'"+
                                                                
                                                            
                                                                " WHERE id = " + str(id) + " END")
            conec.commit()
        else:
            cursor.execute('BEGIN INSERT INTO STAGE.DBO.INT_SLA_MOVIDESK (id, category, servico, status, nome_cliente, createdDate, resolvedIn, closedIn, responsavel, somaAberto, somaEmAndamento, somaRespondido, somaResolvido, somaRespondidoN2, somaEmAndamentoBI, somaEmAndamentoDev, somaEmAndamentoRussia, somaEngenharia, somaOperacao, somaPendenciaCliente,somaAberto_util, somaEmAndamento_util, somaRespondido_util, somaResolvido_util, somaRespondidoN2_util, somaEmAndamentoBI_util, somaEmAndamentoDev_util, somaEmAndamentoRussia_util,somaEngenharia_util, somaOperacao_util, somaPendenciaCliente_util) VALUES ('+ str(id) + ",'"+ category + "','"+ servico + "','" + status + "','" + str(nome_cliente) + "','" + dataCriacao + "','"+ dataResolucao + "','"+ dataFechamento +"','"+ finalizado_por + "'," + str(somaAberto)+',' + str(somaEmAndamento) + ","+ str(somaRespondido) + ","+ str(somaResolvido) +","+ str(somaRespondidoN2) +","+ str(somaEmAndamentoBI) +","+ str(somaEmAndamentoDev) +","+ str(somaEmAndamentoRussia) +","+ str(somaEngenharia) + ","+ str(somaOperacao) + ","+ str(somaPendenciaCliente)+ "," + str(somaAberto_util)+',' + str(somaEmAndamento_util) + ","+ str(somaRespondido_util) + ","+ str(somaResolvido_util) +","+ str(somaRespondidoN2_util) +","+ str(somaEmAndamentoBI_util) +","+ str(somaEmAndamentoDev_util) +","+ str(somaEmAndamentoRussia_util) +","+ str(somaEngenharia_util) + ","+ str(somaOperacao_util) + ","+ str(somaPendenciaCliente_util)+ ") END")

            conec.commit()    

        cursor.close()
        
        
        contagem = contagem +1
        
        print(id,dataCriacao,contagem,status)
        
        

insere_sql()

def exec_sp1():

    print('--- RELATÓRIO HORÁRIO ---')
    print('--- PROCEDURE SENDO EXECUTADA ---')
    
## Conexão com o SQL SERVER 
    conec = pyodbc.connect('DRIVER='+driver+';SERVER=' + server+';DATABASE='+database+';UID='+username+';PWD=' + password)
    cursor = conec.cursor()

# Executar procedure sp_int_sla_movidesk_horario
    storedProc = 'execute etl.dbo.sp_int_sla_movidesk_horario'
    cursor.execute(storedProc)
    
    print('--- PROCEDURE EXECUTADA ---')
    conec.commit() 
    cursor.close()
    conec.close()

exec_sp1()

def exec_sp2():

    print('--- RELATÓRIO DIÁRIO ---')
    print('--- PROCEDURE SENDO EXECUTADA ---')
    
## Conexão com o SQL SERVER 
    conec = pyodbc.connect('DRIVER='+driver+';SERVER=' + server+';DATABASE='+database+';UID='+username+';PWD=' + password)
    cursor = conec.cursor()

# Executar procedure sp_int_sla_movidesk
    storedProc2 = 'execute etl.dbo.sp_int_sla_movidesk'
    cursor.execute(storedProc2)
    
    print('--- PROCEDURE EXECUTADA ---')
    conec.commit() 
    cursor.close()
    conec.close()

exec_sp2()
