#!/usr/bin/env python
import mysql.connector
import os
import sys
import csv
import psycopg2
from psycopg2 import extras

tipoBanco = sys.argv[1]
endereco = sys.argv[2]
usuario = sys.argv[3]
senha = sys.argv[4] 
banco = sys.argv[5]

try:
	tabela = sys.argv[6]
except IndexError:
	tabela = 'todas'

#Criei uma função, para não ter que repetir blocos de código cada tipo de banco de dados
def exportaTabela(tabela, query, cursor):
	if tabela == 'todas':
		#caso nao seja especificada nenhuma tabela, exportar todas
		#pegamos o nome das tabelas do banco de dados, colocando elas numa lista 
		cursor.execute(query)
		dados = cursor.fetchall()	
		listaTabelas = []
		for info in dados:
			for valor in info.values():
				listaTabelas.append(valor)

	else:
		listaTabelas = [tabela]

	for tabela in listaTabelas:	
		nomeArquivo = banco + '-' + tabela + ".csv"

		with open(nomeArquivo, 'w', newline='') as arquivo:
			cwd = os.getcwd() 
			os.path.join(cwd, nomeArquivo)
	
			query = 'Select * FROM ' + tabela
			cursor.execute(query)
			dados = cursor.fetchall()
					
			fieldnames=[]
			for chave in dados[0].keys():
				fieldnames.append(chave)

			writer = csv.writer(arquivo)
			writer.writerow(fieldnames)

			for info in dados:
				writer.writerow(info.values())
	
			arquivo.close()
	conexao.close()

if(tipoBanco == "mysql"): 
	conexao = mysql.connector.connect(host=endereco, user=usuario, password=senha, database=banco)
	cursor = conexao.cursor(dictionary=True)
	query = 'Show tables'
	exportaTabela(tabela, query, cursor)

elif(tipoBanco == "postgresql"):
	conexao = psycopg2.connect(host=endereco, user=usuario, password=senha, database=banco)
	cursor = conexao.cursor(cursor_factory=extras.DictCursor)
	query = "select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';"
	exportaTabela(tabela, query, cursor)

else:
	print("Tipo do banco de dados não identificado")
	print("Escolha uma das opções: mysql ou postgresql")
