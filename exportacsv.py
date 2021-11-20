#!/usr/bin/env python
import mysql.connector
from mysql.connector import errorcode
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

def funcao(tabela, query, cursor):
	if tabela == 'todas':
		#caso nao seja especificada nenhuma tabela, exportar todas
		#pegandos o nome das tabelas do banco de dados, colocando numa lista, e criando o nome do arquivo com elas
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
			caminho = os.path.join(cwd, nomeArquivo)
				
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
	try:
		conexao = mysql.connector.connect(host=endereco, user=usuario, password=senha, database=banco)
		print("MySQL Conectado")
	except mysql.connector.Error as error:
		if error.errno == errorcode.ER_BAD_DB_ERROR:
			print("Este banco de dados não existe")
		elif error.errno == errorcode.ER_ACCESS_DENIED_ERROR:
			print("Nome de usuário ou senha errada")
		else:
			print(error)

	cursor = conexao.cursor(dictionary=True)
	query = 'Show tables'
	funcao(tabela, query, cursor)

elif(tipoBanco == "postgresql"):
	print("Banco PostgreSQL")
	conexao = psycopg2.connect(host=endereco, user=usuario, password=senha, database=banco)
	cursor = conexao.cursor(cursor_factory=extras.DictCursor)
	query = "select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';"
	funcao(tabela, query, cursor)

else:
	print("Tipo do banco de dados não identificado")
	print("Escolha uma das opções: mysql ou postgresql")
