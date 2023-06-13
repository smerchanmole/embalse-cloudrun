#!/usr/bin/env python3
# ####################################################################### #
# APP: Web scrapper to fetch reservoir water capacity                     #
# Reservoir name: Embalse de Santillana                                   #
# Year:2020                                                               #
#                                                                         #
# ####################################################################### #

# import pandas to manage data, request & re  to de the scrapping, json to 
# manage the jsons and sys to load a file with the passwords, psycopg2 to 
# load data to postgress database
import pandas as pd
import requests
import json
import time
import datetime
import re #regular expressions
import psycopg2 #to install with pip3 install psycopg2-binary
import sys #to access parent directory
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import os
from flask import Flask
from markupsafe import escape


# ## Funciones para el acceso a base de datos

# ####################################################################### #
# Data Base functions to avoid complex main program reading.              #
# ####################################################################### #


# ####################################################################### #
# FUNTION: conectar_db                                                    #
# DESCRIPTION: Generate a connection to the database (postgreSQL)         #
# INPUT: Data needed to connect and the inital connection query           #
# OUTPUT: Cursor and Connection,  print error if happens                  #
# ####################################################################### #
def conectar_bd (PS_HOST, PS_PORT, PS_USER, PS_PASS, PS_DB, PS_QUERY):
    try:
        connstr = "host=%s port=%s user=%s password=%s dbname=%s" % (PS_HOST, PS_PORT, PS_USER, PS_PASS, PS_DB)
        conn = psycopg2.connect(connstr)

        # Open the cursor and launch initial query
        cur = conn.cursor()
        
        # Query execution
        cur.execute(PS_QUERY)
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        if conn is not None:
            conn.close()
            print('Database connection closed.')   
            cur=""
            conn=""

    return cur, conn

# ####################################################################### #
# FUNTION: cerrar_conexion_bbdd                                           #
# DESCRIPTION: Close the connection                                       #
# INPUT: Data needed to close                                             #
# OUTPUT: Nothing                                                         #
# ####################################################################### #
def cerrar_conexion_bbdd (PS_CURSOR, PS_CONN):
    PS_CURSOR.close()
    PS_CONN.close()

# ####################################################################### #
# FUNTION: escribir_log                                                   #
# DESCRIPTION: Write the operation in a log table (just for info)         #
# INPUT: Data needed to write the log                                     #
# OUTPUT: 1                                                               #
# ####################################################################### #
def escribir_log (PS_CURSOR, PS_CONN, ip, comando, extra):
    # Escribimos el mensaje en la tabla logs. 
    x=datetime.datetime.now()
    # x.isoformat() para tener el timestamp formato ISO
    InsertLOG="INSERT INTO public.logs (hora, ip, comando, extra) values ('"+str(x.isoformat())+"','"+ip+"','"+comando+"','"+extra+"')"
    # print (InsertLOG)

    PS_CURSOR.execute(InsertLOG)
    return 1


# ####################################################################### #
# ####################################################################### #
# ####################################################################### #
# MAIN                                                                    #
# ####################################################################### #
# ####################################################################### #
# ####################################################################### #



## FLASK CODE
app = Flask(__name__)
@app.route("/")
def ETLembalse():
    database_ip="YOUR IP"
    database_port=5432 #your database port
    database_db="YOUR_DB"
    database_user="YOUR_USER"
    database_password="YOUR_PASS"

    EMAIL_SERVER="YOUR_EMAIL_SERVER"
    EMAIL_ALERT="YOUR_EMAIL"

    http_body='<p style = "font-family:perpetua;"><font color="red">EXTRACCION:</font><br> '
    mail_body="Hola Santiago:\nLa salida de la ETL es:\n "

    uri_total = "https://www.embalses.net/pantano-1013-santillana.html"
    print("-ACCESS TO URL:")
    print("   *** We use this URL: " + uri_total)
    http_body += '<br><font color="green">La URL de la que cogemos los datos:</font>'
    http_body += '<font color="blue">'+uri_total+'</font>'
    mail_body += "SOBRE EXTRACCION: \n   La URL de la que cogemos los datos:"
    mail_body += uri_total

    # Make the request and save the html response.
    response = requests.get(uri_total, verify=False)
    http_body += '<br><font color="green">La respuesta del servidor web es:</font><font color="blue"> '+ str(response.status_code) + '</font>'
    http_body+="</p>"
    mail_body += "\n   La respuesta del servidor web es:" + str(response.status_code)

    # Search the part of the page where the data is printed
    indice = response.text.find('Campo"><strong>Agua embalsada')
    http_body += '<p style = "font-family:perpetua;"><br><font color="red">TRANSFORMACION</font><br><font color="green">El Texto "Agua Embalsada" esta en el caracter:</font><font color="blue"> '+str(indice) + '</font>'
    mail_body += "\nSOBRE TRANSFORMACION:\n   El Texto 'Agua Embalsada' esta en el caracter: "+str(indice)

    # let's fetch 300 chars to look deeper
    crawler = response.text[indice:indice + 300]
    # print ("# ################# #")
    # print ("The 300 chars after the index are:")
    # print ("# ################# #")
    # print (crawler)
    # print ("# ################# #")

    # Now we take the date from the html code
    print("-FETCHING DATA FROM HTML:")
    indice_fecha = crawler.find("strong")
    print("   *** date index:", indice_fecha)
    fecha = crawler[indice_fecha + 23: indice_fecha + 23 + 10]
    print("   *** Date fetched:", fecha)

    # format date in postgreSQL and forget the time
    anio = fecha[6:10]
    mes = fecha[3:5]
    dia = fecha[0:2]
    print("   *** Date we understand:")
    print("   *** dia", dia, "mes", mes, "anio", anio)
    fecha = anio + "/" + mes + "/" + dia
    print("   *** Date Formated:", fecha)
    http_body += '<br><font color="green">Hemos cogido la fecha:  </font><font color="blue">' + fecha + '</font>'
    mail_body += "\n   Hemos cogido la fecha: " + fecha

    # Fetch the volume
    # 1st remove the date
    volumen = crawler[indice_fecha + 23 + 10 + 10:indice_fecha + 23 + 10 + 100]
    # print (volumen)
    indice_volumen = volumen.find("strong")
    volumen = volumen[indice_volumen + 7:indice_volumen + 7 + 5]
    print("   *** Text with the Volume to fetch:", volumen)

    # Fetch the numbers
    lista_numeros_cogidos = [float(s) for s in re.findall(r'-?\d+\.?\d*', volumen)]

    volumen = lista_numeros_cogidos[0]
    volumen = str(volumen)
    print("   *** We fetch the volume:", volumen)

    http_body += '<br><font color="green">Hemos cogido el volumen:</font><font color="blue">' + volumen + '</font>'
    http_body+="</p>"
    mail_body += "\n   Hemos cogido el volumen:" + volumen

    print("-WRITING IN DDBB:")
    # Write date and volume in postgreSQL database
    SQLupsert = "insert into public.embalse (fecha, volumen) "
    SQLupsert += "values ('" + fecha + "'," + volumen + ") on conflict(fecha) do nothing"

    # Now we generate the connection
    cur, con = conectar_bd(database_ip, database_port, database_user, database_password, database_db, "select 1")
    print("   *** SQL to write:", SQLupsert)

    # Execute the upsert
    cur.execute(SQLupsert)

    # Update the logs (you can comment this line if there is no log table)
    escribir_log(cur, con, "xxxxxx", "ACTUALIZAMOS embalse", "operacion normal")

    # Commit the data on database (just to make sure)
    con.commit()

    # Close the connection
    cerrar_conexion_bbdd(cur, con)

    http_body += '<p style = "font-family:perpetua"><font color="red">CARGA:</font><br><font color="green">Hemos actualizado la tabla embalse y la tabla logs.</font></p>'
    mail_body += "\nSOBRE CARGA EN BBDD:\n   Hemos actualizado la tabla embalse y la tabla logs. \n\nTodo Correcto \n\nUn saludo:\nJarbis"

    # Le mandamos un correo a quien esté configurado si nos pasamos en algun limite

    # create message object instance
    print("-SENDING EMAIL:")
    msg = MIMEMultipart()

    message = mail_body

    # setup the parameters of the message
    password = "your_password"
    msg['From'] = "EMAIL@yourdomain.com"
    msg['To'] = EMAIL_ALERT
    msg['Subject'] = "ESTADO_EMBALSE OK vol:" + volumen

    # add in the message body
    msg.attach(MIMEText(message, 'plain'))

    # create server
    server = smtplib.SMTP(EMAIL_SERVER + ': 2525')

    # server.starttls()

    # Login Credentials for sending the mail
    # server.login(msg['From'], password)

    # send the message via the server.
    server.sendmail(msg['From'], msg['To'], msg.as_string())
    server.quit()

    print("   *** successfully sent email to %s:" % (msg['To']))

    http_body += '<p style = "font-family:perpetua;"><font color="red">EMAIL:</font><br><font color="green">Hemos enviado correo sobre resultado.</font>.<br><i>Todo Correcto</i></p>'

    return f"{http_body}"


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
#Here is the URI where the data of the reservoir is noted.









