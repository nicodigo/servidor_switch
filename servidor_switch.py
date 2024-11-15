import socket
import xml.etree.ElementTree as ET
import os
import fdb
import fdb.fbcore
import psycopg2
import struct

HOST = '192.168.0.139'
PUERTO = 8080
nodos = {}

def get_conexiones():
    ruta_absoluta_dir = os.path.dirname(os.path.abspath(__file__))
    archivo_conexiones = os.path.join(ruta_absoluta_dir, 'conexiones.xml')
    
    try:    
        arbol = ET.parse(archivo_conexiones)
        for nodo in arbol.findall('.//nodo'):
            nombre = nodo.find('database').text
            nodos[nombre] = {}
            for elemento in nodo:
                nodos[nombre][elemento.tag] = elemento.text        
    except Exception as e:
        print(f"Error: {e}")
    


def recibir_mensaje(socket_cli: socket.socket, address: socket.AddressInfo):
    
    print(f"Conexion establecida con {address}")
    size_datos = socket_cli.recv(4)
    if not size_datos:
        return None
    
    size = struct.unpack('!I', size_datos)[0]

    datos = b""
    while len(datos) < size:
        paquete = socket_cli.recv(size - len(datos))
        if not paquete:
            return None
        datos += paquete
    
    return datos.decode('utf-8')

def enviar_mensaje(mi_socket: socket.socket, mensaje: str):
    mensaje_bytes = mensaje.encode('utf-8')
    size = len(mensaje_bytes)

    size_bytes = struct.pack('!I', size)

    mi_socket.sendall(size_bytes)
    mi_socket.sendall(mensaje_bytes)
    return     

def enviar_mensaje_error(sCliente: socket.socket, mensaje: str):
    mensaje_xml = f"<error><mensaje>{mensaje}</mensaje></error>"
    enviar_mensaje(sCliente, mensaje_xml)


def start_servidor(host, port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((host, port))
        server_socket.listen()
        print(f"Servidor escuchando en {host}:{port}")
        
    return server_socket

def ejecutar_query_firebird(sCliente:socket.socket, query:str, nodo: dict):
    print("ejecutando")
    print(nodo['ip'] + ':' + nodo['ubicacionDB'])
    print(nodo['usuario'])
    print(nodo['password'])
    try:
        conexion = fdb.connect(
            dsn = nodo['ip'] + ':' + nodo['ubicacionDB'],
            user = nodo['usuario'],
            password = nodo['password'],
            charset = 'UTF8'
        )
    except Exception as e:
        enviar_mensaje_error(sCliente, "No se pudo conectarse a la base de datos, tal vez se encuentre caido el servidor")
        return()
    try:
        cursor = conexion.cursor()
        cursor.execute(query)
        filas = cursor.fetchall()
        columnas = [desc[0] for desc in cursor.description]

        cursor.close()
        conexion.close()
    except Exception as e:
        enviar_mensaje_error(sCliente, "Hubo un error al ejecutar la consulta, verifique la sintaxis y ejecute nuevamente")
        cursor.close()
        conexion.close()
        return()
    
    print("col", columnas)
    print("row", filas)
    return (columnas, filas)

def ejecutar_query_postgresql(sCliente:socket.socket, query:str, nodo: dict, nombre: str):
    print("ejecutando query en post")
    try:
        conexion = psycopg2.connect(
            host = nodo['ip'],
            user = nodo['usuario'],
            password = nodo['password'],
            database = nombre,
            port = '5432'
        )
        print("conexion establecida")
    except Exception as e:
        enviar_mensaje_error(sCliente, "No se pudo conectarse a la base de datos, tal vez se encuentre caido el servidor")
        return()
    
    try:
        cursor = conexion.cursor()
        cursor.execute(query)
        filas = cursor.fetchall()
        columnas = [desc[0] for desc in cursor.description]

        cursor.close()
        conexion.close()
    except Exception as e:
        enviar_mensaje_error(sCliente, "Hubo un error al ejecutar la consulta, verifique la sintaxis y ejecute nuevamente")
        cursor.close()
        conexion.close()
        return()
    
    print("col", columnas)
    print("filas", filas)
    return (columnas, filas)

def ejecutar_query(query: str, sCliente: socket.socket):
    print(query)

    raiz = ET.fromstring(query)
    nombre = raiz.find('database').text
    print(nombre)
    if nombre is None:
        return ()

    nodo = nodos[nombre]
    motor = str(nodo['motor'])
    sql = raiz.find('sql').text
    if motor.lower() == 'firebird':
        print("Firebird")
        return ejecutar_query_firebird(sCliente, sql, nodo)
    elif motor.lower() == 'postgresql':
        print("ejecutando en postgres")
        return ejecutar_query_postgresql(sCliente, sql, nodo, nombre)
    else:
        enviar_mensaje_error('El nombre no es una base de datos existente en este servidor')
        return ()

def respuesta_to_xmlStr(respuesta: tuple):
    colsRespuesta, filasRespuesta  = respuesta
    print("cols", colsRespuesta)
    print("rows", filasRespuesta)
    raiz = ET.Element("query")
    print("raiz", raiz)
    columnas = ET.SubElement(raiz, "columnas")
    i = 0
    for columna in colsRespuesta:
        colnameXml = ET.SubElement(columnas, f"colname{i}")
        colnameXml.text = str(columna)
        i += 1
    print("a")
    filas = ET.SubElement(raiz, "filas")
    i = 0
    for fila in filasRespuesta:
        filaXml = ET.SubElement(filas, f"fila{i}")
        i += 1
        j = 0
        for valorColumna in fila:
            colXml = ET.SubElement(filaXml, f"col{j}")
            colXml.text = str(valorColumna)
            j += 1
    xmlString = ET.tostring(raiz, encoding="unicode", method="xml")
    print("xmlString",xmlString)
    return xmlString


   

def nodos_to_xmlStr():
    raiz = ET.Element("dbs")
    for key in nodos.keys():
        database = ET.SubElement(raiz, "database")
        nombre = ET.SubElement(database, "nombre")
        motor = ET.SubElement(database, "motor")
        nombre.text = key
        motor.text = nodos[key]['motor']
    return ET.tostring(raiz, encoding='unicode', method='xml')

def gestionar_request(peticion):
    raiz = ET.fromstring(peticion)
    respuesta = ""
    if (raiz.text == 'databases'):
        respuesta = nodos_to_xmlStr()
    return respuesta 


def generar_respuesta(peticion: str, sCliente: socket.socket):
    raiz = ET.fromstring(peticion)
    mensaje = ""
    if (raiz.tag == 'query'):
        respuesta = ejecutar_query(peticion, sCliente)
        print("RTA: ", respuesta)
        mensaje = ''
        if respuesta != ():
            mensaje = respuesta_to_xmlStr(respuesta)
    elif (raiz.tag == 'request'):
        mensaje = gestionar_request(peticion)
    return mensaje


def main():
    get_conexiones()
    print(nodos)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as socket_servidor:
        socket_servidor.bind((HOST, PUERTO))
        socket_servidor.listen()
        print(f"Servidor escuchando en {HOST}:{PUERTO}")
        
        sock_cliente, address = socket_servidor.accept()
        while (1):
            peticion = recibir_mensaje(sock_cliente, address)
            if peticion == None:
                print(f"Cliente {address[0]}:{address[1]} desconectado")
                break;
            mensaje = generar_respuesta(peticion, sock_cliente)
            if mensaje != "":
                print("respuesta", mensaje)
                enviar_mensaje(sock_cliente, mensaje)


        
    return

if __name__ == "__main__":
    main()