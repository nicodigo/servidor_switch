import socket
import xml.etree.ElementTree as ET
import struct
from prettytable import PrettyTable
import sys

def enviar_mensaje(mi_socket: socket.socket, mensaje: str):
    mensaje_bytes = mensaje.encode('utf-8')
    size = len(mensaje_bytes)

    size_bytes = struct.pack('!I', size)

    mi_socket.sendall(size_bytes)
    mi_socket.sendall(mensaje_bytes)
    return       


def recibir_mensaje(socket_cli: socket.socket):
    
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

def print_respuesta_xml(respuesta: str):
    tabla = PrettyTable()
    raiz = ET.fromstring(respuesta)
    if (raiz.tag == 'query'):
        columnas = raiz.find("columnas")
        
        for columna in columnas:
            colnames = [columna.text for columna in columnas]
            tabla.field_names = colnames

        for fila in raiz.find("filas"):
            colValores = [colVal.text for colVal in fila]
            tabla.add_row(colValores)
        print(tabla)
    elif (raiz.tag == 'error'):
        print("Error: ", raiz.find("mensaje").text)
    else:
        print(respuesta)
    

def xml_to_databases_dict(xmlStr: str):
    raiz = ET.fromstring(xmlStr)
    dbs = raiz.findall(".//database")
    db_dict ={}
    i = 1
    for db in dbs:
        db_dict[str(i)] = {
            'nombre': db.find('nombre').text,
            'motor': db.find('motor').text
        }
        i += 1
    return db_dict

def get_int():
    num = None
    while (num == None):
        try:
            num = int(input())
        except ValueError:
            print("Error: ingrese un entero valido")
    return num

def mostrar_menu(dbs: dict):
    tabla = PrettyTable()
    tabla.field_names = ['Opc', 'Nombre DB', 'Motor DB']
    for key in dbs.keys():
        tabla.add_row([key, dbs[key]['nombre'], dbs[key]['motor']])
    print(tabla)
    print("0. Salir")
    print("Ingrese una opcion:")


def armar_mensaje(db: dict):
    print(f"Base de datos: {db['nombre']}")
    raiz = ET.Element("query")
    nombre = ET.SubElement(raiz, "database")
    nombre.text = db['nombre']
    print('Ingrese la consulta sql:')
    sql = ET.SubElement(raiz, 'sql')
    sql.text = input()
    xmlString = ET.tostring(raiz, encoding='unicode', method='xml')
    return xmlString

def main():
    if (len(sys.argv) != 3):
        print("Uso correcto: python nombre_del_script ip_del_servidor puerto_del_servidor")
        print("Ejemplo: python ./cliente.py 192.168.0.10 8080")
        return
    ip = sys.argv[1]
    try: 
        puerto = int(sys.argv[2])
    except ValueError:
        print("Error: el puerto debe ser un entero valido")
        return
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as socket_cliente:
        try:
            socket_cliente.connect((ip, puerto))
        except:
            print("Error al conectar")
            return
        
        databases = {}
        enviar_mensaje(socket_cliente, "<request>databases</request>")
        respuesta = recibir_mensaje(socket_cliente)
        print(respuesta)
        databases = xml_to_databases_dict(respuesta)
        print(databases)

        run = True
        while (run):
            opcion = 0
            mostrar_menu(databases)
            opcion = get_int()
            if opcion > 0:
                mensaje = armar_mensaje(databases[str(opcion)])
                enviar_mensaje(socket_cliente, mensaje)
                respuesta = recibir_mensaje(socket_cliente)
                print_respuesta_xml(respuesta)
                print('-'*32)
                print('-'*32)
            else:
                run = False
    return

if __name__ == "__main__":
    main()