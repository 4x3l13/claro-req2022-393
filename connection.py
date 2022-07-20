# -*- coding: utf-8 -*-
"""
Created on Fri Jun 17 08:30:00 2022

@author: Jhonatan Martínez
"""

import inspect
import cx_Oracle
import pyodbc
import mysql.connector
from answer import Answer
import main_functions as mf


class Connection:
    """
    Permite la conexión y la obtención de datos desde una Base de Datos (SQL, MYSQL, ORACLE).
    Los datos de conexión están en el archivo JSON llamado **config.json**.\n
    Para su funcionamiento importa los siguientes paquetes y clases:\n
    * **inspect:** Para obtener información de la función actual.\n
    * **cx_Oracle:** Para conectar a una Base de Datos ORACLE.\n
    * **pyodbc:** Para conectar a una Base de Datos SQL.\n
    * **mysql.connector:** Para conectar a una Base de Datos MYSQL.\n
    * **Answer:** Dónde se envían el estado, mensaje y datos desde una función.\n
    * **main_functions:** Funciones principales para el funcionamiento del aplicativo.

    """

    def __init__(self):
        """
        Los datos están en la sección CONNECTION en el archivo JSON los cuales son:\n
        * **DB_TYPE:** Tipo de base de datos, se permiten a (SQL, MYSQL, ORACLE).\n
        * **DB_DRIVER:** Driver de conexión de ser necesario.\n
        * **DB_SERVER:** IP o nombre del servidor que aloja la Base de Datos.\n
        * **DB_NAME:** Nombre de la Base de Datos a la que se va a conectar.\n
        * **DB_USER:** Usuario para conectar de la Base de Datos.\n
        * **DB_PASSWORD:** Clave para conectar de la Base de Datos.\n
        """

        self.__this = self.__class__.__name__ + '.'
        self.__connection = None
        self.__connection_data = {}
        self._read_setup()

    def _close_connection(self):
        """
        Cierra la conexión a la Base de Datos.

        """

        try:
            # Close connection.
            self.__connection.close()
        except Exception as exc:
            # Show the error in console
            print(self.__this + inspect.stack()[0][3] + ': ' + str(exc))

    def _get_connection(self):
        """
        Realiza y establece la conexión a la Base de Datos.\n
        Los datos son leídos desde la función '_read_setup()',
        Estos datos se obtienen y agregan en el constructor de la clase.

        Returns:
            **Answer (Class):** Devuelve un estado y mensaje de la función.

        """

        # Invoke class Answer.
        answer = Answer()
        try:
            # Choose the type of connection.
            match self.__connection_data["type"]:
                case 'SQL':
                    connection_string = 'DRIVER={' + self.__connection_data["driver"] + '};'
                    connection_string += 'SERVER=' + self.__connection_data["server"] + ';'
                    connection_string += 'DATABASE=' + self.__connection_data["database"] + ';'
                    connection_string += 'UID=' + self.__connection_data["user"] + ';'
                    connection_string += 'PWD=' + self.__connection_data["password"]
                    self.__connection = pyodbc.connect(connection_string)
                case 'MYSQL':
                    self.__connection = mysql.connector.connect(
                        host=self.__connection_data["server"],
                        user=self.__connection_data["user"],
                        passwd=self.__connection_data["password"],
                        db=self.__connection_data["database"])
                case 'ORACLE':
                    dsn = self.__connection_data["server"] + '/' + self.__connection_data["database"]
                    self.__connection = cx_Oracle.connect(user=self.__connection_data["user"],
                                                          password=self.__connection_data["password"],
                                                          dsn=dsn,
                                                          encoding="UTF-8")
            # Fill answer object with status and message.
            answer.load(status=True,
                        message='Connection established with ' + self.__connection_data["type"])
        except (ConnectionError, Exception) as exc:
            # Fill variable error
            error_message = self.__this + inspect.stack()[0][3] + ': ' + str(exc)
            # Show error message in console
            print(error_message)
            # Fill answer object with status and error message.
            answer.load(status=False,
                        message=error_message)
        # Return answer object.
        return answer

    def read_data(self, query, datatype="dict"):
        """
        Permite ejecutar una consulta en la base de datos y devuelve los datos encontrados.

        Params:
            * **query (String):** Recibe un select o un procedimiento almacenado. \n
            * **datatype (String, optional):** Identifica que tipo de dato a retornar. \n
            \t* **dict:** diccionario, por defecto. \n
            \t* **list:** lista con las columnas y otra con los valores.

        Returns:
            **answer (Class):** Devuelve estado, mensaje y datos (diccionario, lista) de la función.

        """

        # Invoke class Answer.
        answer = Answer()
        try:
            # Open connection
            cnx = self._get_connection()
            if cnx.get_status():
                # Create cursor object.
                cursor = self.__connection.cursor()
                cursor.prefetchrows = 100000
                cursor.arraysize = 100000

                # Execute query
                cursor.execute(query)
                # Gets data
                data = cursor.fetchall()
                # Gets column_names
                columns = [column[0].upper() for column in cursor.description]
                # Validate the datatype to return
                if datatype == 'dict':
                    dictionary = []
                    for item in data:
                        dictionary.append(dict(zip(columns, item)))
                    # Fill answer object with status, message and data list.
                    answer.load(status=True,
                                message='Data obtained',
                                data=dictionary)
                elif datatype == 'list':
                    # Fill answer object with status, message and data list.
                    answer.load(status=True,
                                message='Data obtained',
                                data=[columns, data])
                # Close cursor
                cursor.close()
            else:
                answer.load(status=cnx.get_status(),
                            message=cnx.get_message())
        except (ConnectionError, cx_Oracle.DatabaseError, cx_Oracle.InterfaceError, Exception) as exc:
            # Fill variable error
            error_message = self.__this + inspect.stack()[0][3] + ': ' + str(exc)
            # Show error message in console
            print(error_message)
            # Fill answer object with status and error message.
            answer.load(status=False,
                        message=error_message)
        finally:
            # Close connection
            self._close_connection()
        # Return answer object.
        return answer

    def _read_setup(self):
        """
        Llama a 'read_setup()' desde 'main_functions', para leer y obtener los datos desde el JSON.

        Returns:
            **answer (Class):** Devuelve un estado y mensaje de la función.

        """

        # Invoke class Answer.
        answer = Answer()
        try:
            # Read CONNECTION configuration from JSON file.
            config = mf.read_setup(item='CONNECTION')
            if config.get_status():
                self.__connection_data["type"] = config.get_data()["DB_TYPE"]
                self.__connection_data["driver"] = config.get_data()["DB_DRIVER"]
                self.__connection_data["server"] = config.get_data()["DB_SERVER"]
                self.__connection_data["database"] = config.get_data()["DB_NAME"]
                self.__connection_data["user"] = config.get_data()["DB_USER"]
                self.__connection_data["password"] = config.get_data()["DB_PASSWORD"]
                if self.__connection_data["type"] == "ORACLE":
                    libraries = mf.get_current_path().get_data() + self.__connection_data["driver"]
                    cx_Oracle.init_oracle_client(lib_dir=libraries)
            # Fill answer object with status, message and data.
            answer.load(status=True,
                        message='Configuration obtained from CONNECTION')
        except Exception as exc:
            # Fill variable error
            error_message = self.__this + inspect.stack()[0][3] + ': ' + str(exc)
            # Show error message in console
            print(error_message)
            # Fill answer object with status and error message.
            answer.load(status=False,
                        message=error_message)
        # Return answer object.
        return answer

    def execute_query(self, query):
        """
        Permite ejecutar una consulta en la base de datos.

        Params:
            * **query (String):** Recibe un insert, update, delete o un procedimiento almacenado. \n

        Returns:
            **answer (Class):** Devuelve estado y mensaje de la función.

        """

        # Invoke class Answer.
        answer = Answer()
        try:
            # Open connection
            self._get_connection()
            # Create cursor object.
            cursor = self.__connection.cursor()
            # Execute the query.
            cursor.execute(query)
            # Confirm action
            self.__connection.commit()
            # Fill answer object with status, message and data list.
            answer.load(status=True,
                        message='Query executed')
            self._close_connection()
        except (ConnectionError, Exception) as exc:
            # Fill variable error
            error_message = self.__this + inspect.stack()[0][3] + ': ' + str(exc)
            # Show error message in console
            print(error_message)
            # Fill answer object with status and error message.
            answer.load(status=False,
                        message=error_message)
        # Return answer object.
        return answer
