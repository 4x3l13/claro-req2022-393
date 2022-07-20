# -*- coding: utf-8 -*-
"""
Created on Mon Jul 11 17:00:00 2022

@author: Jhonatan Martínez
"""

import inspect
from ftplib import FTP
import pysftp
from answer import Answer
import main_functions as mf


class ConnectionFTP:
    """
    Permite la conexión y envío de email por SMTP,
    los datos de configuración están en el archivo JSON llamado **config.json**.\n
    Para su funcionamiento importa los siguientes paquetes y clases:\n
    * **inspect:** Para obtener información de la función actual.\n
    * **ftplib:** Para conectar al servidor FTP.\n
    * **pysftp:** Para conectar al servidor SFTP.\n
    * **Answer:** Dónde se envían el estado, mensaje y datos desde una función.\n
    * **main_functions:** Funciones principales para el funcionamiento del aplicativo.

    """

    def __init__(self):
        """
        Los datos están en la sección FTP en el archivo JSON los cuales son:\n
        * **FTP_TYPE:** Tipo de servidor, se permiten (FTP, SFTP).\n
        * **FTP_SERVER:** Host del servidor.\n
        * **FTP_PORT:** Puerto al que nos vamos a conectar.\n
        * **FTP_USER:** Usuario para conectar al servidor.\n
        * **FTP_PASSWORD:**  Clave para conectar al servidor.\n
        * **FTP_PATH:** Directorio en el que se va a trabajar.\n
        """
        self.__this = self.__class__.__name__ + '.'
        self.__connection = None
        self.__connection_data = {}
        self._read_setup()

    def change_path(self):
        """
        Cambia el directorio en el que se va a trabajar en el servidor FTP.

        Returns:
            **Answer (Class):** Devuelve un estado y mensaje de la función.

        """

        # Invoke class Answer.
        answer = Answer()
        try:
            # Changes path
            self.__connection.cwd(self.__connection_data["path"])
            # Fill answer object with status and message.
            answer.load(True, 'Changed path', None)
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

    def close_connection(self):
        """
        Cierra la conexión a la Base de Datos.

        """

        try:
            # Close connection.
            self.__connection.close()
        except Exception as exc:
            # Show the error in console
            print(self.__this + inspect.stack()[0][3] + ': ' + str(exc))

    def get_connection(self):
        """
        Realiza y establece la conexión al servidor FTP o SFTP.\n
        Los datos son leídos desde la función '_read_setup()',
        Estos datos se obtienen y agregan en el constructor de la clase.

        Returns:
            **Answer (Class):** Devuelve un estado y mensaje de la función.

        """

        # Invoke class Answer.
        answer = Answer()
        try:
            # Choose the type of server.
            if str(self.__connection_data["type"]).upper() == "FTP":
                self.__connection = FTP(host=self.__connection_data["server"],
                                        user=self.__connection_data["user"],
                                        passwd=self.__connection_data["password"])
            else:
                # This is for no use the host keys
                opciones = pysftp.CnOpts()
                opciones.hostkeys = None
                self.__connection = pysftp.Connection(host=self.__connection_data["server"],
                                                      port=int(self.__connection_data["port"]),
                                                      username=self.__connection_data["user"],
                                                      password=self.__connection_data["password"],
                                                      cnopts=opciones)
            # Fill answer object with status and message.
            answer.load(True, 'Established Connection to ' + self.__connection_data["type"])
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

    def upload_file(self, original_file, end_file):
        """
        Subir archivos al servidor FTP o SFTP.

        Params:
            * **original_file (String):** Recibe la ruta absoluta y el archivo a subir. \n
            * **end_file (String, optional):** Nombre con el que se va a guardar el archivo. \n

        Returns:
            **answer (Class):** Devuelve estado, mensaje y datos (diccionario, lista) de la función.

        """

        # Invoke class Answer.
        answer = Answer()
        try:
            # Choose the type of server.
            if str(self.__connection_data["type"]).upper() == "FTP":
                with open(original_file, 'rb') as file:
                    self.__connection.storbinary('STOR ' + end_file, file)
            elif str(self.__connection_data["type"]).upper() == "SFTP":
                self.__connection.put(original_file, end_file)
            answer.load(status=True,
                        message=end_file + ' file was uploaded to ' + self.__connection_data["type"])
        except (pysftp.CredentialException, pysftp.ConnectionException, Exception) as exc:
            # Fill variable error
            error_message = self.__this + inspect.stack()[0][3] + ': ' + str(exc)
            # Show error message in console
            print(error_message)
            # Fill answer object with status and error message.
            answer.load(status=False,
                        message=error_message)
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
            # Read FTP configuration from JSON file.
            config = mf.read_setup(item='FTP')
            if config.get_status():
                self.__connection_data["type"] = config.get_data()["FTP_TYPE"]
                self.__connection_data["server"] = config.get_data()["FTP_SERVER"]
                self.__connection_data["port"] = config.get_data()["FTP_PORT"]
                self.__connection_data["user"] = config.get_data()["FTP_USER"]
                self.__connection_data["password"] = config.get_data()["FTP_PASSWORD"]
                self.__connection_data["path"] = config.get_data()["FTP_PATH"]
            # Fill answer object with status, message and data.
            answer.load(status=True,
                        message='Configuration obtained from FTP')
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
