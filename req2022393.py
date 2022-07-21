# -*- coding: utf-8 -*-
"""
Created on Mon Jul 11 17:00:00 2022

@author: Jhonatan Martínez
"""

import inspect
from os import scandir, remove
from os.path import abspath, basename
from multiprocessing import cpu_count, Pool, current_process
import main_functions as mf
from answer import Answer
from connection import Connection
from connection_ftp import ConnectionFTP
from email_smtp import EmailSMTP


class Req2022393:
    """
    Esta es la clase principal de la aplicación, trabaja en BackEnd, no posee FrontEnd.\n
    En el archivo de configuración **config.json** están las diferentes secciones
    para las configuraciones de conexiones.\n
    ¿Qué realiza la aplicación?:\n
    * Conectar a una Base de Datos y consulta tablas para KPIS
    y la información de cada tabla es almacenada en un fichero .xlsx de forma local.\n
    * Después estos ficheros son enviados a un FTP y luego eliminados localmente.\n
    * Guarda un log localmente.
    """

    def __init__(self):
        self.__this = self.__class__.__name__ + '.'
        self.__setup = {}
        self._read_setup()
        self._load()

    def _load(self):
        """
        This method has all the logic of the program.
        """
        # Create Log folders if they don't exist.'
        folder = self._create_folder()
        # Validate folder creation status
        if folder.get_status():
            # Write on the Log file.
            self._write_log_file(message="Application has been launched")
            self._write_log_file(message=folder.get_message())
            # Start function to connect to DB
            pool = self._pool_connection()
            # Validate if data exists
            if pool is not None:
                # Loop on pool data
                for item in pool:
                    # Validate item
                    if not item:
                        # Send email
                        self._send_email(body="El error se presentó pool_connection, por favor revisar el archivo log.")
                        # Break loop
                        break
            # Send file to ftp
            self._send_file_ftp()
            # Write on the Log file.
            self._write_log_file(message="Application has been finished")

    def _create_book(self, data):
        """
        Crear un libro de Excel xlsx

        Params:
            **data (list):** Lista con los nombres que tendrán los archivos y los queries.

        Returns:
            **status (boolean):** estado del método.

        """

        # Variable process
        process = inspect.stack()[0][3] + " - " + current_process().name
        # Write on the Log file.
        self._write_log_file(message="Start process " + process)
        # Variable status
        status = True
        try:
            # Read data
            values = self._read_data(data[1])
            # Validate values variable
            if values is not None:
                # Creates book and Write on the Log file.
                self._write_log_file(message=mf.write_file_xlsx(file_name="Files/" + data[0],
                                                                name_columns=values[0],
                                                                data=values[1]).get_message())
        except Exception as exc:
            # Write on the Log file.
            self._write_log_file(message=self.__this + process + ': ' + str(exc))
            status = False
        finally:
            # Write on the Log file.
            self._write_log_file(message="End process " + process)
        return status

    def _create_folder(self):
        """
        Llama la función 'create_folder()' desde 'main_functions' y le envía el nombre de la carpeta
        la cual será creada en la raíz del aplicativo.

        Returns:
            **answer (Class):** Devuelve un estado y mensaje de la función.

        """

        # Invoke class Answer.
        answer = Answer()
        try:
            # Folder names to create.
            folders = ["Log", "Files"]
            # Start loop on folders
            for folder in folders:
                # Create folder.
                mf.create_folder(folder_name=folder)
            # Fill answer object with status, message and data.
            answer.load(status=True,
                        message="Folders are Ok")
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

    def _get_names_and_queries(self):
        """
        Obtiene los nombres de los archivos y su respectiva consulta desde el JSON
        luego son pasados por los diferentes clusters.

        Returns:
            **names_queries (list):** Devuelve una lista con nombres y queries.
        """

        # Variable process
        process = inspect.stack()[0][3]
        # Write on the Log file.
        self._write_log_file(message="Start process " + process)
        # Variable names_queries
        names_queries = []
        try:
            # Get data
            data = self._read_data(query=self.__setup["CLUSTERS"])
            # Validate data.
            if data is not None:
                # Get yesterday's date.
                yesterday = mf.get_current_date(days=-1,
                                                separator="-").get_data()
                # Read queries from JSON file.
                queries = mf.read_setup("QUERIES").get_data()
                # Loop on queries
                for query in queries:
                    # Loop on data values
                    for item in data[1]:
                        # Get file name
                        name = query["NOMBRE"] + yesterday + " RC" + str(item[0]) + ".xlsx"
                        # Get sql query
                        sql = query["QUERY"] + str(item[0])
                        # Fill list
                        names_queries.append([name, sql])
            else:
                # Send email
                self._send_email("Hubo un error leyendo la base de datos, revisar el log para más información.")
        except Exception as exc:
            # Write on the Log file.
            self._write_log_file(message=self.__this + process + ': ' + str(exc))
        finally:
            # Write on the Log file.
            self._write_log_file(message="End process " + process)
        # Return names_queries.
        return names_queries

    def _pool_connection(self):
        """
        Obtiene los nombres y queries, luego crea un pool para crear varias conexiones simultáneas para luego
        crear los archivos con la data obtenida.

        Returns:
            **data (list):** Lista de boolean.
        """

        # Variable process
        process = inspect.stack()[0][3]
        # Write on the Log file.
        self._write_log_file(message="Start process " + process)
        # Variable data
        data = None
        try:
            # Validate setup status
            if self.__setup["CONNECTION"] == 1:
                # Read to get file names and queries from txt file
                names_queries = self._get_names_and_queries()
                # Create pool with the half of CPU cores
                pool = Pool(processes=round(cpu_count() / 2))
                # Create xlsx file.
                data = pool.map(self._create_book, names_queries)
                # Close pool
                pool.close()
                pool.join()
            else:
                # Write on the Log file.
                self._write_log_file(message="CONNECTION in SETUP is not enabled")
        except Exception as exc:
            # Show error message in console
            self._write_log_file(message=self.__this + process + ': ' + str(exc))
        finally:
            # Write on the Log file.
            self._write_log_file(message="End process " + process)
        # Return data.
        return data

    def _read_data(self, query):
        """
        Obtiene los datos que devuelven un query desde la base de datos.

        Params:
            **query (String):** Sentencia select.

        Returns:
            **data (List):** Lista con columnas y valores.
        """

        # Variable process
        process = inspect.stack()[0][3] + " - " + current_process().name
        # Write on the Log file.
        self._write_log_file(message="Start process " + process)
        # Invoke class Connection.
        cnx = Connection()
        # variable data
        data = None
        try:
            # Read data
            data = cnx.read_data(query=query, datatype='list').get_data()
            # Write on the Log file.
            self._write_log_file(message='Data obtained of: ' + query)
        except Exception as exc:
            # Write on the Log file.
            self._write_log_file(message=self.__this + process + ': ' + str(exc))
        finally:
            # Write on the Log file.
            self._write_log_file(message="End process " + process)
        # Return data.
        return data

    def _read_setup(self):
        """
        Llama a 'read_setup()' desde 'main_functions', para leer y obtener los datos desde el JSON.

        """

        try:
            # Read SETUP configuration from JSON file.
            config = mf.read_setup(item='SETUP')
            # Validate read status
            if config.get_status():
                self.__setup["CONNECTION"] = config.get_data()["CONNECTION"]
                self.__setup["FTP"] = config.get_data()["FTP"]
                self.__setup["EMAIL"] = config.get_data()["EMAIL"]
                self.__setup["CLUSTERS"] = config.get_data()["CLUSTERS"]
            # Show message in console.
            print(config.get_message())
        except Exception as exc:
            # Show error message in console.
            print(self.__this + inspect.stack()[0][3] + ': ' + str(exc))

    def _send_file_ftp(self):
        """
        Llama la función 'upload_file' de la clase 'ConnectionFTP' para enviar archivos al FTP desde la carpeta File.

        """

        # Variable process
        process = inspect.stack()[0][3]
        # Write on the Log file.
        self._write_log_file(message="Start process " + process)
        # Invoke class ConnectionFTP.
        ftp = ConnectionFTP()
        try:
            # Validate setup status
            if self.__setup["FTP"] == 1:
                # Open connection to FTP server
                cnx = ftp.get_connection()
                # Validate connection status
                if cnx.get_status():
                    # Change path on FTP and write on txt log
                    change = ftp.change_path()
                    # Write on the Log file.
                    self._write_log_file(message=change.get_message())
                    # Validate path change status
                    if change.get_status():
                        # Loop on the xlsx files in the Files folder
                        for file in scandir(abspath("Files")):
                            # Validate if it is a file
                            if file.is_file():
                                # Upload file to ftp
                                upload = ftp.upload_file(file.path, basename(file))
                                # Write on the Log file.
                                self._write_log_file(message=upload.get_message())
                                # Validate if the file was sent
                                if upload.get_status():
                                    # Delete local file
                                    remove(file.path)
                                else:
                                    # Send email
                                    self._send_email(body=upload.get_message())
                    else:
                        # Send email
                        self._send_email(body=change.get_message())
                    # Close connection
                    ftp.close_connection()
                else:
                    # Write on the Log file.
                    self._write_log_file(message=cnx.get_message())
                    # Send email
                    self._send_email(body=cnx.get_message())
            else:
                # Write on the Log file.
                self._write_log_file(message="FTP in SETUP is not enabled")
        except Exception as exc:
            # Write on the Log file.
            self._write_log_file(message=self.__this + process + ': ' + str(exc))
            # Send email
            self._send_email(body=self.__this + process + ': ' + str(exc))
        finally:
            # Write on the Log file.
            self._write_log_file(message="End process " + process)

    def _send_email(self, body):
        """
        Llama la función 'send_email' de la clase 'EmailSMTP' para enviar un correo electrónico.

        Params:
            **body (String):** Cuerpo del correo electrónico.

        """

        # Variable process
        process = inspect.stack()[0][3]
        # Write on the Log file.
        self._write_log_file(message="Start process " + process)
        # Invoke class EmailSMTP.
        email = EmailSMTP()
        try:
            # Validate setup status
            if self.__setup["EMAIL"] == 1:
                # Build body variable
                body = "Ocurrió un error durante la ejecución del proceso. \n" + body
                # Send email and Write on the Log file.
                self._write_log_file(message=email.send_email(body=body, html=True).get_message())
            else:
                # Write on the Log file.
                self._write_log_file(message="Email in SETUP is not enabled")
        except Exception as exc:
            # Write on the Log file.
            self._write_log_file(message=self.__this + process + ': ' + str(exc))
        finally:
            # Write on the Log file.
            self._write_log_file(message="End process " + process)

    def _write_log_file(self, message):
        """
        Llama a 'write_file_text()' desde 'main_functions' y guarda el texto en el archivo plano.

        Params:
            **message (String):** Dato a copiar en el archivo plano.
        """

        # Variable final_message.
        final_message = '[' + mf.get_current_time().get_data() + ']: ' + message + '.'
        # Write on the Log file.
        mf.write_file_text(file_name='Log/Log' + mf.get_current_date().get_data(),
                           message=final_message + '\n')
        # Show message in console.
        print(final_message)


if __name__ == '__main__':
    main = Req2022393()
