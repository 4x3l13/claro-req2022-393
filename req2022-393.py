# -*- coding: utf-8 -*-
"""
Created on Mon Jul 11 17:00:00 2022

@author: Jhonatan Martínez
"""

import inspect
import main_functions as mf
from answer import Answer
from connection import Connection
from connection_ftp import ConnectionFTP
from email_smtp import EmailSMTP
from os import scandir, remove
from os.path import abspath, basename
from multiprocessing import cpu_count, Pool


class Req2022_393:
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
        self.__ftp = ConnectionFTP()
        self.__email = EmailSMTP()
        self._load()

    def _load(self):
        """
        This method has all the logic of the program.
        """
        try:
            # Create Log folders if they don't exist.'
            folder = self._create_folder()
            if folder.get_status():
                # Write on the Log file.
                self._write_log_file(message="Application has been launched")
                # Write on the Log file.
                self._write_log_file(message=folder.get_message())
                # Read to get file names and queries from txt file
                names_queries = self._get_names_and_queries().get_data()
                # Create pool
                pool = Pool(processes=cpu_count())
                # Create xlsx file.
                data = pool.map(self._create_book, names_queries)
                # Close pool
                pool.close()
                pool.join()
                status = True
                for item in data:
                    if not item.get_status():
                        status = False
                        self._send_email(body=item.get_message())
                        break
                if status:
                    # Send file to ftp
                    self._send_file_ftp()
                # Write on the Log file.
                self._write_log_file(message="Application has been finished")
            else:
                print("Folder were not created")
        except Exception as exc:
            print(exc)

    def _create_book(self, data):
        """
        Crear un libro de Excel xlsx
        Returns:
            **answer (Class):** Devuelve un estado y mensaje de la función.
        """

        # Invoke class Answer.
        answer = Answer()
        try:
            # Read data
            values = self._read_data(data[1])
            if values.get_status():
                # Replacing file name
                name_file = "Files/" + str(data[0]).replace("XXXXXXXXXX",
                                                            mf.get_current_date(days=0, separator="_").get_data())
                # Creates book
                mf.write_file_xlsx(name_file, values.get_data()[0], values.get_data()[1])
                # Fill answer object with status, message and data.
                answer.load(status=True,
                            message=name_file + " file was created")
            else:
                # Fill answer object with status, message and data.
                answer.load(status=values.get_status(),
                            message=values.get_message())
        except Exception as exc:
            # Fill variable error
            error_message = self.__this + inspect.stack()[0][3] + ': ' + str(exc)
            # Show error message in console
            print(error_message)
            # Fill answer object with status and error message.
            answer.load(status=False,
                        message=error_message)
        finally:
            # Write on the Log file.
            self._write_log_file(message=answer.get_message())
        # Return answer object.
        return answer

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
            # Folder names
            folders = ["Log", "Files"]
            for folder in folders:
                # Create folder.
                mf.create_folder(folder_name=folder)
            # Fill answer object with status, message and data.
            answer.load(status=True,
                        message="Folders Ok")
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
        Obtiene los nombres de los archivos y su respectiva consulta.

        Returns:
            **answer (Class):** Devuelve un estado, mensaje y dato (nombres y queries) de la función.
        """

        # Invoke class Answer.
        answer = Answer()
        # Variable with the file name to read
        file = "queries.txt"
        # Write on the Log file.
        self._write_log_file(message='Reading ' + file + ' file')
        try:
            # Gets list about the flat file
            all_list = mf.read_file(file).get_data()
            # Variables
            query_list = []
            name_list = []
            for index, item in enumerate(all_list):
                if ((index + 1) % 2) == 0:
                    query_list.append(str(item).strip())
                else:
                    name_list.append(str(item).strip())
            names_queries = []
            for index in range(len(name_list)):
                names_queries.append([name_list[index], query_list[index]])
            answer.load(True, 'File was read', names_queries)
        except Exception as exc:
            # Fill variable error
            error_message = self.__this + inspect.stack()[0][3] + ': ' + str(exc)
            # Show error message in console
            print(error_message)
            # Fill answer object with status and error message.
            answer.load(status=False,
                        message=error_message)
        finally:
            # Write on the Log file.
            self._write_log_file(message=answer.get_message())
        # Return answer object.
        return answer

    def _read_data(self, query):
        cnx = Connection()
        # Invoke class Answer.
        answer = Answer()
        try:
            # Write on the Log file.
            self._write_log_file(message='Opening connection and Getting data from Data Base')
            cnx1 = cnx.read_data(query=query, datatype='list')
            # Fill answer object with status, message and data.
            if cnx1.get_status():
                answer.load(status=True,
                            message="Data obtained",
                            data=cnx1.get_data())
            else:
                answer.load(status=cnx1.get_status(),
                            message=cnx1.get_message())
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

    def _read_setup(self, item):
        """
        Llama a 'read_setup()' desde 'main_functions' para la lectura de la configuración
        en el JSON.

        Args:
            **item (String):** Nombre del item a leer del archivo de configuración.

        Returns:
            **answer (Class):** Devuelve un estado, mensaje y datos (Dict) de la función.
        """

        # Invoke class Answer.
        answer = Answer()
        # Write on the Log file.
        self._write_log_file(message='Reading ' + item + ' configuration from JSON file')
        try:
            # Read STORED_PROCEDURE configuration from JSON file.
            config = mf.read_setup(item=item)
            # Fill answer object with status, message and data.
            answer.load(status=config.get_status(),
                        message=config.get_message(),
                        data=config.get_data())
        except Exception as exc:
            # Fill variable error
            error_message = self.__this + inspect.stack()[0][3] + ': ' + str(exc)
            # Show error message in console
            print(error_message)
            # Fill answer object with status and error message.
            answer.load(status=False,
                        message=error_message)
        finally:
            # Write on the Log file.
            self._write_log_file(message=answer.get_message())
        # Return answer object.
        return answer

    def _send_file_ftp(self):
        # Invoke class Answer.
        answer = Answer()
        # Open connection to FTP server and write on txt log
        try:
            ftp = self.__ftp.get_connection()
            if ftp.get_status():
                # Change path on FTP and write on txt log
                self._write_log_file(self.__ftp.change_path().get_message())
                # Loop on the xlsx files in the Files folder
                for file in scandir(abspath("Files")):
                    if file.is_file():
                        print(self.__ftp.upload_file(file.path, basename(file)).get_message())
                        remove(file.path)
                        # Fill answer object with status, message and data.
                self.__ftp.close_connection()
                answer.load(status=True,
                            message="Data obtained")
            else:
                answer.load(status=ftp.get_status(),
                            message=ftp.get_message())
        except Exception as exc:
            # Fill variable error
            error_message = self.__this + inspect.stack()[0][3] + ': ' + str(exc)
            # Show error message in console
            print(error_message)
            # Fill answer object with status and error message.
            answer.load(status=False,
                        message=error_message)
            self._send_email(body=error_message)
        finally:
            # Write on the Log file.
            self._write_log_file(message=answer.get_message())

    def _send_email(self, body):
        """
        Llama la función 'send_email' de la clase 'Email_smtp' para enviar un correo electrónico.

        Args:
            **body (String):** Cuerpo del correo electrónico.

        Returns:
            **answer (Class):** Devuelve un estado y mensaje de la función.
        """

        # Invoke class Answer.
        answer = Answer()
        # Write on the Log file.
        self._write_log_file('Preparing to send email')
        try:
            body = "Ocurrió un error durante la ejecución del proceso \n" + body
            email = self.__email.send_email(body=body, html=True)
            # Fill answer object with status, message and data.
            answer.load(status=email.get_status(),
                        message=email.get_message())
        except Exception as exc:
            # Fill variable error
            error_message = self.__this + inspect.stack()[0][3] + ': ' + str(exc)
            # Show error message in console
            print(error_message)
            # Fill answer object with status and error message.
            answer.load(status=False,
                        message=error_message)
        finally:
            # Write on the Log file.
            self._write_log_file(message=answer.get_message())
        # Return answer object.
        return answer

    def _write_log_file(self, message):
        """
        Llama a 'write_file_text()' desde 'main_functions' y guarda el texto en el archivo plano.

        Args:
            **message (String):** Dato a copiar en el archivo plano.
        """

        # Write on the Log file.
        mf.write_file_text(file_name='Log/Log' + mf.get_current_date().get_data(),
                           message='[' + mf.get_current_time().get_data() + ']: ' + message + '.' + '\n')



if __name__ == '__main__':
    main = Req2022_393()
