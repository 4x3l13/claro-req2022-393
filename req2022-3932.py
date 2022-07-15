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
from os import scandir, remove, getpid
from os.path import abspath, basename
from mymultiprocessing import cpu_count, Pool, Process

class Req2022_393:
    """
    Esta es la clase principal de la aplicación, trabaja en BackEnd, no posee FrontEnd.\n
    En el archivo de configuración **config.json** están las diferentes secciones
    para las configuraciones de conexiones.\n
    ¿Qué realiza la aplicación?:\n
    * Conectar a una Base de Datos y consulta tablas para KPIs desde un fichero
    y la información de cada tabla es almacenada en un fichero .xlsx de forma local.\n
    * Después estos ficheros son enviados a un FTP y luego eliminados localmente.\n
    * Guarda un log localmente.
    """

    def __init__(self):
        self.__this = self.__class__.__name__ + '.'
        self.__ftp = ConnectionFTP()
        self._load()

    def _load(self):
        """
        This method has all the logic of the program.
        """
        # Create Log folder if it doesn't exist.'
        folder = self._create_folder()
        if folder.get_status():
            # Write on the Log file.
            self._write_log_file(message="Application has been launched")
            self._write_log_file(message=folder.get_message())
            # Create xlsx file.
            self._create_book()
            # Send file to ftp
            self._send_file_ftp()
            # Write on the Log file.
            self._write_log_file(message="Application has been finished")

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
            # Fill answer object with status and error message.
            answer.load(status=False,
                        message=self.__class__.__name__ + '.' + inspect.stack()[0][3] + ': ' + str(exc))
        finally:
            # Show the message by console
            print(answer.get_message())
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
        self._write_log_file(message='Reading ' + file + 'file')
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
            names_queries =[]
            for index in range(len(name_list)):
                names_queries.append([name_list[index], query_list[index]])
            answer.load(True, 'File was read', names_queries)
        except Exception as exc:
            # Fill answer object with status and error message.
            answer.load(status=False,
                        message=self.__class__.__name__ + '.' + inspect.stack()[0][3] + ': ' + str(exc))
        finally:
            # Show function message in console
            print(answer.get_message())
            # Write on the Log file.
            self._write_log_file(message=answer.get_message())
        # Return answer object.
        return answer

    def _read_data(self, queries):
        cnx = Connection()
        # Invoke class Answer.
        answer = Answer()
        # Write on the Log file.
        self._write_log_file(message='Opening connection and Getting data from Data Base')
        try:
            cnx1 = cnx.read_data(query=queries[1], datatype='list')
            name_file = "Files/" + str(queries[0]).replace("XXXXXXXXXX",
                                                           mf.get_current_date(days=-1, separator="_").get_data())
            book = mf.write_file_xlsx(name_file, cnx1.get_data()[0], cnx1.get_data()[1])
            self._write_log_file(message=book.get_message())
            # Fill answer object with status, message and data.
            answer.load(status=True,
                        message="Data obtained",
                        data=cnx1.get_data())
        except Exception as exc:
            # Fill answer object with status and error message.
            answer.load(status=False,
                        message=self.__class__.__name__ + '.' + inspect.stack()[0][3] + ': ' + str(exc))
        finally:
            # Write on the Log file.
            self._write_log_file(message=answer.get_message())
            # Return answer object.
        return answer

    def _create_book(self):
        """

        Returns:
            **answer (Class):** Devuelve un estado, mensaje y dato (nombres y queries) de la función.
        """

        # Invoke class Answer.
        answer = Answer()
        # Get file names and queries from txt file
        names_queries = self._get_names_and_queries().get_data()
        print(names_queries)
        try:
            # Loop queries list
            cores = cpu_count()
            pool = Pool(processes=cores).map(self._read_data, names_queries)
            pool.close()

            # Fill answer object with status, message and data.
            answer.load(status=True,
                        message="Files were written")
        except Exception as exc:
            # Fill answer object with status and error message.
            answer.load(status=False,
                        message=self.__class__.__name__ + '.' + inspect.stack()[0][3] + ': ' + str(exc))
        finally:
            # Write on the Log file.
            self._write_log_file(message=answer.get_message())
        # Return answer object.
        return answer

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
            email = self.__email.send_email(body=body, html=True)
            # Fill answer object with status, message and data.
            answer.load(email.get_status(), email.get_message(), None)
        except Exception as exc:
            # Fill answer object with status and error message.
            answer.load(False,
                        self.__class__.__name__ + '.' + inspect.stack()[0][3] + ': ' + str(exc),
                        None)
        finally:
            # Write on the Log file.
            self._write_log_file(message=answer.get_message())
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
            # Fill answer object with status and error message.
            answer.load(status=False,
                        message=self.__this + inspect.stack()[0][3] + ': ' + str(exc))
        finally:
            # Write on the Log file.
            self._write_log_file(message=answer.get_message())
        # Return answer object.
        return answer

    def _send_file_ftp(self):
        for file in scandir(abspath("Files")):
            if file.is_file():
                try:
                    print(self.__ftp.upload_file(file.path, basename(file)).get_message())
                    remove(file.path)
                except Exception as exc:
                    pass

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
