# -*- coding: utf-8 -*-
"""
Created on Mon Jul 11 17:00:00 2022

@author: Jhonatan Martínez

 Este paquete tiene funciones principales para el funcionamiento del aplicativo,
 importa los siguientes paquetes y clases:\n
    * **inspect:** Para obtener información de la función actual.\n
    * **json:** Para la lectura del fichero de configuración config.json.\n
    * **os:** Para obtener la ruta absoluta de la aplicación.\n
    * **datetime:** Dónde se obtiene la fecha y hora actual.\n
    * **requests:** Para conectar y hacer peticiones a una URL.\n
    * **Answer:** Dónde se envían el estado, mensaje y datos desde una función.\n

"""
import inspect
import json
import openpyxl
import os
from datetime import datetime, timedelta
from answer import Answer


def create_folder(folder_name):
    """
    Crear una carpeta local en la ruta dónde se encuentra la aplicación.

    Params:
        **folder_name (String):** Nombre de la carpeta para ser creada.

    Returns:
        **Answer (Class):** Devuelve un estado y mensaje de la función.

    """

    # Invoke class Answer.
    answer = Answer()
    try:
        # Variable for the message.
        message = 'Folder'
        # Validate if the folder don´t exists.
        if not os.path.exists(get_current_path().get_data() + '\\' + folder_name):
            # Create the folder.
            os.mkdir(folder_name)
            # Fill the message.
            message = folder_name + ' Folder was created'
        else:
            # Fill the message.
            message = folder_name + ' Folder already exists'
        # Fill answer object with status and message.
        answer.load(status=True,
                    message=message)
    except Exception as exc:
        # Fill variable error
        error_message = 'main_functions.' + inspect.stack()[0][3] + ': ' + str(exc)
        # Show error message in console
        print(error_message)
        # Fill answer object with status and error message.
        answer.load(status=False,
                    message=error_message)
    # Return answer object.
    return answer


def get_current_date(days=0, separator="-"):
    """
    Obtener la fecha

    Params:
        * **days (Integer, optional):** Suma o resta días a la fecha actual.
        * **separator (String, optional):** Separador para dar formato a la fecha "-, _"

    Returns:
        **Answer (Class):** Devuelve un estado, mensaje y dato (fecha String) de la función.
    """

    # Invoke class Answer.
    answer = Answer()
    try:
        # Gets current date.
        now = datetime.now() + timedelta(days=days)
        # Fill answer object with status, message and data.
        answer.load(status=True,
                    message='Current date obtained',
                    data=str(now.year) + separator
                    + str('0' + str(now.month))[-2:] + separator
                    + str('0' + str(now.day))[-2:])
    except Exception as exc:
        # Fill variable error
        error_message = 'main_functions.' + inspect.stack()[0][3] + ': ' + str(exc)
        # Show error message in console
        print(error_message)
        # Fill answer object with status and error message.
        answer.load(status=False,
                    message=error_message)
    # Return answer object.
    return answer


def get_current_path():
    """
        Obtiene la ruta actual de la aplicación.

        Returns:
            **Answer (Class):** Devuelve un estado, mensaje y dato (ruta String) de la función.

        """

    # Invoke class Answer.
    answer = Answer()
    try:
        # Variable for the path.
        path = os.getcwd()
        # Fill answer object with status and message.
        answer.load(status=True,
                    message="Path obtained",
                    data=path)
    except Exception as exc:
        # Fill variable error
        error_message = 'main_functions.' + inspect.stack()[0][3] + ': ' + str(exc)
        # Show error message in console
        print(error_message)
        # Fill answer object with status and error message.
        answer.load(status=False,
                    message=error_message)
    # Return answer object.
    return answer


def get_current_time():
    """
    Obtener la hora actual en formato hh:mm:ss.

    Returns:
        **Answer (Class):** Devuelve estado, mensaje y dato (hora String) de la función.

    """

    # Invoke class Answer.
    answer = Answer()
    try:
        # Gets current date.
        now = datetime.now()
        # Fill answer object with status, message and data.
        answer.load(status=True,
                    message='Current time obtained',
                    data=str('0' + str(now.hour))[-2:] + ':'
                    + str('0' + str(now.minute))[-2:] + ':'
                    + str('0' + str(now.second))[-2:])
    except Exception as exc:
        # Fill variable error
        error_message = 'main_functions.' + inspect.stack()[0][3] + ': ' + str(exc)
        # Show error message in console
        print(error_message)
        # Fill answer object with status and error message.
        answer.load(status=False,
                    message=error_message)
    # Return answer object.
    return answer


def read_file(file_name):
    """
    Leer un archivo plano y obtener línea por línea.
    Args:
        **file_name (String):** Nombre con ruta absoluta del archivo.

    Returns:
        **Answer (Class):** Devuelve estado, mensaje y dato (archivo List) de la función.
    """

    # Invoke class Answer.
    answer = Answer()
    try:
        # read file
        with open(file_name) as file:
            line_list = file.readlines()
        # Fill answer object with status, message and data.
        answer.load(status=True,
                    message='The file was read',
                    data=line_list)
    except Exception as exc:
        # Fill variable error
        error_message = 'main_functions.' + inspect.stack()[0][3] + ': ' + str(exc)
        # Show error message in console
        print(error_message)
        # Fill answer object with status and error message.
        answer.load(status=False,
                    message=error_message)
    # Return answer object.
    return answer


def read_setup(item=None):
    """
    Esta función permite leer y obtener los datos desde archivo JSON.\n

    Params:
        **item (String, opcional):** Si se requiere un item principal especifico. None por defecto.

    Returns:
        **Answer (Class):** Devuelve un estado, mensaje y dato (Dict) de la función.

    """

    # Invoke class Answer.
    answer = Answer()
    try:
        # Open the JSON file.
        with open(file='config.json', encoding='utf-8') as file:
            if item is None:
                # Read all the data.
                answer.load(status=True,
                            message='Configuration obtained',
                            data=json.load(file))
            else:
                # Read the specified data.
                answer.load(status=True,
                            message='Configuration obtained',
                            data=json.load(file)[item])
    except Exception as exc:
        # Fill variable error
        error_message = 'main_functions.' + inspect.stack()[0][3] + ': ' + str(exc)
        # Show error message in console
        print(error_message)
        # Fill answer object with status and error message.
        answer.load(status=False,
                    message=error_message)
    # Return answer object.
    return answer


def write_file_text(file_name, message):
    """
    Permite escribir en un archivo plano.

    Params:
        * **file_name (String):** Nombre del archivo plano.\n
        * **message (String):** Mensaje a escribir en el archivo plano.

    Returns:
        **Answer (Class):** Devuelve un estado y mensaje de la función.

    """

    # Invoke class Answer.
    answer = Answer()
    try:
        # Open the file to write.
        with open(file=file_name + '.txt', mode='a', encoding='utf-8') as file:
            # Write on the file.
            file.write(message + '\n')
        # Fill answer object with status and message.
        answer.load(status=True,
                    message='Written file')
    except Exception as exc:
        # Fill variable error
        error_message = 'main_functions.' + inspect.stack()[0][3] + ': ' + str(exc)
        # Show error message in console
        print(error_message)
        # Fill answer object with status and error message.
        answer.load(status=False,
                    message=error_message)
    # Return answer object.
    return answer


def write_file_xlsx(file_name, column_names, row_values):
    """
    Crear un archivo xlsx.
    Params:
        * **file_name (String):** Nombre y path del archivo a crear.
        * **column_names (List):** Nombre de las columnas.
        * **row_values (List):** Valores de las filas.

    Returns:
        **Answer (Class):** Devuelve un estado y mensaje de la función.
    """

    # Invoke class Answer.
    answer = Answer()
    try:
        # Create workbook
        workbook = openpyxl.Workbook()
        # Create sheet
        sheet = workbook.active
        # Add column names
        sheet.append(column_names)
        # Add row values
        for item in row_values:
            sheet.append(item)
        # Save workbook
        workbook.save(file_name)
        # Fill answer object with status and message.
        answer.load(status=True,
                    message='The file ' + file_name + " was saved")
    except Exception as exc:
        # Fill variable error
        error_message = 'main_functions.' + inspect.stack()[0][3] + ': ' + str(exc)
        # Show error message in console
        print(error_message)
        # Fill answer object with status and error message.
        answer.load(status=False,
                    message=error_message)
    # Return answer object.
    return answer
