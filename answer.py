# -*- coding: utf-8 -*-
"""
Created on Thu Jun 16 08:00:00 2022

@author: Jhonatan Martínez
"""


class Answer:
    """
    Permite manejar información (estado, mensaje, datos) de una función en una forma transversal.
    """

    def __init__(self):
        self.__data = None
        self.__message = None
        self.__status = None

    def get_data(self):
        """
        Devuelve la data que pueda enviar una función.

        Returns:
            **Data (Any):** Devuelve cualquier dato almacenado(String, Integer, lista, diccionario).
        """

        # Return data value.
        return self.__data

    def get_message(self):
        """
        Devuelve un mensaje (String) que pueda enviar una función.

        Returns:
            **Message (String):** Devuelve un mensaje tipo String.

        """

        # Return message value.
        return str(self.__message)

    def get_status(self):
        """
       Devuelve el estado (Boolean) que pueda enviar una función.

        Returns:
            **Status (Boolean):** \n
            \t* **True:** Para saber que la función terminó de forma correcta. \n
            \t* **False:** Para saber que la función no terminó correctamente.

        """

        # Return status value.
        return self.__status

    def load(self, status, message, data=None):
        """
        Permite cargar información del estado, el mensaje y los datos que pueda enviar una función.

        Params:
            * **status (Boolean):** Permite saber si el proceso en una función fue o no exitosa. \n
            * **message (String):** Permite guardar un mensaje del proceso en una función. \n
            * **data (Any, optional):** Permite guardar cualquier tipo de dato. None por defecto.

        """

        # Variable assignment.
        self.__status = status
        self.__message = message
        self.__data = data
