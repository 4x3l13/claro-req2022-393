# -*- coding: utf-8 -*-
"""
Created on Fri Jun 22 08:00:00 2022

@author: Jhonatan Martínez
"""

import inspect
import smtplib
from email.message import EmailMessage
from answer import Answer
import main_functions as mf


class EmailSMTP:
    """
    Permite la conexión y envío de email por SMTP,
    los datos de configuración están en el archivo JSON llamado **config.json**.\n
    Para su funcionamiento importa los siguientes paquetes y clases:\n
    * **inspect:** Para obtener información de la función actual.\n
    * **smtplib:** Para conectar al servidor SMTP.\n
    * **EmailMessage:** Dónde se configura el email.\n
    * **Answer:** Dónde se envían el estado, mensaje y datos desde una función.\n
    * **main_functions:** Funciones principales para el funcionamiento del aplicativo.

    """

    def __init__(self):
        """
        Los datos están en la sección EMAIL en el archivo JSON los cuales son:\n
        * **SMTP:** Servidor del SMTP.\n
        * **PORT:** Puerto del servidor SMTP.\n
        * **FROM:** Correo desde el que se va a enviar el email.\n
        * **PASSWORD:** Clave del correo desde que se va a enviar el email.\n
        * **TO:** Correo(s) a los que se va a enviar el email.\n
        * **SUBJECT:** Asunto que va a tener el email.\n
        """
        self.__this = self.__class__.__name__ + '.'
        self.__server = {}
        self.__email = EmailMessage()
        self.__connection = None
        self._read_setup()

    def _close_connection(self):
        """
        Esta función cierra la conexión al servidor SMTP.

        """

        try:
            # Close connection.
            self.__connection.quit()
        except ConnectionError as exc:
            # Show the error in console
            print(self.__this + inspect.stack()[0][3] + ': ' + str(exc))

    def _get_connection(self):
        """
        Realiza y establece la conexión al servidor SMTP.\n
        Los datos son leídos desde la función '_read_setup()', desde la sección EMAIL del JSON,
        Estos datos se obtienen y agregan en el constructor de la clase.

        Returns:
            **Answer (Class):** Devuelve un estado y mensaje de la función.

        """

        # Invoke class Answer.
        answer = Answer()
        try:
            # Set smtp server and port.
            self.__connection = smtplib.SMTP(self.__server["smtp"], self.__server["port"])
            if self.__server["password"] != "":
                # Identify this cliente to the SMTP server.
                self.__connection.ehlo()
                # Secure the SMTP connection.
                self.__connection.starttls()
                # Identify this cliente to the SMTP server.
                self.__connection.ehlo()
                # Login to email account.
                self.__connection.login(self.__email["From"], self.__server["password"])
            # Fill answer object with status and message.
            answer.load(status=True,
                        message='Connection established to SMTP')
        except ConnectionError as exc:
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
        Llama al método 'read_setup()' desde 'main_functions',
        para leer y obtener los datos desde archivo JSON.

        Returns:
            **Answer (Class):** Devuelve un estado y mensaje de la función.

        """

        # Invoke class Answer.
        answer = Answer()
        try:
            # Read EMAIL configuration from JSON file.
            config = mf.read_setup(item='EMAIL')
            if config.get_status():
                self.__server["smtp"] = config.get_data()["SMTP"]
                self.__server["port"] = config.get_data()["PORT"]
                self.__email["From"] = config.get_data()["FROM"]
                self.__server["password"] = config.get_data()["PASSWORD"]
                self.__email["To"] = config.get_data()["TO"]
                self.__email["Subject"] = config.get_data()["SUBJECT"]
            # Fill answer object with status, message and data.
            answer.load(status=True,
                        message='Configuration obtained from EMAIL')
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

    def send_email(self, body, html=False):
        """
        Configura y permite enviar un correo electrónico.

        Params:
            * **body (String):** Contiene la descripción o cuerpo del email.\n
            * **html (Boolean, optional):** Email con cuerpo HTML en True. False por defecto.

        Returns:
            **Answer (Class):** Devuelve un estado y, mensaje de la función.
        """

        # Invoke class Answer.
        answer = Answer()
        try:
            # Connect to the SMTP server.
            cnx = self._get_connection()
            if cnx.get_status():
                # Add content to the email and with type html.
                if html:
                    self.__email.set_content(body, 'html')
                else:
                    print(body)
                    self.__email.set_content(body)
                # Send message by email.
                self.__connection.send_message(self.__email)
                # Close the connection.
                self._close_connection()
                # Fill answer object with status and message.
                answer.load(status=True,
                            message='Email sent to: ' + self.__email["To"])
            else:
                answer.load(status=cnx.get_status(),
                            message=cnx.get_message())
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
