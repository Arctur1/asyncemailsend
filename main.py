import sqlite3
from sqlite3 import Error
import datetime
import aiosmtplib

from email.message import EmailMessage
import asyncio

db_file = 'contacts.db'

from more_itertools import chunked


def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)
    return conn


async def send_to_contacts(contact: tuple):
    message = EmailMessage()
    message["From"] = "root@localhost"
    message["To"] = contact[3]
    message["Subject"] = "Здравствуйте"
    message.set_content(f"Уважаемый {contact[1]} {contact[2]}\nСпасибо, что пользуетесь нашим сервисом объявлений.")
    await aiosmtplib.send(message,
                          hostname="smtp.gmail.com",
                          port=465, use_tls=True,
                          username="",
                          password="")
    print("succes", str(datetime.datetime.now()))


async def send_test(contact: tuple):
    message = EmailMessage()
    message["From"] = "root@localhost"
    message["To"] = "totest"
    message["Subject"] = "Здравствуйте"
    message.set_content(f"Уважаемый \nСпасибо, что пользуетесь нашим сервисом объявлений.")
    await aiosmtplib.send(message,
                          hostname="smtp.gmail.com",
                          port=465, use_tls=True,
                          username="",
                          password="")
    print("succes", str(datetime.datetime.now()))


async def main():
    conn = create_connection(db_file)
    cur = conn.cursor()
    cur.execute("SELECT * FROM contacts")
    contacts = cur.fetchall()
    for chunk in chunked(contacts, 10):
        print(list(chunk))
        tasks = [asyncio.create_task(send_to_contacts(contact)) for contact in chunk]
        results = await asyncio.gather(*tasks)


asyncio.run(main())
