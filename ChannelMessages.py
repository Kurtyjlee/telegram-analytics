import configparser
import json
import asyncio
from datetime import date, datetime

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import PeerChannel
import pandas as pd


# some functions to parse json date
class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()

        if isinstance(o, bytes):
            return list(o)

        return json.JSONEncoder.default(self, o)


# Reading Configs
config = configparser.ConfigParser()
config.read("config.ini")

# Setting configuration values
api_id = config['Telegram']['api_id']
api_hash = config['Telegram']['api_hash']

api_hash = str(api_hash)

phone = config['Telegram']['phone']
username = config['Telegram']['username']

# Create the client and connect
client = TelegramClient(username, api_id, api_hash)

async def main(phone):
    await client.start()
    print("Client Created")
    # Ensure you're authorized
    if await client.is_user_authorized() == False:
        await client.send_code_request(phone)
        try:
            await client.sign_in(phone, input('Enter the code: '))
        except SessionPasswordNeededError:
            await client.sign_in(password=input('Password: '))

    me = await client.get_me()

    user_input_channel = input('enter entity(telegram URL or entity id):')

    if user_input_channel.isdigit():
        entity = PeerChannel(int(user_input_channel))
    else:
        entity = user_input_channel

    my_channel = await client.get_entity(entity)

    offset_id = 0
    limit = 100
    all_messages = []
    total_messages = 0
    total_count_limit = 0

    while True:
        print("Current Offset ID is:", offset_id, "; Total Messages:", total_messages)
        history = await client(GetHistoryRequest(
            peer=my_channel,
            offset_id=offset_id,
            offset_date=None,
            add_offset=0,
            limit=limit,
            max_id=0,
            min_id=0,
            hash=0
        ))

        if not history.messages:
            break
        messages = history.messages
        for message in messages:
            all_messages.append(message.to_dict())
        offset_id = messages[len(messages) - 1].id
        total_messages = len(all_messages)
        if total_count_limit != 0 and total_messages >= total_count_limit:
            break
    
    # Getting a concised version of the view count
    new_messages = {"Date": None}
    for msg in all_messages:
        # Reject any msg with no views
        if "views" not in msg:
            continue
        # Skip non november months
        elif msg["date"].month != 12 or msg["date"].year != 2022:
            continue

        msg["date"] = msg["date"].date().strftime('%d/%m/%Y')

        if msg["date"] not in new_messages:
            new_messages[msg["date"]] = msg["views"]
        else: 
            new_messages[msg["date"]] = new_messages[msg["date"]] + msg["views"]

    # Getting csv
    df = pd.read_csv('channel_msg.csv')

    # Getting values
    new_row = list(new_messages.values())
    new_row[0] = date.today().strftime('%d/%m/%Y')

    # Operations
    for i in range(1, len(df.columns), 1):
        new_row[i] = new_row[i] - df.iloc[-1, i]
    
    # Adding new col
    if len(df.columns) < len(new_row):
        df[list(new_messages)[-1]] = None

    # Adding new row
    df.loc[len(df.index)] = new_row

    # # Saving to csv
    df.to_csv('channel_msg.csv', index=False)

with client:
    client.loop.run_until_complete(main(phone))
