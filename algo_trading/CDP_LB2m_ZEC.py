from cdp import CdpClient
from dotenv import load_dotenv
import asyncio

load_dotenv()

async def main():
    async with CdpClient() as cdp:
        pass

asyncio.run(main())