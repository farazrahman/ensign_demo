import json
import asyncio
import warnings

from pyensign.ensign import Ensign
from pyensign.api.v1beta1.ensign_pb2 import Nack

# TODO: Python>=3.10 raises a DeprecationWarning: There is no current event loop. We need to fix this in PyEnsign!
warnings.filterwarnings("ignore", category=DeprecationWarning)


class AqiSubscriber:
    """
    AqiSubscriber consumes air quality events from Ensign.
    """

    def __init__(self, topic="aqi-json", ensign_creds=""):
        """
        Create a AqiSubscriber to consume AQI json events from an Ensign
        topic. Nothing will be consumed until run() is called.

        Parameters
        ----------
        topic : str (default: "aqi-json")
            The name of the topic to consume from.

        ensign_creds : str (optional)
            The path to your Ensign credentials file. If not provided, credentials will
            be read from the ENSIGN_CLIENT_ID and ENSIGN_CLIENT_SECRET environment
            variables.
        """
        self.topic = topic
        self.ensign = Ensign(cred_path=ensign_creds)

    def run(self):
        """
        Run the subscriber forever.
        """
        asyncio.get_event_loop().run_until_complete(self.subscribe())

    async def handle_event(self, event):
        """
        Decode and ack the event back to Ensign.
        """
        try:
            data = json.loads(event.data)
        except json.JSONDecodeError:
            print("Received invalid JSON in event payload:", event.data)
            await event.nack(Nack.Code.UNKNOWN_TYPE)
            return

        print("Received AQI json:", data)
        await event.ack()

    async def subscribe(self):
        """
        Subscribe to the AQI json topic and parse the events.
        """
        id = await self.ensign.topic_id(self.topic)
        await self.ensign.subscribe(id, on_event=self.handle_event)
        await asyncio.Future()


if __name__ == "__main__":
    subscriber = AqiSubscriber(ensign_creds="cred/ensign_key.json")
    subscriber.run()