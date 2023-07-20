import json, requests
import asyncio
import warnings
from datetime import datetime, date
from aiohttp import ClientSession, BasicAuth
from pyensign.events import Event
from pyensign.ensign import Ensign

# TODO: Python>=3.10 raises a DeprecationWarning: There is no current event loop. We need to fix this in PyEnsign!
warnings.filterwarnings("ignore", category=DeprecationWarning)


class AqiPublisher:
    """
    AqiPublisher queries the Air Now API for real-time air quality index data and publishes
    AQI events to Ensign.
    """

    def __init__(
            self,
            topic="aqi-json",
            ensign_creds="cred/ensign_key.json",
            airnow_creds="cred/airnow_key.json",
            zipcode=94568,
            current_date=date.today(),
            # timezone="US/Pacific",
            interval=60,
    ):
        """
        Create a AqiPublisher to publish AQI json data to an Ensign topic. Nothing
        will be published until run() is called.

        Parameters
        ----------
        topic : str (default: "aqi-json")
            The name of the topic to publish to.

        ensign_creds : str (optional)
            The path to your Ensign credentials file. If not provided, credentials will
            be read from the ENSIGN_CLIENT_ID and ENSIGN_CLIENT_SECRET environment
            variables.

        airnow_creds : str (default: "cred/airnow_key.json")
            The path to your Airnow API credentials file. The credentials file must be a
            JSON file with the Airnow API key.

        zipcode : Int (default: 94568, Dublin CA)
            zipcode of the region you want to extract the AQI

        current_date : date (default: today's date)
            The date you want to fetch the data

        interval : int (default: 60)
            The number of seconds to wait between queries to the Airnow API.
        """
        self.topic = topic
        self.zipcode = zipcode,
        self.current_date = current_date,
        self.interval = interval
        self.ensign = Ensign(cred_path=ensign_creds)
        with open(airnow_creds) as f:
            self.airnow_creds = json.load(f)
            if (
                    "AQI_API_KEY" not in self.airnow_creds
            ):
                raise ValueError(
                    "Airnow API credentials must contain the API key"
                )

    def run(self):
        """
        Run the publisher forever.
        """
        asyncio.get_event_loop().run_until_complete(self.recv_and_publish())

    async def print_ack(self, ack):
        ts = datetime.fromtimestamp(ack.committed.seconds + ack.committed.nanos / 1e9)
        print(f"Event committed at {ts}")

    async def print_nack(nack):
        print(f"Event was not committed with error {nack.code}: {nack.error}")

    def compose_query(self):
        """

        :return:
        """
        url = f"https://www.airnowapi.org/aq/observation/zipCode/current/?format=application/json&zipCode={self.zipcode}&distance=25&API_KEY={self.airnow_creds}"
        return url

    def unpack_airnow_response(self, message):
        """
        Convert a message from the Airnow API to potentially multiple Ensign events,
        and yield each.

        Parameters
        ----------
        message : dict
            JSON formatted response from the Airnow API containing AQI details
        """
        aqi_events = message.get("features", None)
        if aqi_events is None:
            raise Exception(
                "unexpected response from Airnow request, no aqi-events found")
        for aqi_event in aqi_events:
            details = aqi_event.get("properties", None)
            if details is None:
                raise Exception(
                    "unable to parse airnow api response, no aqi-event details found")

            data = {
                "date_observed": details.get("DateObserved", None),
                "aqi_values": details.get("AQI", None)
            }

            yield Event(json.dumps(data).encode("utf-8"), mimetype="application/json")

    async def recv_and_publish(self):
        """
        Retrieve aqi json data from Airnow API and publish events to Ensign.
        """
        await self.ensign.ensure_topic_exists(self.topic)

        while True:
            # Call the Airnow API to get current date's AQI data.
            query = self.compose_query()
            response = requests.get(query).json()

            # unpack the API response and parse it into events
            events = self.unpack_airnow_response(response)
            for event in events:
                await self.ensign.publish(
                    self.topic,
                    event,
                    on_ack=self.print_ack,
                    on_nack=self.print_nack,
                )

            # sleep for a bit before we ping the API again
            await asyncio.sleep(self.interval)


if __name__ == "__main__":
    publisher = AqiPublisher(ensign_creds="cred/ensign_key.json")
    publisher.run()
