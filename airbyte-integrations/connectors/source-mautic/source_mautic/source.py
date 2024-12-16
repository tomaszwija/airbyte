
from distutils import core
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import BasicHttpAuthenticator
from airbyte_cdk.logger import AirbyteLogger

#connector specific imports
from requests.auth import HTTPBasicAuth
import re
from urllib.parse import urlparse
import json
import phpserialize
from datetime import datetime


class MauticStream(HttpStream):
    url_base = "https://example-api.com/v1/"

    def request_params(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {}

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield {}


class IncrementalMauticStream(MauticStream):
    _state: MutableMapping[str, Any] = {}
    state_checkpoint_interval = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @property
    def state(self) -> Mapping[str, Any]:
        return self._state
    
    @state.setter
    def state(self, value: Mapping[str, Any]):
        if value:
            if self.cursor_field in value:
                self._cursor_value = value[self.cursor_field]
        self._state = value or {}

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):
            state_cursor_value = self.state.get(self.cursor_field, "") or self.start_date
            record_cursor_value = record.get(self.cursor_field, "") or self.state.get(self.cursor_field)
            updated_state = {
                self.cursor_field: max(record_cursor_value, state_cursor_value)
            }
            self.state = updated_state
            yield record


class DoNotContactEvents(IncrementalMauticStream):
    cursor_field = "timestamp"
    primary_key = "eventId"
    page = 0
    include_events = 'lead.donotcontact'

    def __init__(self,start_date="",url_base="",**kwargs):
        super().__init__(**kwargs)
        self.url_base = url_base
        self.start_date = start_date
        self.limit = 10000

    def path(self, **kwargs) -> str:
        return "contacts/activity"
    
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        response_data = response.json()
        self.total_records = int(response_data["total"])
        if int(response_data["maxPages"]) > int(response_data["page"]):
            self.page+=1
            print(f"debugme: next_page_token: {self.page}")
            return {"page": self.page}
        else:
            return None
        
    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:

        params = {}

        if next_page_token:
            params.update(next_page_token)

        next_date = self.start_date
        stream_state = stream_state or {}
        print(f"debugme streamstate: {stream_state}")
        if stream_state.get(self.cursor_field) is not None:
            next_date = stream_state.get(self.cursor_field)

        params["filters[dateFrom]"] = next_date
        params["limit"] = self.limit
        params["orderBy"] = self.cursor_field
        params["orderByDir"] = "ASC"

        # add events to include
        i=0
        for event in self.include_events.split(","):
            params[f"filters[includeEvents][{i}]"] = event
            i+=1

        # first iteration add the page number (there's not default in the API)
        if "page" not in params:
            params["page"] = 0
        print(f"debugme requestparams: {params}")
        return params
    

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        print(f"debugme: {response.request.path_url}")
        print(f"debugme limit: {self.limit}")
        response_json = response.json()["events"]
        yield from response_json

    
# Source
class SourceMautic(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        logger.info("Checking Mautic API connection...")
        try:
            host = config["host"].strip(' /')
            username = config["username"]
            password = config["password"]
            response = requests.get(
                f"{host}/users/self", auth=HTTPBasicAuth(username=username, password=password)
            )
            if response.status_code == 200:
                return True, None
            else:
                return False, "Invalid Mautic API credentials, make sure that your user has API access"
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = BasicHttpAuthenticator(username=config["username"], password=config["password"])
        url_base = f'{config["host"].strip(" /")}/api'
       
        return [
            DoNotContactEvents(authenticator=auth,start_date=config['start_date'],url_base=url_base),
        ]
