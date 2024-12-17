
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
        self.limit = 5000

    def path(self, **kwargs) -> str:
        return "contacts/activity"
    
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        response_data = response.json()

        self.total_records = int(response_data["total"])
        if int(response_data["maxPages"]) > int(response_data["page"]):
            self.page+=1
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
        # print(f"debugme requestparams: {params}")
        return params
    

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()["events"]
        yield from response_json


class Contacts(IncrementalMauticStream):
    _state: MutableMapping[str, Any] = {}
    cursor_field = "dateAdded"
    alt_cursor_field = "dateModified"
    primary_key = "id"
    total_records = 0
    records_count = 0
    start = 0
    checkpointed_slices = []

    def __init__(self,start_date="",url_base="",**kwargs):
        super().__init__(**kwargs)
        self.url_base = url_base
        self.start_date = start_date
        self.limit = 2000

    @property
    def state(self) -> Mapping[str, Any]:
        return self._state
    
    @state.setter
    def state(self, value: Mapping[str, Any]):
        # print (f"debugme STATE {value}")

        # if self.state is None:
        start_state = {
            self.cursor_field: self.start_date,
            self.alt_cursor_field: self.start_date
        }

        if value:
            if self.cursor_field in value:
                self._cursor_value = value[self.cursor_field]
            if self.alt_cursor_field in value:
                self.alt_cursor_field_current_stream_value = value[self.alt_cursor_field]
        self._state = value or start_state

    def path(self, **kwargs) -> str:
        return "contacts"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        response_data = response.json()
        self.total_records = int(response_data["total"])

        # print(f"debugme: Pagination current start: {self.start}")

        if int(response_data["total"]) >= self.start:
            self.start+=self.limit
            # print(f"debugme: Pagination next start: {self.start}")
            return {"start": self.start}
        else:
            return None


    def stream_slices(self, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:

        next_dateModified = self.start_date
        next_dateAdded = self.start_date
        alt_cursor_field_snake_case = re.sub(r'(?<!^)(?=[A-Z])', '_', self.alt_cursor_field).lower()
        cursor_field_snake_case = re.sub(r'(?<!^)(?=[A-Z])', '_', self.cursor_field).lower()
        stream_state = stream_state or {}
        next_dateAdded = stream_state.get(self.cursor_field, self.start_date)
        next_dateModified = stream_state.get(self.alt_cursor_field, self.start_date)

        slices = []

        where = [
            # updated
            {
                'where[0][col]': 'dateModified',
                'where[0][expr]': 'gte',
                'where[0][val]': next_dateModified,
            },
            #new users
            {
                'where[0][col]': 'dateAdded',
                'where[0][expr]': 'gte',
                'where[0][val]': next_dateAdded,
                'where[1][col]': 'dateModified',
                'where[1][expr]': 'isNull',
            }
        ]

        for where_clause in where:
            where_clause_params = {}
            order_by_params = {}
            order_by_params["orderByDir"] = "ASC"
            for key,val in where_clause.items():
                where_clause_params[key] = val

                # it's super important to have the correct order for incremental load
                # because the state is coming from the stream and save the last row as the state
                # therefore we set the correct field in each slice to the orderBy
                if key == 'where[0][col]' and val == 'dateModified':
                    order_by_params["orderBy"] = alt_cursor_field_snake_case
                if key == 'where[0][col]' and val == 'dateAdded':
                    order_by_params["orderBy"] = cursor_field_snake_case
            merged_params = {**where_clause_params,**order_by_params}
            slices.append(merged_params)

  

        for slice in slices:
            self.start = 0 # reset pagination for each slice
            yield slice
                

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:

        params = super().request_params(stream_state,stream_slice,next_page_token)
        params.update(stream_slice)
        if next_page_token:
            params.update(next_page_token)
        params['limit'] = self.limit
        return params


    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        url = response.request.path_url
        # print(f'debugme URL : {url}')
        response_json = response.json()
        
        if not "start" in url:
            print("Total records: ",response_json["total"])
        response_dict = []
        for contact in response_json['contacts']:
            data = {}
            custom_fields = {}
            fields = response_json['contacts'][contact]['fields']

            for field_name in fields['all']:
                data[field_name] = fields['all'][field_name]

            base_fields = response_json['contacts'][contact]
            for field in base_fields:
                if field != 'fields':
                    data[field] = base_fields[field]

            # # add custom fields as a json
            # for field_name in fields['core']:
            #     if fields['core'][field_name]['is_fixed'] == '0':
            #         custom_fields[field_name] = fields['core'][field_name]['value']

            # data['custom_fields'] = custom_fields

            # add updated_at
            data['updated_at'] = max(data['dateAdded'],data['dateModified'] or '1970-01-01 00:00:00')
            try:
                if data['date_of_birth'] == '0000-00-00':
                    data['date_of_birth'] = '1970-01-01 00:00:00'
            except Exception as e:
                pass

            # fixes mismatched schemas (tech debt I'm unwilling to fix as it requires table schema changes)
            data["createdBy"] = str(data.get("createdBy", ""))
            data["modifiedBy"] = str(data.get("modifiedBy", ""))

            # also fixes our dumb idea of having multiple apps in mautic
            if "app" in data:
                del data["app"]

            response_dict.append(data)
       
        yield from response_dict


    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):
            current_alt_cursor_value = self.state.get(self.alt_cursor_field, self.start_date) or self.start_date
            current_cursor_value = self.state.get(self.cursor_field, "") or self.start_date
            
            # Get record-specific values
            alt_cursor_value = record.get(self.alt_cursor_field, "") or self.state.get(self.alt_cursor_field) or self.start_date
            cursor_value = record.get(self.cursor_field, "") or self.state.get(self.cursor_field) or self.start_date
            
            # Distinguish updates for the two slices
            updated_state = self.state.copy()

            if record.get(self.alt_cursor_field):  # Only update alt_cursor_field if it exists (Slice 1)
                updated_state[self.alt_cursor_field] = max(alt_cursor_value, current_alt_cursor_value)
            
            if record.get(self.cursor_field) and not record.get(self.alt_cursor_field):  # Slice 2 logic
                updated_state[self.cursor_field] = max(cursor_value, current_cursor_value)

            self.state = updated_state
            yield record

    
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
            Contacts(authenticator=auth,start_date=config['start_date'],url_base=url_base),
        ]