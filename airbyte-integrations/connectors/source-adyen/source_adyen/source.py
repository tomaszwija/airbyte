from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from datetime import datetime, timedelta
import requests
import pandas as pd
import io
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream


class AdyenAuth:
    def __init__(self, token: str):
        self.token = token

    def get_auth_header(self) -> Mapping[str, Any]:
        return {"X-API-KEY": self.token}


class AdyenStream(HttpStream):
    url_base = "https://ca-live.adyen.com/reports/download/"
    raise_on_http_errors = False  # Allows handling 404 errors gracefully

    def __init__(self, authenticator: AdyenAuth, start_date: str, report_type: str, company_account: str, merchant_account: str):
        super().__init__(authenticator=authenticator)
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
        self._cursor_value = self.start_date
        self.report_type = report_type  # Accepts "company" or "merchant"
        self.company_account = company_account
        self.merchant_account = merchant_account

    def request_params(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {}

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None
    
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield {}


class IncrementalAdyenStream(AdyenStream):
    cursor_field = "Creation_Date"  # Override in subclass if required
    state_checkpoint_interval = None

    @property
    def state(self) -> Mapping[str, Any]:
        if self._cursor_value:
            return {self.cursor_field: self._cursor_value.strftime("%Y-%m-%d %H:%M:%S")}
        else:
            return {self.cursor_field: self.start_date.strftime("%Y-%m-%d %H:%M:%S")}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        if value and self.cursor_field in value:
            self._cursor_value = datetime.strptime(value[self.cursor_field], "%Y-%m-%d %H:%M:%S")

    def get_updated_state(
        self, current_stream_state: Mapping[str, Any], latest_record: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        # Parse current state or use start date
        current_state_value = current_stream_state.get(self.cursor_field, self.start_date.strftime("%Y-%m-%d %H:%M:%S"))
        current_parsed_date = datetime.strptime(current_state_value, "%Y-%m-%d %H:%M:%S")
        
        # Update to latest cursor value if available, else retain the last known state
        latest_record_date = max(current_parsed_date, self._cursor_value) if self._cursor_value else current_parsed_date
        
        return {self.cursor_field: latest_record_date.strftime("%Y-%m-%d %H:%M:%S")}


class ReceivedPaymentsReport(IncrementalAdyenStream):
    cursor_field = "Creation_Date"
    primary_key = ["Psp_Reference", "Creation_Date"]

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        date_str = stream_slice["Creation_Date"]
        account_type = "MerchantAccount" if self.report_type == "merchant" else "Company"
        account_name = self.merchant_account if self.report_type == "merchant" else self.company_account
        return f"{account_type}/{account_name}/received_payments_report_{date_str}.csv"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        if response.status_code == 404:
            self.logger.info("Data not available for this date")
            return []
        elif response.status_code == 200:
            df = pd.read_csv(io.BytesIO(response.content))
            df.columns = df.columns.str.replace(" ", "_")
            if not df.empty:
                self._cursor_value = max(pd.to_datetime(df["Creation_Date"]))
            for _, row in df.iterrows():
                yield row.to_dict()
        else:
            response.raise_for_status()

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        stream_state = stream_state or {}
        start_date = datetime.strptime(stream_state.get(self.cursor_field, self.start_date.strftime("%Y-%m-%d %H:%M:%S")), "%Y-%m-%d %H:%M:%S")
        end_date = datetime.now()

        while start_date <= end_date:
            yield {"Creation_Date": start_date.strftime("%Y_%m_%d")}
            start_date += timedelta(days=1)


class DisputeReport(IncrementalAdyenStream):
    cursor_field = "Record_Date"
    primary_key = ["Dispute_PSP_Reference", "Record_Date"]

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        date_str = stream_slice["Record_Date"]
        account_type = "Company" if self.report_type == "company" else "MerchantAccount"
        account_name = self.company_account if self.report_type == "company" else self.merchant_account
        return f"{account_type}/{account_name}/dispute_report_{date_str}.csv"

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        stream_state = stream_state or {}
        start_date = datetime.strptime(stream_state.get(self.cursor_field, self.start_date.strftime("%Y-%m-%d %H:%M:%S")), "%Y-%m-%d %H:%M:%S")
        end_date = datetime.now()

        while start_date <= end_date:
            yield {"Record_Date": start_date.strftime("%Y_%m_%d")}
            start_date += timedelta(days=1)

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        if response.status_code == 404:
            self.logger.info("Data not available for this date")
            return []
        elif response.status_code == 200:
            df = pd.read_csv(io.BytesIO(response.content))
            df.columns = df.columns.str.replace(" ", "_")
            
            # Let's not store iban and bic
            df = df.drop(columns=["Iban", "Bic"], errors="ignore")

            if not df.empty:
                self._cursor_value = max(pd.to_datetime(df["Record_Date"]))

            for _, row in df.iterrows():
                yield row.to_dict()
        else:
            response.raise_for_status()


class SourceAdyen(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, Any]:
        try:
            auth = AdyenAuth(token=config["api_token"])
            headers = auth.get_auth_header()
            yesterday_date = (datetime.now() - timedelta(days=1)).strftime("%Y_%m_%d")
            company_account = config["company_account"]
            test_url = f"https://ca-live.adyen.com/reports/download/Company/{company_account}/exchange_rate_report_{yesterday_date}.csv"
            response = requests.get(test_url, headers=headers)

            if response.status_code == 200:
                return True, None
            elif response.status_code == 404:
                logger.info("No data available for the test date; assuming API is accessible.")
                return True, None
            else:
                return False, f"Failed with status code {response.status_code}"
        except Exception as e:
            return False, str(e)

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = AdyenAuth(token=config["api_token"])
        start_date = config["start_date"]
        company_account = config["company_account"]
        merchant_account = config["merchant_account"]
        
        return [
            ReceivedPaymentsReport(
                authenticator=auth, start_date=start_date, report_type="merchant",
                company_account=company_account, merchant_account=merchant_account
            ),
            DisputeReport(
                authenticator=auth, start_date=start_date, report_type="company",
                company_account=company_account, merchant_account=merchant_account
            )
        ]
