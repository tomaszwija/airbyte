from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple
from datetime import datetime, timedelta
import requests
import pandas as pd
import io
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
import numpy as np

class AdyenAuth:
    def __init__(self, token: str):
        self.token = token

    def get_auth_header(self) -> Mapping[str, Any]:
        return {"X-API-KEY": self.token}


class AdyenStream(HttpStream):
    url_base = "https://ca-live.adyen.com/reports/download/"
    raise_on_http_errors = False  # Allows handling 404 errors gracefully

    def __init__(self, authenticator: AdyenAuth, start_date: str, report_type: str, company_account: str, merchant_account: str, report_name: str, start_batch: int = 300):
        super().__init__(authenticator=authenticator)
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
        self.report_name = report_name
        if report_name == 'settlement_details_report':
            self._cursor_value = start_batch
        else:
            self._cursor_value = self.start_date
        self.report_type = report_type  # Accepts "company" or "merchant"
        self.company_account = company_account
        self.merchant_account = merchant_account
        self.start_batch = start_batch

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
        
        if self.report_name == 'settlement_details_report':
            current_state_value = current_stream_state.get(self.cursor_field, self.start_batch)
            latest_record_date = max(current_state_value, self._cursor_value)
            return {self.cursor_field: latest_record_date}
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
            df = df.apply(lambda col: col.fillna("").astype(str) if col.name != 'Amount' else col)
            df = df.fillna(np.nan)

            if not df.empty:
                self._cursor_value = max(pd.to_datetime(df["Creation_Date"]))
            for _, row in df.iterrows():
                row_dict = {key: (None if pd.isna(value) else value) for key, value in row.to_dict().items()}
                yield row_dict
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
            df = df.apply(lambda col: col.fillna("").astype(str) if col.name != 'Dispute_Amount' else col)
            df = df.fillna(np.nan)

            if not df.empty:
                self._cursor_value = max(pd.to_datetime(df["Record_Date"]))

            for _, row in df.iterrows():
                row_dict = {key: (None if pd.isna(value) else value) for key, value in row.to_dict().items()}
                yield row_dict
        else:
            response.raise_for_status()


class ExchangeRateReport(IncrementalAdyenStream):
    cursor_field = "Valid_From"
    primary_key = ["Valid_From", "Base_Currency", "Target_Currency"]

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        date_str = stream_slice["Valid_From"]
        account_type = "Company" if self.report_type == "company" else "MerchantAccount"
        account_name = self.company_account if self.report_type == "company" else self.merchant_account
        return f"{account_type}/{account_name}/exchange_rate_report_{date_str}.csv"

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        stream_state = stream_state or {}
        start_date = datetime.strptime(stream_state.get(self.cursor_field, self.start_date.strftime("%Y-%m-%d %H:%M:%S")), "%Y-%m-%d %H:%M:%S")
        end_date = datetime.now()

        while start_date <= end_date:
            yield {"Valid_From": start_date.strftime("%Y_%m_%d")}
            start_date += timedelta(days=1)

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        if response.status_code == 404:
            self.logger.info("Data not available for this date")
            return []
        elif response.status_code == 200:
            # we need to skip first 4 rows as it's an "about these rates text"
            df = pd.read_csv(io.BytesIO(response.content), skiprows=4)
            df.columns = df.columns.str.replace(" ", "_")
            # drop description field
            df = df.drop(columns=["About_these_rates", "Symbol"], errors="ignore")
            numeric_columns = ('Exponent', 'Exchange_Rate')
            # convert to timezone-aware datetime
            df["Valid_From"] = pd.to_datetime(df["Valid_From"]).apply(
                lambda x: x.tz_convert(None) if x.tzinfo else x.tz_localize('UTC').tz_convert(None)
            )
            df["TimeZone"] = "UTC"
            df = df.apply(lambda col: col.fillna("").astype(str) if col.name not in numeric_columns else col)
            df = df.fillna(np.nan)

            if not df.empty:
                self._cursor_value = max(pd.to_datetime(df["Valid_From"]))

            for _, row in df.iterrows():
                row_dict = {key: (None if pd.isna(value) else value) for key, value in row.to_dict().items()}
                yield row_dict
        else:
            response.raise_for_status()


class PaymentsAccountingReport(IncrementalAdyenStream):
    cursor_field = "Booking_Date"
    primary_key = ["Booking_Date", "Psp_Reference"]

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        date_str = stream_slice["Booking_Date"]
        account_type = "Company" if self.report_type == "company" else "MerchantAccount"
        account_name = self.company_account if self.report_type == "company" else self.merchant_account
        return f"{account_type}/{account_name}/payments_accounting_report_{date_str}.csv"

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        stream_state = stream_state or {}
        start_date = datetime.strptime(stream_state.get(self.cursor_field, self.start_date.strftime("%Y-%m-%d %H:%M:%S")), "%Y-%m-%d %H:%M:%S")
        end_date = datetime.now()

        while start_date <= end_date:
            yield {"Booking_Date": start_date.strftime("%Y_%m_%d")}
            start_date += timedelta(days=1)

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        if response.status_code == 404:
            self.logger.info("Data not available for this date")
            return []
        elif response.status_code == 200:
            df = pd.read_csv(io.BytesIO(response.content))
            numeric_columns = (
                'Main Amount', 'Received (PC)', 'Authorised (PC)', 
                'Captured (PC)', 'Payable (SC)', 'Commission (SC)', 
                'Markup (SC)', 'Scheme Fees (SC)', 'Interchange (SC)',
                'Processing Fee (FC)' 
            )
            df = df.apply(lambda col: col.fillna("").astype(str) if col.name not in numeric_columns else col)
            df = df.fillna(np.nan)
            df.columns = df.columns.str.replace(" ", "_")
            if not df.empty:
                self._cursor_value = max(pd.to_datetime(df["Booking_Date"]))

            for _, row in df.iterrows():
                row_dict = {key: (None if pd.isna(value) else value) for key, value in row.to_dict().items()}
                yield row_dict
        else:
            response.raise_for_status()


class SettlementDetailReport(IncrementalAdyenStream):
    cursor_field = "Batch_Number"
    primary_key = ["Batch_Number", "Psp_Reference"]

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        batch_number = stream_slice["Batch_Number"]
        account_type = "Company" if self.report_type == "company" else "MerchantAccount"
        account_name = self.company_account if self.report_type == "company" else self.merchant_account
        return f"{account_type}/{account_name}/settlement_detail_report_batch_{batch_number}.csv"

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        stream_state = stream_state or {}
        current_batch = stream_state.get(self.cursor_field, self.start_batch)
        end_batch = self.determine_end_batch(current_batch)

        for batch_number in range(current_batch, end_batch + 1):
            yield {"Batch_Number": batch_number}

    def determine_end_batch(self, current_batch: int) -> int:
        fixed_batch = 470
        dynamic_batch = current_batch + 5
        return max(fixed_batch, dynamic_batch)

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        if response.status_code == 404:
            self.logger.info("Data not available for this batch number")
            return []
        elif response.status_code == 200:
            df = pd.read_csv(io.BytesIO(response.content))
            numeric_columns = (
                'Gross Debit (GC)', 'Gross Credit (GC)', 'Exchange Rate', 
                'Net Debit (NC)', 'Net Credit (NC)', 'Commission (NC)', 
                'Markup (NC)', 'Scheme Fees (NC)', 'Interchange (NC)',
                'Batch Number' 
            )
            df = df.apply(lambda col: col.fillna("").astype(str) if col.name not in numeric_columns else col)
            df = df.fillna(np.nan)
            df.columns = df.columns.str.replace(" ", "_")

            if not df.empty:
                self._cursor_value = max(df["Batch_Number"])

            for _, row in df.iterrows():
                row_dict = {key: (None if pd.isna(value) else value) for key, value in row.to_dict().items()}
                yield row_dict
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
        start_batch = config["settlement_details_starting_batch"]
        return [
            ReceivedPaymentsReport(
                authenticator=auth, start_date=start_date, report_type="merchant",
                company_account=company_account, merchant_account=merchant_account,
                report_name='received_payments_report'
            ),
            DisputeReport(
                authenticator=auth, start_date=start_date, report_type="company",
                company_account=company_account, merchant_account=merchant_account,
                report_name='dispute_report'
            ),
            ExchangeRateReport(
                authenticator=auth, start_date=start_date, report_type="company",
                company_account=company_account, merchant_account=merchant_account,
                report_name='exchange_rate_report'
            ),
            PaymentsAccountingReport(
                authenticator=auth, start_date=start_date, report_type="merchant",
                company_account=company_account, merchant_account=merchant_account,
                report_name='payments_accounting_report'
            ),
            SettlementDetailReport(
                authenticator=auth, start_date=start_date, report_type="merchant",
                company_account=company_account, merchant_account=merchant_account,
                report_name='settlement_details_report',
                start_batch=start_batch
            )
        ]
