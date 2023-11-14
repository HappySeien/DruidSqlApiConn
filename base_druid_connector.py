import re
import json
import typing
import requests

from datetime import datetime


class BaseDruidConnector:
    """
    Базовый класс коннектор, для бд Druid.
    При наследовании от класса ОБЯЗАТЕЛЬНО в __init__ указать переменную SQL_DRUID_API_URL.
    Содержит 3 метода:
    ---> get_iso_time - Переводит дату из Epoch формата в ISO формат.
    ---> get_druid_data_from_sql - Делает запрос в Druid по sql api.
    ---> sql_inject_check - Проверяет полученные значения на содержание элементов
                            способных разорвать строку в sql запросе и внедрить 
                            вредоносный sql-код.
    """

    def __init__(self, SQL_DRUID_API_URL: str) -> None:
        
        if isinstance(SQL_DRUID_API_URL, str):
            self.SQL_DRUID_API_URL = SQL_DRUID_API_URL
        else:
            raise TypeError(f"Object SQL_DRUID_API_URL must be str, not {type(SQL_DRUID_API_URL)}")

    def get_iso_time(self, epoch_time: typing.Any) -> datetime.isoformat:
            """
            Переводит дату из Epoch формата в ISO формат.
            Принимает на вход дату в Epoch формате, строкой или числом.
            Возвращает обьект datetime в iso формате.
            """
            if isinstance(epoch_time, str):   
                date_time = datetime.isoformat(datetime.fromtimestamp(int(epoch_time/1000)))
                
            if isinstance(epoch_time, int):
                date_time = datetime.isoformat(datetime.fromtimestamp(epoch_time/1000))
            
            return date_time

    def get_druid_data_from_sql(self, query: str) -> list[dict]|str:
        """
        Делает запрос в Druid по sql api.
        Принимает на вход JSON строку, содержащую sql запрос.
        Возвращает обьект ответа от базы в формате list[dict],
        если запрос успешен, либо str содержащий ошибку.
        """
        url = self.SQL_DRUID_API_URL
        headers = {"Content-Type": "application/json"} 

        query_ = {
            "query": query,
            "resultFormat" : "object",
        }
        
        response = requests.post(url=url ,headers=headers, data=json.dumps(query_))
        druid_data = json.loads(response.text)

        if isinstance(druid_data, dict) and druid_data.get("error", None):
            return druid_data["error"]

        return druid_data
    
    def sql_inject_check(self, value: typing.Any) -> str|typing.Any:
        """
        Проверяет полученные значения на содержание элементов
        способных разорвать строку в sql запросе и внедрить 
        вредоносный sql-код.
        Принимает на вход value любого типа. Если переданный value
        является str, проверяет ее по шаблону.
        Возвращает обработанную строку, или обьект любого другого типа. 
        """
        if isinstance(value, str):
            regex = re.compile(
                r"\+|;|=|\<|\>|\||`|'|\"|/|\\", 
                re.IGNORECASE
            )
            match_ = re.search(regex, value)
            if match_:
                sub_string = re.sub(regex, "", value)
                return sub_string
        
        return value
