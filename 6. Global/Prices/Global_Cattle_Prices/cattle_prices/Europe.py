import requests
import os
import asyncio
import aiohttp

from datetime import datetime
from bs4 import BeautifulSoup, Tag

class CattlePricesEurope:
    def __init__(self, path: str, proxies: dict = None, all_dates: dict = None):
        path = os.path.join(path,"Europa")
        if not os.path.exists(path):
            os.makedirs(path)
        self.path = path

        if not proxies:
            self.proxies = {}
        else:
            self.proxies = proxies
        if not all_dates:
            self.all_dates = {"year": datetime.now().year}  # Replace with your logic to get current week info
        else:
            self.all_dates = all_dates

    @classmethod
    def get_Prices(cls, path: str, proxies: dict = None, all_dates: dict = None):
        service = cls(path, proxies, all_dates)
        asyncio.run(service.pega_eu_data_async())

    async def pega_eu_data_async(self):
        await asyncio.gather(
            self.pega_eu_data_async_present(),
            # self.pega_eu_data_async_past()
        )
        print('Europe data downloaded')

    async def pega_eu_data_async_present(self):
        year = self.all_dates["year"]
        host = f"https://agriculture.ec.europa.eu/data-and-analysis/markets/price-data/price-monitoring-sector/beef-carcases_en"
        await self.save_year(host, year)

    async def pega_eu_data_async_past(self):
        year = self.all_dates["year"]-1
        year_is_available = True

        while year_is_available:
            host = f"https://agriculture.ec.europa.eu/data-and-analysis/markets/price-data/price-monitoring-sector/beef-carcases/{year}_en"
            year_is_available = await self.save_year(host, year)
            year -= 1

    async def download_item(self, item: Tag, prefix: str, retries=0) -> bool:
        try:
            items_with_xlsx = tuple(filter(lambda x: "xlsx" in x.a.attrs.get("href") if x.a else False, item.children))
            if not items_with_xlsx:
                print(f"Skipping item due to missing xlsx: {prefix}")
                return False

            link = items_with_xlsx[0].a.attrs['href']
            name = items_with_xlsx[0].text

            if "https://agriculture.ec.europa.eu/" not in link:
                link = "https://agriculture.ec.europa.eu/" + link
            try:
                async with aiohttp.ClientSession() as client:
                    async with client.get(link, proxy=self.proxies.get('https')) as response:
                        content = await response.content.read()
                        folder_path = os.path.join(self.path, prefix)
                        if not os.path.exists(folder_path):
                            os.makedirs(folder_path)
                        file_path = os.path.join(folder_path ,f"{name}.xlsx")
                        with open(file_path, "wb") as file:
                            file.write(content)
            except Exception as error:
                if retries == 3:
                    raise error
                await asyncio.sleep(5)
                await self.download_item(item, prefix, retries + 1)

        except Exception as e:
            print(f"Unexpected error: {e}")
            raise e

        return True


    async def save_year(self, host: str, year: int) -> bool:
        response = requests.get(host, proxies=self.proxies)
        soup = BeautifulSoup(response.text, "html.parser")
        if str(year) not in soup.th.text:
            print(f"Error: expecting to find table header {year}, but got {soup.th.text}")
            return False
        else:
            print(f'Data for Europe about {year} downloaded')
        lista_de_links = soup.tbody

        if lista_de_links:
            futures = [
                self.download_item(item, str(year))
                for item in filter(
                    lambda item: isinstance(item, Tag),
                    lista_de_links.children
                )
            ]
            return all(await asyncio.gather(*futures))
        else:
            print(f"No table body found for {host}")
            return False


if __name__ == "__main__":
    CattlePricesEurope.get_Prices("./tmp")
