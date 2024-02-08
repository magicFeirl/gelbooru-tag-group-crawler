import asyncio
import os
import re

from typing import Union

import aiofiles
from aiohttp import ClientSession
from lxml.html import fromstring
import nest_asyncio


nest_asyncio.apply()


PROXY = 'http://127.0.0.1:10809'


class GelTagGroupCrawler(object):
    def __init__(self, proxy: str = None) -> None:
        headers = {'user-agent': 'wasp'}

        self.session = ClientSession(headers=headers)
        self.proxy = proxy
        self.base_url = 'https://gelbooru.com/'

    async def __aexit__(self, *args):
        await self.session.close()

    async def __aenter__(self):
        return self

    async def _get(self, url, json=False, bin=False, **kwargs):
        async with self.session.get(url, proxy=self.proxy, **kwargs) as resp:
            if bin:
                content = await resp.read()
            elif json:
                content = await resp.json()
            else:
                content = await resp.text()

        return content

    async def get_wiki_page(self, id: str | int):
        url = f'https://gelbooru.com/index.php?page=wiki&s=&s=view&id={id}'

        html = await self._get(url)
        # with open('gel.html', 'w', encoding='utf-8') as f:
        #     f.write(html)

        # 获取 wiki 页下所有跳转到其它 wiki 的 url
        sel = fromstring(html, base_url=self.base_url).cssselect('body > div.padding15 > table a')
        for el in sel:
            text, href = el.text_content(), el.get('href')
            if '&search=' in href:
                yield (text, href)

    async def download_file(self, filename: str, url: str):
        async with aiofiles.open(filename, 'wb') as f:
            bin = await self._get(url, bin=True)
            await f.write(bin)

            print(filename, 'downloaded')

    async def search_tag(self, tag_name: list[str] | str, pid: int = 0, limit: int = 20, file_quality: Union['file_url', 'sample_url', 'preview_url'] = 'sample_url'):
        url = f'{self.base_url}index.php?page=dapi&s=post&q=index&json=1'
        tag_name = tag_name if isinstance(tag_name, str) else ' '.join(tag_name)

        data = await self._get(url, json=True, params={
            'tags': tag_name,
            'pid': pid,
            'limit': limit
        })
        
        count = data['@attributes']['count']

        if not count:
            return
        
        for idx, post in enumerate(data['post']):
            if file_quality in post and post[file_quality]:
                file_url = post[file_quality]
            else:
                file_url = post['file_url']

            ext = file_url.split('.')[-1]
            filename = f'{tag_name}_{idx}_{post["id"]}.{ext}'

            yield filename, file_url

    async def run(self, wiki_id: str | int, wiki_name: str, file_download_coro: int = 3, skip_exist: bool = True):
        if not os.path.isdir(wiki_name):
            os.mkdir(wiki_name)
      
        sem = asyncio.Semaphore(file_download_coro)

        async def download_worker(path, file_url):
            async with sem:
                await self.download_file(path, file_url)

        tasks = []
        async for tag, href in self.get_wiki_page(wiki_id):
            async for filename, file_url in self.search_tag(tag, limit=3):
                # 将不可使用的字符替换为下划线 - 其实应该放到 download_file 里面
                invalid_chars = r'[<>:"/\\|?*\x00-\x1F\x7F]'
                filename = re.sub(invalid_chars, '_', filename)
            
                path = os.path.join(wiki_name, filename)
                if skip_exist and os.path.isfile(path):
                    print(f'{filename} existed, skip')
                    continue

                tasks.append(asyncio.create_task(download_worker(path, file_url)))

        await asyncio.gather(*tasks)


async def main():
    async with GelTagGroupCrawler(proxy=PROXY) as g:
        await g.run(12311, 'tag_group-gestures')


if __name__ == '__main__':
    asyncio.run(main())