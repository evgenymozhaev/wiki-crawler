import asyncio
import re

from aiohttp import ClientSession, ClientConnectorError
from sqlalchemy import Column, Integer, String, create_engine, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


async def get_pages(current_page, current_depth, http_session, db_session):
    if current_depth == 6:
        return
    try:
        tasks = []
        async with http_session.get(URL_BASE + current_page.url) as response:
            if response.status == 200:
                response = await response.text()
                urls = re.findall('<a href="/wiki/([^"#]+)"', response, flags=re.U | re.DOTALL)
                urls = set(urls)
                for url in urls:
                    page = Page(url=url, request_depth=current_depth + 1)
                    db_session.add(page)
                    db_session.commit()

                    relation = Relation(from_page_id=current_page.id, link_id=page.id)
                    db_session.add(relation)
                    db_session.commit()

                    task = asyncio.ensure_future(get_pages(page,
                                                           current_depth+1,
                                                           http_session,
                                                           db_session))
                    tasks.append(task)
        await asyncio.gather(*tasks)
    except (ClientConnectorError, asyncio.TimeoutError):
        return


async def main():
    url = 'Заглавная_страница'
    depth = 0

    current_page = Page(url=url, request_depth=depth)
    Session = sessionmaker(bind=engine)
    db_session = Session()
    db_session.add(current_page)
    db_session.commit()

    async with ClientSession() as http_session:
        await get_pages(current_page, depth, http_session, db_session)

    db_session.close()


if __name__ == '__main__':
    URL_BASE = 'https://ru.wikipedia.org/wiki/'

    Base = declarative_base()

    class Relation(Base):
        __tablename__ = 'relations'
        from_page_id = Column(Integer, ForeignKey('pages.id'), primary_key=True)
        link_id = Column(Integer, ForeignKey('pages.id'), primary_key=True)

    class Page(Base):
        __tablename__ = 'pages'
        id = Column(Integer, primary_key=True, autoincrement=True)
        url = Column(String(500))
        request_depth = Column(Integer)

    engine = create_engine('sqlite:///wiki.db')
    Base.metadata.create_all(engine)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
