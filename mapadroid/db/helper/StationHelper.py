from typing import Optional, List

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from mapadroid.db.model import Station
from mapadroid.utils.DatetimeWrapper import DatetimeWrapper


class StationHelper:
    @staticmethod
    async def get(session: AsyncSession, station_id: str) -> Optional[Station]:
        stmt = select(Station).where(Station.id == station_id)
        result = await session.execute(stmt)
        return result.scalars().first()

    @staticmethod
    async def get_changed_since(session: AsyncSession, _timestamp: int) -> List[Station]:
        stmt = select(Station).where(Station.updated > DatetimeWrapper.fromtimestamp(_timestamp))
        result = await session.execute(stmt)
        return result.scalars().all()
