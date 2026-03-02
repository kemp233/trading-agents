"""StateWriter — 单写队列 + WAL 模式 (v3: 解决 SQLite 并发问题)"""
import asyncio
import sqlite3
from typing import Callable, Any


class StateWriter:
    """所有数据库写操作的唯一入口
    
    设计原则:
    - 单写多读: 所有写操作走同一个队列
    - WAL 模式: 允许读写并发
    - 明确事务边界: 尤其是 Outbox 原子写入
    """

    def __init__(self, db_path: str):
        self._queue: asyncio.Queue = asyncio.Queue()
        self._db_path = db_path
        self._db: sqlite3.Connection | None = None
        self._running = False

    async def start(self):
        self._db = sqlite3.connect(self._db_path)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA busy_timeout=5000")
        self._db.row_factory = sqlite3.Row
        self._running = True
        asyncio.create_task(self._writer_loop())

    async def stop(self):
        self._running = False
        if self._db:
            self._db.close()

    async def write(self, fn: Callable[[sqlite3.Connection], Any]) -> Any:
        """将写操作放入队列，等待结果"""
        future: asyncio.Future = asyncio.get_event_loop().create_future()
        await self._queue.put((fn, future))
        return await future

    async def _writer_loop(self):
        """单一写协程，顺序执行所有写操作"""
        while self._running:
            try:
                fn, future = await asyncio.wait_for(
                    self._queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue

            try:
                result = fn(self._db)
                self._db.commit()
                if not future.done():
                    future.set_result(result)
            except Exception as e:
                self._db.rollback()
                if not future.done():
                    future.set_exception(e)

    def get_reader(self) -> sqlite3.Connection:
        """获取只读连接（可多个并发读）"""
        conn = sqlite3.connect(self._db_path, uri=True)
        conn.execute("PRAGMA query_only=ON")
        conn.row_factory = sqlite3.Row
        return conn
