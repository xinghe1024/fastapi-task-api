from redis.asyncio import Redis

from collections.abc import Callable
from random import randint

from pydantic import ValidationError

from models import TaskResponse

from dataclasses import dataclass


TASK_NOT_FOUND_CACHE_MARKER = "__task_not_found__"


@dataclass(frozen=True)
class TaskCacheLookup:
    cache_hit: bool
    task: TaskResponse | None

class TaskCache:
    def __init__(
            self,
            redis_client: Redis,
            ttl_seconds: int,
            negative_ttl_seconds: int = 30,
            ttl_jitter_seconds: int = 0,
            jitter_generator: Callable[
                [int, int],
                int,
            ] = randint,
    ) -> None:
        if ttl_seconds < 1:
            raise ValueError(
                "ttl_seconds must be at least 1",
            )

        if negative_ttl_seconds < 1:
            raise ValueError(
                "negative_ttl_seconds must be at least 1",
            )

        if ttl_jitter_seconds < 0:
            raise ValueError(
                "ttl_jitter_seconds cannot be negative",
            )

        self._ttl_jitter_seconds = (
            ttl_jitter_seconds
        )
        self._jitter_generator = jitter_generator

        self._redis_client = redis_client
        self._ttl_seconds = ttl_seconds
        self._negative_ttl_seconds = (
            negative_ttl_seconds
        )

    @staticmethod
    def _build_key(
        owner_id: int,
        task_id: int,
    ) -> str:
        return f"task:{owner_id}:{task_id}"

    async def get_task(
            self,
            owner_id: int,
            task_id: int,
    ) -> TaskResponse | None:
        lookup = await self.lookup_task(
            owner_id=owner_id,
            task_id=task_id,
        )

        return lookup.task

    def _calculate_positive_ttl(self) -> int:
        jitter_seconds = self._jitter_generator(
            0,
            self._ttl_jitter_seconds,
        )

        return self._ttl_seconds + jitter_seconds


    async def set_task(
        self,
        owner_id: int,
        task: TaskResponse,
    ) -> None:
        cache_key = self._build_key(
            owner_id,
            task.id,
        )

        await self._redis_client.set(
            cache_key,
            task.model_dump_json(),
            ex=self._calculate_positive_ttl(),
        )

    async def delete_task(
        self,
        owner_id: int,
        task_id: int,
    ) -> None:
        cache_key = self._build_key(
            owner_id,
            task_id,
        )

        await self._redis_client.delete(
            cache_key,
        )

    async def lookup_task(
            self,
            owner_id: int,
            task_id: int,
    ) -> TaskCacheLookup:
        cache_key = self._build_key(
            owner_id,
            task_id,
        )
        cached_value = await self._redis_client.get(
            cache_key,
        )

        if cached_value is None:
            return TaskCacheLookup(
                cache_hit=False,
                task=None,
            )

        if cached_value == TASK_NOT_FOUND_CACHE_MARKER:
            return TaskCacheLookup(
                cache_hit=True,
                task=None,
            )

        try:
            task = TaskResponse.model_validate_json(
                cached_value,
            )
        except ValidationError:
            await self._redis_client.delete(
                cache_key,
            )
            return TaskCacheLookup(
                cache_hit=False,
                task=None,
            )

        return TaskCacheLookup(
            cache_hit=True,
            task=task,
        )

    async def set_task_not_found(
            self,
            owner_id: int,
            task_id: int,
    ) -> None:
        cache_key = self._build_key(
            owner_id,
            task_id,
        )

        await self._redis_client.set(
            cache_key,
            TASK_NOT_FOUND_CACHE_MARKER,
            ex=self._negative_ttl_seconds,
        )

