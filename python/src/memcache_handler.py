from pymemcache.client.base import Client
from config import MEMCACHED_CONFIG


def create_memcache_client():
    return Client((MEMCACHED_CONFIG["host"],MEMCACHED_CONFIG["port"])) 