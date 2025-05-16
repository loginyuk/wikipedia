from typing import List, Dict, Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from datetime import datetime, timedelta


class CassandraClient:
    def __init__(self, host, port, keyspace):
        self.host = host
        self.port = port
        self.keyspace = keyspace
        self.session = None

    def connect(self):
        cluster = Cluster([self.host], port=self.port)
        self.session = cluster.connect(self.keyspace)
        self.session.row_factory = dict_factory

    def execute(self, query, params=None):
        return self.session.execute(query, params) if params else self.session.execute(query)

    def close(self):
        if self.session:
            self.session.shutdown()



class DomainStat(BaseModel):
    time_start: str
    time_end: str
    statistics: List[Dict[str, int]]

class BotStat(BaseModel):
    domain: str
    created_by_bots: int

class BotStatsResponse(BaseModel):
    time_start: str
    time_end: str
    statistics: List[BotStat]

class TopUser(BaseModel):
    user_id: str
    user_name: str
    page_count: int
    page_titles: List[str]

class TopUsersResponse(BaseModel):
    time_start: str
    time_end: str
    top_users: List[TopUser]



app = FastAPI()
client = CassandraClient(host="cassandra-node", port=9042, keyspace="wiki_keyspace")
client.connect()



def get_time_bounds():
    now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    end_time = now - timedelta(hours=1)
    start_time = end_time - timedelta(hours=6)
    return start_time, end_time


# request 1
@app.get("/category-a/domain-hourly-stats", response_model=List[DomainStat])
def get_domain_hourly_stats():
    start_time, end_time = get_time_bounds()
    query = """
        SELECT hour_bucket, domain, page_count
        FROM domain_hourly_stats
        WHERE hour_bucket >= %s AND hour_bucket < %s
        ALLOW FILTERING
    """
    rows = client.execute(query, (start_time, end_time))

    buckets = {}
    for row in rows:
        hour = row["hour_bucket"]
        if hour not in buckets:
            buckets[hour] = []
        buckets[hour].append({row["domain"]: row["page_count"]})

    result = []
    for hour in sorted(buckets.keys()):
        result.append(DomainStat(
            time_start=hour.strftime("%H:%M"),
            time_end=(hour + timedelta(hours=1)).strftime("%H:%M"),
            statistics=buckets[hour]
        ))

    return result


# request 2
@app.get("/category-a/bot-creation-stats", response_model=BotStatsResponse)
def get_bot_creation_stats():
    start_time, end_time = get_time_bounds()
    time_range = f"{start_time.isoformat()}/{end_time.isoformat()}"

    query = """
        SELECT domain, created_by_bots
        FROM bot_creation_stats
        WHERE time_range = %s
    """
    rows = client.execute(query, (time_range,))
    statistics = [BotStat(**row) for row in rows]

    return BotStatsResponse(
        time_start=start_time.strftime("%H:%M"),
        time_end=end_time.strftime("%H:%M"),
        statistics=statistics
    )


# request 3
@app.get("/category-a/top-users", response_model=TopUsersResponse)
def get_top_users():
    start_time, end_time = get_time_bounds()
    time_range = f"{start_time.isoformat()}/{end_time.isoformat()}"

    query = """
        SELECT user_id, user_name, page_count, page_titles
        FROM top_users
        WHERE time_range = %s
        LIMIT 20
    """
    rows = client.execute(query, (time_range,))
    top_users = [TopUser(**row) for row in rows]

    return TopUsersResponse(
        time_start=start_time.strftime("%H:%M"),
        time_end=end_time.strftime("%H:%M"),
        top_users=top_users
    )
