from typing import List, Dict, Optional
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from datetime import datetime, timedelta

class CassandraClient:
    def __init__(self, host: str, port: int, keyspace: str):
        self.host = host
        self.port = port
        self.keyspace = keyspace
        self.session = None

    def connect(self):
        cluster = Cluster([self.host], port=self.port)
        self.session = cluster.connect(self.keyspace)
        self.session.row_factory = dict_factory

    def execute(self, query: str, params: Optional[tuple] = None):
        if params:
            return self.session.execute(query, params)
        return self.session.execute(query)

    def close(self):
        if self.session:
            self.session.shutdown()

# pydantic  models for category A
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

# pydantic models for category B
class PageInfo(BaseModel):
    page_id: int
    title: str
    domain: str
    timestamp: datetime

class PageDetail(BaseModel):
    page_id: int
    domain: str
    is_bot: bool
    timestamp: datetime
    title: str
    user_id: int
    user_name: str

class ActiveUser(BaseModel):
    user_id: int
    user_name: str
    page_count: int

app = FastAPI()
client = CassandraClient(host="cassandra-node", port=9042, keyspace="wiki_keyspace")
client.connect()

def get_time_bounds():
    now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    end_time = now - timedelta(hours=1)
    start_time = end_time - timedelta(hours=6)
    return start_time, end_time

# ─── Category A ───────
@app.get("/category-a/domain-hourly-stats", response_model=List[DomainStat])
def get_domain_hourly_stats():
    start_time, end_time = get_time_bounds()
    query = (
        "SELECT hour_bucket, domain, page_count "
        "FROM domain_hourly_stats "
        "WHERE hour_bucket >= %s AND hour_bucket < %s ALLOW FILTERING"
    )
    rows = client.execute(query, (start_time, end_time))
    buckets: Dict[datetime, List[Dict[str, int]]] = {}
    for row in rows:
        hour = row["hour_bucket"]
        buckets.setdefault(hour, []).append({row["domain"]: row["page_count"]})
    return [
        DomainStat(
            time_start=hour.strftime("%H:%M"),
            time_end=(hour + timedelta(hours=1)).strftime("%H:%M"),
            statistics=buckets[hour]
        )
        for hour in sorted(buckets.keys())
    ]

@app.get("/category-a/bot-creation-stats", response_model=BotStatsResponse)
def get_bot_creation_stats():
    start_time, end_time = get_time_bounds()
    query = (
        "SELECT domain, created_by_bots "
        "FROM bot_creation_stats "
        "WHERE time_start = %s AND time_end = %s"
    )
    rows = client.execute(query, (start_time, end_time))
    stats = [BotStat(**row) for row in rows]
    return BotStatsResponse(
        time_start=start_time.strftime("%H:%M"),
        time_end=end_time.strftime("%H:%M"),
        statistics=stats
    )

@app.get("/category-a/top-users", response_model=TopUsersResponse)
def get_top_users():
    start_time, end_time = get_time_bounds()
    query = (
        "SELECT user_id, user_name, page_count, page_titles "
        "FROM top_users "
        "WHERE time_start = %s AND time_end = %s LIMIT 20"
    )
    rows = client.execute(query, (start_time, end_time))
    users = [TopUser(**row) for row in rows]
    return TopUsersResponse(
        time_start=start_time.strftime("%H:%M"),
        time_end=end_time.strftime("%H:%M"),
        top_users=users
    )

# ─── Category B ─────
@app.get("/category-b/domains", response_model=List[str])
def list_domains():
    query = "SELECT domain FROM wiki_row"
    rows = client.execute(query)
    return sorted({row["domain"] for row in rows})

@app.get("/category-b/pages-by-user/{user_id}", response_model=List[PageInfo])
def pages_by_user(user_id: int):
    rows = client.execute("SELECT page_id, title, domain, timestamp, user_id FROM wiki_row")
    return [PageInfo(**row) for row in rows if row["user_id"] == user_id]

@app.get("/category-b/domain/{domain}/count", response_model=int)
def count_domain(domain: str):
    query = "SELECT domain FROM wiki_row"
    rows = client.execute(query)
    return sum(1 for row in rows if row.get("domain") == domain)

@app.get("/category-b/page/{page_id}", response_model=PageDetail)
def page_detail(page_id: int):
    query = "SELECT * FROM wiki_row"
    rows = client.execute(query)
    for row in rows:
        if row.get("page_id") == page_id:
            return PageDetail(**row)
    raise HTTPException(status_code=404, detail="Page not found")

@app.get("/category-b/active-users", response_model=List[ActiveUser])
def active_users(
    start_time: datetime = Query(...),
    end_time: datetime = Query(...)
):
    rows = client.execute("SELECT user_id, user_name, timestamp FROM wiki_row")
    user_counts: Dict[str, Dict[str, int]] = {}
    for row in rows:
        ts = row.get("timestamp")
        if not ts or not (start_time <= ts <= end_time):
            continue
        uid = row.get("user_id")
        uname = row.get("user_name")
        if not uid or not uname:
            continue
        user_counts.setdefault(uid, {"user_name": uname, "count": 0})
        user_counts[uid]["count"] += 1
    return [
        ActiveUser(user_id=uid, user_name=data["user_name"], page_count=data["count"])
        for uid, data in user_counts.items()
    ]
