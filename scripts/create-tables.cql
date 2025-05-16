CREATE KEYSPACE IF NOT EXISTS wiki_keyspace WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE wiki_keyspace;

-- Raw Events
CREATE TABLE wiki_row (
  page_id INT,
  event_id TEXT,
  user_id INT,
  domain TEXT,
  title TEXT,
  user_name TEXT,
  is_bot BOOLEAN,
  timestamp TIMESTAMP,
  event_hour TIMESTAMP,
  PRIMARY KEY ((event_hour), timestamp, page_id)
);

-- Domain Hourly Stats (Category A, Query 1)
CREATE TABLE domain_hourly_stats (
  hour_bucket TIMESTAMP,
  domain TEXT,
  page_count INT,
  PRIMARY KEY ((hour_bucket), domain)
);

-- Bot Creation Stats (Category A, Query 2)
CREATE TABLE bot_creation_stats (
  time_start TIMESTAMP,
  time_end TIMESTAMP,
  domain TEXT,
  created_by_bots INT,
  PRIMARY KEY ((time_start, time_end), domain)
);

-- Top Users (Category A, Query 3)
CREATE TABLE top_users (
  time_start TIMESTAMP,
  time_end TIMESTAMP,
  user_id TEXT,
  user_name TEXT,
  page_count INT,
  page_titles LIST<TEXT>,
  PRIMARY KEY ((time_start, time_end), user_id)
);






-- Domain List (Category B, Query 1)
CREATE TABLE domains (
  domain TEXT PRIMARY KEY
);
-- User Pages (Category B, Query 2)
CREATE TABLE user_pages (
  user_id TEXT,
  page_id TEXT,
  title TEXT,
  domain TEXT,
  timestamp TIMESTAMP,
  PRIMARY KEY ((user_id), timestamp, page_id)
);

-- Domain Pages (Category B, Query 3)
CREATE TABLE domain_pages (
  domain TEXT,
  timestamp TIMESTAMP,
  page_id TEXT,
  title TEXT,
  PRIMARY KEY ((domain), timestamp, page_id)
);

-- Pages By ID (Category B, Query 4)
CREATE TABLE pages_by_id (
  page_id TEXT,
  domain TEXT,
  title TEXT,
  user_id TEXT,
  user_name TEXT,
  is_bot BOOLEAN,
  timestamp TIMESTAMP,
  PRIMARY KEY (page_id)
);

-- Users By Time Range (Category B, Query 5)
CREATE TABLE users_by_hour (
  hour_bucket TIMESTAMP,
  user_id TEXT,
  user_name TEXT,
  page_count INT,
  PRIMARY KEY ((hour_bucket), user_id)
);