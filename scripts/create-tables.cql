CREATE KEYSPACE IF NOT EXISTS wiki_keyspace WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE wiki_keyspace;

-- Raw Wiki Data
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

-- Domain Hourly Stats
CREATE TABLE domain_hourly_stats (
  hour_bucket TIMESTAMP,
  domain TEXT,
  page_count INT,
  PRIMARY KEY ((hour_bucket), domain)
);

-- Bot Creation Stats
CREATE TABLE bot_creation_stats (
  time_start TIMESTAMP,
  time_end TIMESTAMP,
  domain TEXT,
  created_by_bots INT,
  PRIMARY KEY ((time_start, time_end), domain)
);

-- Top Users
CREATE TABLE top_users (
  time_start TIMESTAMP,
  time_end TIMESTAMP,
  user_id TEXT,
  user_name TEXT,
  page_count INT,
  page_titles LIST<TEXT>,
  PRIMARY KEY ((time_start, time_end), user_id)
);
