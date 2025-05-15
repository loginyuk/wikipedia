CREATE KEYSPACE IF NOT EXISTS hw12_lohin WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE hw12_lohin;

CREATE TABLE IF NOT EXISTS wiki (
    user_id text,
    domain text,
    created_at text,
    page_title text,
    PRIMARY KEY ((domain), created_at, user_id)
);
