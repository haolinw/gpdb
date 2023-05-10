-- check parser

-- correct syntax
CREATE RESOURCE GROUP rg_io_limit_1 WITH (concurrency=10, cpu_hard_quota_limit=10, io_limit='pg_default:rbps=1000,wbps=1000,riops=1000,wiops=1000');

-- * must be unique tablespace
CREATE RESOURCE GROUP rg_io_limit_2 WITH (concurrency=10, cpu_hard_quota_limit=10, io_limit='pg_default:rbps=1000,wbps=1000,riops=1000,wiops=1000;*:wbps=1000');

-- tail ;
CREATE RESOURCE GROUP rg_io_limit_2 WITH (concurrency=10, cpu_hard_quota_limit=10, io_limit='pg_default:rbps=1000,wbps=1000,riops=1000,wiops=1000;');

-- tail ,
CREATE RESOURCE GROUP rg_io_limit_2 WITH (concurrency=10, cpu_hard_quota_limit=10, io_limit='pg_default:rbps=1000,wbps=1000,riops=1000,');

-- with space
CREATE RESOURCE GROUP rg_io_limit_2 WITH (concurrency=10, cpu_hard_quota_limit=10, io_limit=' pg_default:rbps=1000,wbps=1000,riops=1000,wiops=1000');

-- with space
CREATE RESOURCE GROUP rg_io_limit_2 WITH (concurrency=10, cpu_hard_quota_limit=10, io_limit='pg_default:rbps=1000, wbps=1000,riops=1000,wiops=1000');

DROP RESOURCE GROUP rg_io_limit_1;
