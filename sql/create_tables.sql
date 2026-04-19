-- Database bootstrap
CREATE DATABASE IF NOT EXISTS mf_benchmarking;
USE mf_benchmarking;

-- Drop tables for a clean, schema-correct reload (safe for this analytics project)
DROP TABLE IF EXISTS fund_metrics;
DROP TABLE IF EXISTS fund_nav;
DROP TABLE IF EXISTS benchmark_data;
DROP TABLE IF EXISTS fund_metadata;

-- Table 1: fund_metadata
CREATE TABLE IF NOT EXISTS fund_metadata (
  fund_id VARCHAR(10) PRIMARY KEY,
  fund_name VARCHAR(150),
  fund_house VARCHAR(100),
  category VARCHAR(50),
  scheme_type VARCHAR(100),
  benchmark VARCHAR(50),
  expense_ratio FLOAT,
  aum_cr FLOAT
);

-- Table 2: fund_nav
CREATE TABLE IF NOT EXISTS fund_nav (
  id INT AUTO_INCREMENT PRIMARY KEY,
  fund_id VARCHAR(10),
  date DATE,
  nav FLOAT,
  CONSTRAINT fk_fund_nav_fund_id
    FOREIGN KEY (fund_id) REFERENCES fund_metadata(fund_id)
);

-- Table 3: benchmark_data
CREATE TABLE IF NOT EXISTS benchmark_data (
  id INT AUTO_INCREMENT PRIMARY KEY,
  index_name VARCHAR(50),
  date DATE,
  close_price FLOAT
);

-- Table 4: fund_metrics
CREATE TABLE IF NOT EXISTS fund_metrics (
  id INT AUTO_INCREMENT PRIMARY KEY,
  fund_id VARCHAR(10),
  calculated_on DATE,
  annualized_return FLOAT,
  benchmark_return FLOAT,
  alpha FLOAT,
  beta FLOAT,
  sharpe_ratio FLOAT,
  sortino_ratio FLOAT,
  max_drawdown FLOAT,
  CONSTRAINT fk_fund_metrics_fund_id
    FOREIGN KEY (fund_id) REFERENCES fund_metadata(fund_id)
);

