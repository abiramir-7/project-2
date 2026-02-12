-- Create the sector table [cite: 54]
CREATE TABLE stock_sectors (
    symbol VARCHAR(20) PRIMARY KEY,
    sector VARCHAR(50)
);

-- Insert mappings (Examples for Nifty 50) [cite: 54]
INSERT INTO stock_sectors (symbol, sector) VALUES 
('RELIANCE', 'Energy'), ('TCS', 'IT'), ('INFY', 'IT'), 
('HDFCBANK', 'Financial Services'), ('ICICIBANK', 'Financial Services'),
('TATAMOTORS', 'Automobile'), ('HINDUNILVR', 'Consumer Goods');
-- Note: Add the remaining symbols based on your dataset.