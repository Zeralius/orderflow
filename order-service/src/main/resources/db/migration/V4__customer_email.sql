-- The customer's email from the login token, so notifications know where to write.
ALTER TABLE orders ADD COLUMN customer_email VARCHAR(320);
