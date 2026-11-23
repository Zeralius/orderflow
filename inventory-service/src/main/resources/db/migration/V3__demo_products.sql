-- Demo catalogue. LIMITED-POSTER has a single item, to show "out of stock" quickly.
INSERT INTO products (sku, name, price, available_quantity) VALUES
    ('BOOK-JAVA25',    'Modern Java 25 (book)',        39.90, 100),
    ('MUG-COFFEE',     'OrderFlow coffee mug',         12.50, 250),
    ('KEYBOARD-MECH',  'Mechanical keyboard',         129.00,  25),
    ('HEADSET-PRO',    'Noise-cancelling headset',    249.00,  10),
    ('LAPTOP-STAND',   'Aluminium laptop stand',       49.00,  40),
    ('LIMITED-POSTER', 'Signed limited-edition poster', 75.00,   1);
