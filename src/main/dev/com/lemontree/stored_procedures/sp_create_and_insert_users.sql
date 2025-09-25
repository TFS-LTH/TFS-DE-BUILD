CREATE OR REPLACE PROCEDURE sp_create_and_insert_users()
 LANGUAGE plpgsql
AS $$
BEGIN
    -- Insert sample data
    INSERT INTO user_table (username, email)
    VALUES
        ('alice', 'alice@example.com'),
        ('bob', 'bob@example.com'),
        ('charlie', 'charlie@example.com'),
        ('soumyajit', 'soumyajit@example.com'),
        ('soumyajit1', 'soumyajit1@example.com');
END;
$$
