DROP PROCEDURE IF EXISTS sp_create_and_insert_users();

CREATE PROCEDURE sp_create_and_insert_users()
AS $$
BEGIN
    -- Create table if it doesn't exist
    CREATE TABLE IF NOT EXISTS user_table (
        user_id INT IDENTITY(1,1) PRIMARY KEY,
        username VARCHAR(50) NOT NULL,
        email VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Insert sample data
    INSERT INTO user_table (username, email)
    VALUES
        ('alice', 'alice@example.com'),
        ('bob', 'bob@example.com'),
        ('charlie', 'charlie@example.com'),
        ('soumyajit', 'soumyajit@example.com'),
        ('soumyajit1', 'soumyajit1@example.com');
END;
$$ LANGUAGE plpgsql;
