
CREATE TABLE users (
    Userid INT NOT NULL  AUTO_INCREMENT PRIMARY KEY,
    Name VARCHAR (100),
    SecondName VARCHAR(100) NOT NULL,
    Country VARCHAR(100),
    Age INT
);

INSERT INTO users (Name, SecondName, Country, Age) VALUES
('John', 'Doe', 'USA', 30),
('Alice', 'Smith', 'UK', 25),
('Carlos', 'Gomez', 'Spain', 40),
('Emma', 'Dubois', 'France', 28),
('Liam', 'Müller', 'Germany', 35),
('Daniel', 'Janca', 'Poland', 24);




