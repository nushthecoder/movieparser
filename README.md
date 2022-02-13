# Movies Parser


### **Description**

This parser extracts raw data from a CSV file and prepares the data to popualate the relational database.

### **Technology Stack**

Python

### **IDE** 

Visual Studio Code

### **Files**

*db*: handles database connection 

*populate.py*: heart of the application. Parses raw CSV data and populates relational database with the parsed data.

*movies.csv*: original flat file containing movie data in CSV format. 

*requirements.txt*: file with required packages in order to run populate.py. 
### **Relational Model**

### **Data Definition Language**

### **TABLES**

movies
------
```
CREATE TABLE movies (
	movie_id SERIAL PRIMARY KEY,
	movie_title VARCHAR(400),
    movie_description VARCHAR (1000),
	release_year VARCHAR(4),
	runtime_minutes VARCHAR (3),
	rating numeric,
	CHECK (rating >= 0.0 and rating <= 10.0),
	votes int,
	revenue_millions numeric,
	metascore numeric,
	CHECK (metascore >= 0 and metascore <= 100)
)
```

actor_ref
---------
```
CREATE TABLE actor_reference_table (
	actor_id SERIAL PRIMARY KEY,
	first_name VARCHAR(200),
    middle_name VARCHAR (200) NULL,
	last_name VARCHAR (200)
)
```

genre_ref
--------
```
CREATE TABLE genre_reference_table (
	genre_id SERIAL PRIMARY KEY,
	genre VARCHAR (100)
)
```

director_ref
-----------
```
CREATE TABLE director_reference_table (
	director_id SERIAL PRIMARY KEY,
	first_name VARCHAR(200),
	middle_name VARCHAR (200) NULL,
	last_name VARCHAR (200)

)
```

movie_actor
-----------
1 primary composite key composed of movie_id + actor_id

2 foreign keys:

1. *movie_id from movies table*
2. *actor_id from actor_ref*
```
CREATE TABLE movie_actor (
	movie_id INT,
	actor_id INT,
	PRIMARY KEY (movie_id, actor_id)
	CONSTRAINT fk_movie
		FOREIGN KEY(movie_id)
			REFERENCES movies(movie_id)
	CONSTRAINT fk_actor
		FOREIGN KEY(actor_id)
			REFERENCES actor_reference_table(actor_id)

)
```

movie_genre
-----------
1 primary composite key composed of movie_id + genre_id

2 foreign keys:

1. *movie_id from movies table*
2. *genre_id from genre_ref*
```
CREATE TABLE movie_genre (
	movie_id INT,
	genre_id INT,
	PRIMARY KEY (movie_id, genre_id),
	CONSTRAINT fk_movie
		FOREIGN KEY(movie_id)
			REFERENCES movies(movie_id),
	CONSTRAINT fk_genre
		FOREIGN KEY (genre_id)
			REFERENCES genre_reference_table(genre_id)

)
```

movie_director
--------------
1 primary composite key composed of movie_id + director_id

2 foreign keys:

1. *movie_id from movies table*
2. *director_id from director_ref*

```
CREATE TABLE movie_director (
	movie_id INT,
	director_id INT,
	PRIMARY KEY (movie_id, director_id),
	CONSTRAINT fk_movie
		FOREIGN KEY(movie_id)
			REFERENCES movies(movie_id),
	CONSTRAINT fk_director
		FOREIGN KEY(director_id)
			REFERENCES director_reference_table(director_id)
)
```

## MOVIES ERD

![Entity Relationship Diagram](/assets/movies-erd.png)

## **SAMPLE SQL COMMANDS**

#### Count of total movies

```
SELECT COUNT(movie_id) FROM movies;
```

#### Movie title with list of actors

```
 SELECT a.movie_title,
	   b.actor_id,
	   c.first_name,
	   c.middle_name,
	   c.last_name
    FROM
	    movies a
    INNER JOIN movie_actor b
	    ON b.movie_id = a.movie_id
    INNER JOIN actor_ref c
	    ON b.actor_id = c.actor_id;
```

#### Movies with their genre(s)

```
SELECT a.movie_title,
	   b.genre_id,
	   c.genre
    FROM
	    movies a
    INNER JOIN movie_genre b
	    ON b.movie_id = a.movie_id
    INNER JOIN genre_ref c
	    ON b.genre_id = c.genre_id;
```

#### Movie title with director

```
SELECT a.movie_title,
	   b.director_id,
	   c.first_name,
	   c.middle_name,
	   c.last_name
    FROM
	    movies a
    INNER JOIN movie_director b
	    ON b.movie_id = a.movie_id
    INNER JOIN director_ref c
	    ON b.director_id = c.director_id;
```

#### Unique list of actors

```
SELECT actor_id, first_name, middle_name, last_name FROM actor_ref;
```

#### Unique list of directors

```
SELECT director_id, first_name, middle_name, last_name FROM director_ref;
```

#### Unique list of genres

```
SELECT genre_id, genre FROM genre_ref;
```

#### Movies with ratings that are greater than 4.0

```
SELECT movie_title, rating
    FROM movies
    WHERE rating > 4.0;
```


