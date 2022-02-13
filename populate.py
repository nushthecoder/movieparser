'''
This parser extracts raw data from a CSV flat file and parses through it in order to insert it into respective tables inside of a relational database.
@author nushthecoder
'''
import psycopg2
from db.connection import Connection
from collections import OrderedDict
import csv

'''
getGenresFromMovieRecord() - grabs a list of genres from a given movie, and returns a list of genres from a comma separated string
@param {movie} - dictionary composed of data from movies CSV file
@return {genres} - list of genres
'''
def getGenresFromMovieRecord(movie):
    genre = movie["Genre"]
    genre = genre.replace(", ", ",")
    genres = genre.split(",")
    return genres

'''
getActorsFromMovieRecord() - grabs the string, replaces spaces and splits at each space.
@param {movie} - dictionary composed of data from movies CSV file
@return {actors} - list of actors
'''
def getActorsFromMovieRecord(movie):
   
    actor = movie["Actors"]   
    actor = actor.replace(", ", ",")
    actors = actor.split(",")    
    return actors 

'''
getDirectorsFromMovieRecord() - grabs the string, replaces spaces and splits at each space.
@param {movie} - dictionary composed of data from movies CSV file
@return {directors} - list of directors
'''
def getDirectorsFromMovieRecord(movie):
 
    director = movie["Director"]
    director = director.replace(", ", ",") 
    directors = director.split(",")
    return directors

'''
checkIfExist() - checks to see whether a record exists in the database or not.
@param {conn} - database connection
@param {sql} - sql to execute
@param {data} - dynamic values for bind variables
@return {exists} - boolean, True or False
'''
def checkIfExist(conn, sql, data):
    cur = conn.cursor()
    exists = False
    if cur is not None:
        cur.execute(sql, data)
        result = cur.fetchone()
        exists = result[0] > 0
        cur.close()
    return exists

'''
getId() - retrieves identifier(s) from the database
@param {conn} - database connection 
@param {sql} - sql to execute 
@param {data} - dyanmic values for bind variables 
@return {id} - unique identifier
'''
def getId(conn, sql, data):
    cur = conn.cursor()
    cur.execute(sql, data)
    result = cur.fetchone()
    id = result[0]
    return id

'''
checkMiddleName() - accepts a full name and checks whether the name has a middle name.
@param {name} - full name inside of a string
@return {tuple} - returns a tuple of a boolean and the list generated from a full name
'''
def checkMiddleName(name):
    values = name.split(" ")
    return len(values) == 3, values

'''
clearDatabase() - clears database after each run in order to maintain data integrity.
@param {conn} - database connection
@return {rows_deleted} - each row that is deleted from database

'''
def clearDatabase(conn):    
    rows_deleted = 0

    try:
        cur = conn.cursor()
        
        sql_delete = "Delete from movie_actor"
        cur.execute(sql_delete)

        sql_delete = "Delete from movie_genre"
        cur.execute(sql_delete)

        sql_delete = "Delete from movie_director"
        cur.execute(sql_delete)

        sql_delete = "Delete from actor_ref"
        cur.execute(sql_delete)

        sql_delete = "Delete from genre_ref"
        cur.execute(sql_delete)

        sql_delete = "Delete from director_ref"
        cur.execute(sql_delete)

        sql_delete = "Delete from movies"
        cur.execute(sql_delete)

        rows_deleted = cur.rowcount

        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    return rows_deleted
    
'''
addMovieRecord() - adds a movie record.
@param {conn} - database connection
@param {movie} - dictionary composed of data from movies CSV file
'''
def addMovieRecord(conn, movie):
    try:
        cur = conn.cursor()

        title = movie["Title"]
        #title = title.replace("'", "''")
        title = title.replace("'", "\'")
        
        description = movie["Description"]
        description = description.replace("'", "''")
        description = description.replace("'", "\'")

        year = movie["Year"]
        runtime = movie["Runtime (Minutes)"]
        rating = movie["Rating"]
        votes = movie["Votes"]
        revenue = movie["Revenue (Millions)"]
        metascore = movie["Metascore"]
            
        sql_insert = """
                        INSERT INTO movies 
                        (movie_title, 
                        movie_description, 
                        release_year, 
                        runtime_minutes, 
                        rating, votes, 
                        revenue_millions, 
                        metascore) 
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s)"""     
        
        values = [title, description, year, runtime, rating, votes, revenue, metascore]
        cur.execute(sql_insert, values)

        conn.commit()
        count = cur.rowcount
        print(count, "Movie record inserted successfully into table")
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)

'''
populateGenreRef() - loops through genres list and inserts unique genre name into genre_ref table in database.
@param {conn} - database connection
@param {movie} - dictionary composed of data from movies CSV file

'''
def populateGenreRef (conn, movie):
    try:
        cur = conn.cursor()
        genres = getGenresFromMovieRecord(movie)
        
        
        for g in genres:
            sql = "SELECT count(*) FROM genre_ref WHERE genre = %s"
            values = (g,)
            result = checkIfExist(conn, sql, values)

            if (result == False):
                sql = "INSERT INTO genre_ref (genre) VALUES(%s)"
                values = (g,)
                cur.execute(sql, values)
                conn.commit()
                print("Genre inserted successfully into table!")
            else:
                print(g, ' genre exists!')
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)

'''
populateActorRef() - populates actor reference table with movie_id and actor_id(s).
@param {conn} - database connection
@param {movie} - dictionary composed of data from movies CSV file
'''
def populateActorRef (conn, movie):
    try:
        cur = conn.cursor()
        actors = getActorsFromMovieRecord(movie) 

        for a in actors:
            hasMiddleName, full_name = checkMiddleName(a)
            
            # check for a middle name
            if hasMiddleName == False:
                first_name, last_name = full_name  
                sql = "SELECT count(*) FROM actor_ref WHERE (first_name, last_name) = (%s, %s)"
                values = (first_name, last_name)
                result = checkIfExist(conn, sql, values)
                
                if (result == False):
                    sql = "INSERT INTO actor_ref (first_name, last_name) VALUES(%s, %s)"
                    values = (first_name, last_name)
                    cur.execute(sql, values)
                    conn.commit()
                    print("Actor successfully inserted into table!")
                else:
                    print(first_name, last_name, "Already exists in table!")
            elif hasMiddleName == True:
                first_name, middle_name, last_name = full_name
                sql = "SELECT count(*) FROM actor_ref WHERE (first_name, middle_name, last_name) = (%s, %s, %s)"
                values = (first_name, middle_name, last_name)
                result = checkIfExist(conn, sql, values)
                
                if (result == False):
                    sql = "INSERT INTO actor_ref (first_name, middle_name, last_name) VALUES(%s, %s, %s)"
                    values = (first_name, middle_name, last_name)
                    cur.execute(sql, values)
                    conn.commit()
                    print("Actor successfully inserted into table!")
                else:
                    print(first_name, middle_name, last_name, "Already exists in table!")
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)

'''
populateDirectorRef () - populates a director reference table with movie_id and director_id(s)
@param {conn} - database connection
@param {movie} - dictionary composed of data from movies CSV file
'''  
def populateDirectorRef (conn, movie):
    try:
        cur = conn.cursor()
        directors = getDirectorsFromMovieRecord(movie)

        for d in directors:
            hasMiddleName, full_name = checkMiddleName(d)
        
            if hasMiddleName == False:
                first_name, last_name = full_name
                sql = "SELECT count(*) FROM director_ref WHERE (first_name, last_name) = (%s, %s)"
                values = (first_name, last_name)
                cur.execute(sql, values)
                conn.commit()
                result = checkIfExist(conn, sql, values)

                if (result == False):
                    sql = "INSERT INTO director_ref (first_name, last_name) VALUES(%s, %s)"
                    values = (first_name, last_name)
                    cur.execute(sql, values)
                    conn.commit()
                    print("Director successfully inserted into table!") 
                else:
                    print(first_name, last_name, "Director already exists in table!")
            elif hasMiddleName == True:
                first_name, middle_name, last_name = full_name
                sql = "SELECT count(*) FROM director_ref WHERE (first_name, middle_name, last_name) = (%s, %s, %s)"
                values = (first_name, middle_name, last_name)
                cur.execute(sql, values)
                conn.commit()
                result = checkIfExist(conn, sql, values)

                if (result == False):
                    sql = "INSERT INTO director_ref (first_name, middle_name, last_name) VALUES(%s, %s, %s)"
                    values = (first_name, middle_name, last_name)
                    cur.execute(sql, values) 
                    conn.commit()
                    print("Director sucessfully inserted into table!")
                else:
                    print(first_name, middle_name, last_name, "Director already exists in table!")
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    
'''
populateMovieGenre() - loops through genres list and splits each individual genre into its own element,
                       retrieves movie_id from movies table and inserts into movie_genre along with each individual genre_id that is associated with given movie.
@param {conn} - database connection
@param {movie} - dictionary composed of data from movies CSV file
@param {movie id} - unique identifier for each movie record

'''
def populateMovieGenre(conn, movie, movie_id):
    cur = conn.cursor()
    genres = getGenresFromMovieRecord(movie)
    
    for g in genres:
        sql = "SELECT genre_id id FROM genre_ref WHERE genre = %s"
        genre_id = getId(conn, sql, (g,))
        sql = "INSERT INTO movie_genre (movie_id, genre_id) VALUES(%s, %s)"
        values = (movie_id, genre_id)
        cur.execute(sql, values)
        conn.commit()

'''
populateMovieActor() - checks to see if actor has first_name and last_name, or first_name, middle_name, and last_name.
                       retrieves movie_id from movies table and inserts it into movie_actor table 
                       along with corresponding actor_id for each actor that belongs in given movie.                    
@param {conn} - database connection
@param {movie} - dictionary of data composed from movies CSV file
@param {movie id} - unique identifier for each movie record
'''
def populateMovieActor(conn, movie, movie_id):
    try:
        cur = conn.cursor()
        actors = getActorsFromMovieRecord(movie)
        for a in actors:
            hasMiddleName, full_name = checkMiddleName(a)
            actor_id = None 

            if hasMiddleName == False:
                first_name, last_name = full_name
                sql = "SELECT actor_id id FROM actor_ref WHERE (first_name, last_name) = (%s, %s)"  
                values = (first_name, last_name,)
                actor_id = getId(conn, sql, values)
            elif hasMiddleName == True:
                first_name, middle_name, last_name = full_name
                sql = "SELECT actor_id id  FROM actor_ref WHERE (first_name, middle_name, last_name) = (%s, %s, %s)"
                values = (first_name, middle_name, last_name)
                actor_id = getId(conn, sql, values)
               
            if (actor_id > 0):
                sql = "INSERT INTO movie_actor (movie_id, actor_id) VALUES(%s, %s)"
                values = (movie_id, actor_id)
                cur.execute(sql, values)
                conn.commit()
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)       

'''
populateMovieDirector() - checks to see if director has first_name and last_name or first_name, middle_name, and last_name.
                          retrieves movie_id from movies table and inserts it into movie_director table 
                          along with corresponding director_id from director_ref for the director that is associated with given movie.
@param {conn} - database connection
@param {movie} - dictionary composed of data from movies CSV file
@param {movie id} - unique identifier for each movie record
'''
def populateMovieDirector(conn, movie, movie_id):
    try:
        cur = conn.cursor()
        directors = getDirectorsFromMovieRecord(movie)
        for d in directors:
            hasMiddleName, full_name = checkMiddleName(d)
            director_id = None 

            if hasMiddleName == False:
                first_name, last_name = full_name
                sql = "SELECT director_id id FROM director_ref WHERE (first_name, last_name) = (%s, %s)"  
                values = (first_name, last_name,)
                director_id = getId(conn, sql, values)
            elif hasMiddleName == True:
                first_name, middle_name, last_name = full_name
                sql = "SELECT director_id id  FROM director_ref WHERE (first_name, middle_name, last_name) = (%s, %s, %s)"
                values = (first_name, middle_name, last_name)
                director_id = getId(conn, sql, values)
          
            if (director_id > 0):
                sql = "INSERT INTO movie_director (movie_id, director_id) VALUES(%s, %s)"
                values = (movie_id, director_id)
                cur.execute(sql, values)
                conn.commit()
    except(Exception, psycopg2.DatabaseError) as error:
        print(error) 
'''
main() - heart of the script to parse movies CSV file and populate relational data in a movies database
'''
def main():
    try:
        # grab a PostGreSql database connection
        db = Connection()
        conn = db.getConnection()

        # clear database for multiple run(s)
        clearDatabase(conn)

        with open("movies.csv", newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                movie = OrderedDict(row)
                movie = dict(movie)
                title = movie["Title"]
                
                # add a new movie in the movies table
                addMovieRecord(conn, movie)
                sql = "SELECT movie_id id FROM movies WHERE movie_title = %s"
                movie_id = getId(conn, sql, (title,))

                # populate actors reference table
                populateActorRef(conn, movie)

                # populate director reference table
                populateDirectorRef(conn, movie)

                # populate generes reference table
                populateGenreRef(conn, movie)

                # populate genres for a given movie
                populateMovieGenre(conn, movie, movie_id)

                # populate actors for a given movie
                populateMovieActor(conn, movie, movie_id)

                # populate directors for a given movie
                populateMovieDirector(conn, movie, movie_id)

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn:
            conn.close()    

if __name__ == '__main__':
    main()
    