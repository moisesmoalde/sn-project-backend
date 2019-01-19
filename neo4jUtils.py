from py2neo import Graph, Node, Relationship
import json
import pandas as pd

from Movie import Movie
from User import User

DB_USER = 'app'
DB_PWD = 'apirest'
MOVIE_TYPE = 'Movie'
USER_TYPE = 'User'
LIKES_TYPE = 'LIKES'
DISLIKES_TYPE = 'DISLIKES'

GRAPH = Graph(user = DB_USER, password = DB_PWD)

def init():
    GRAPH.delete_all()
    movies = json.load(open("dump_with_images.json"))

    for movie in movies:
        movieDict = vars(Movie(movie))
        GRAPH.create(Node(MOVIE_TYPE, **movieDict))

    ratings = pd.read_csv("final_reduced_ratings.csv")

    current_id, user = '', None
    for index, row in ratings.iterrows():
        movie = GRAPH.nodes.match(MOVIE_TYPE, tmdb_id = int(row['tmdbId'])).first()
        if not movie: continue

        if row['userId'] != current_id:
            user = Node(USER_TYPE, name = "anonymous" + str(row['userId']))
            GRAPH.create(user)
            current_id = row['userId']

        GRAPH.create(Relationship(user, LIKES_TYPE, movie))

def getLikesCount(email):
    "Returns the number of movies the given user likes"
    return GRAPH.run('''
                MATCH r = (u)-->(m)
                WHERE u.email = '{0}'
                RETURN COUNT(m)'''.format(email)).evaluate()

def getUsersWithCommonLikes(email):
    ''' Returns other users with common liked movies
    Each entry is a triple (ID, N1, N2) where:
    - ID is the ID of the other user in the DB
    - N1 is the number of common liked movies
    - N2 is the number of total liked movies of the other user
    '''
    return GRAPH.run('''
                MATCH r1=(u1)-->(m1)
                MATCH r2=(u2)-->(m1)
                MATCH r3=(u2)-->(m2)
                WHERE u1.email = '{0}' AND NOT u1 = u2
                RETURN DISTINCT ID(u2), COUNT(DISTINCT r1), COUNT(DISTINCT r3)'''
                .format(email))

def updateSimilarity(email, otherID, similarity):
    "Creates or updates the similarity edge between the given users"
    GRAPH.run('''
        MATCH (u1:User {{email:'{0}'}})
        MATCH (u2:User)
        WHERE ID(u2) = {1}
        MERGE (u1)-[:SIMILAR{{similarity: {2} }}]->(u2)'''
        .format(email, otherID, similarity))

def deleteSimilarities(email):
    "Deletes all similarity relationships with other users"
    GRAPH.run('''
        MATCH (u:User {{email: '{0}'}})-[r:SIMILAR]->()
        DELETE r'''.format(email))

def getRecommendedMovies(email, skip = 0, limit = 10):
    "Returns movies ordered by the number of likes from similar users"
    return GRAPH.run('''
                MATCH (u01:User {{email:'{0}'}})-[:SIMILAR]-(u02)
                WITH TOFLOAT(COUNT(u02)) AS uCount
                MATCH (u1:User {{email:'{0}'}})-[:SIMILAR]-(u2)
                MATCH (u2)-[:LIKES]->(m2)
                WHERE NOT (u1)-[:LIKES]->(m2)
                AND NOT (u1)-[:DISLIKES]->(m2)
                WITH uCount, m2, COUNT(m2) AS mCount
                RETURN DISTINCT m2 AS movie, mCount/uCount AS score
                ORDER BY score DESC SKIP {1} LIMIT {2}'''
                .format(email, skip, limit)).data()

def insertUser(fbID, email, name):
    "Insert new user in the graph database"
    userDict = vars(User(fbID, email, name,''))
    GRAPH.merge(Node(USER_TYPE, **userDict), USER_TYPE, "email")

def insertEdge(email, movieFBID, edgeType = LIKES_TYPE):
    "Insert relationship between the user and the movie of the given type"
    GRAPH.run('''
        MATCH (u:User {{email : '{0}'}})
        MATCH (m:Movie {{fb_id : '{1}'}})
        MERGE (u)-[:{2}]->(m)'''
        .format(email, movieFBID, edgeType))


if __name__ == '__main__':
    init()