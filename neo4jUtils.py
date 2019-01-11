from py2neo import Graph, Node, Relationship
import json
import pandas as pd

from Movie import Movie

DB_USER = 'neo4j'
DB_PWD = 'mesopotamia'
MOVIE_TYPE = 'Movie'
USER_TYPE = 'User'

GRAPH = Graph(user = DB_USER, password = DB_PWD)

def init():
    graph.delete_all()
    movies = json.load(open("dump.json"))

    for movie in movies:
        movieDict = vars(Movie(movie))
        graph.create(Node(MOVIE_TYPE, **movieDict))

    ratings = pd.read_csv("final_reduced_ratings.csv")

    current_id, user = '', None
    for index, row in ratings.iterrows():
        movie = graph.nodes.match(MOVIE_TYPE, tmdb_id = int(row['tmdbId'])).first()
        if not movie: continue

        if row['userId'] != current_id:
            user = Node(USER_TYPE, name = "anonymous" + str(row['userId']))
            graph.create(user)
            current_id = row['userId']

        graph.create(Relationship(user, "LIKES", movie))

def getLikesCount(userName):
    "Returns the number of movies the given user likes"
    return GRAPH.run('''
                MATCH r = (u)-->(m)
                WHERE u.name = '{0}'
                RETURN COUNT(m)'''.format(userName)).evaluate()

def getUsersWithCommonLikes(userName):
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
                WHERE u1.name = '{0}' AND NOT u2.name = '{0}'
                RETURN DISTINCT ID(u2), COUNT(DISTINCT r1), COUNT(DISTINCT r3)'''
                .format(userName))

def updateSimilarity(userName, otherID, similarity):
    "Creates or updates the similarity edge between the given users"
    GRAPH.run('''
        MATCH (u1:User {{name:'{0}'}})
        MATCH (u2:User)
        WHERE ID(u2) = {1}
        MERGE (u1)-[:SIMILAR{{similarity: {2} }}]->(u2)'''
        .format(userName, otherID, similarity))

def deleteSimilarities(userName):
    "Deletes all similarity relationships with other users"
    GRAPH.run('''
        MATCH (u:User {{name: '{0}'}})-[r:SIMILAR]->()
        DELETE r'''.format(userName))

def getRecommendedMovies(userName, skip = 0, limit = 10):
    "Returns movies ordered by the number of likes from similar users"
    return GRAPH.run('''
                MATCH (u01:User {{name:'{0}'}})-[:SIMILAR]-(u02)
                WITH TOFLOAT(COUNT(u02)) AS uCount
                MATCH (u1:User {{name:'{0}'}})-[:SIMILAR]-(u2)
                MATCH (u2)-[:LIKES]->(m2)
                WHERE NOT (u1)-[:LIKES]->(m2)
                WITH uCount, m2, COUNT(m2) AS mCount
                RETURN DISTINCT m2 AS movie, mCount/uCount AS score
                ORDER BY score DESC SKIP {1} LIMIT {2}'''
                .format(userName, skip, limit)).data()


if __name__ == '__main__':
    init()