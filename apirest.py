import cherrypy
import random
import string
from py2neo import Graph, Node, Relationship
import json
import pandas as pd
from Movie import Movie
import User
import similarity
import neo4jUtils

def error_page_404(status, message, traceback, version):
    return "404 Error!"

DB_USER = 'app'
DB_PWD = 'apirest'
MOVIE_TYPE = 'Movie'
USER_TYPE = 'User'

GRAPH = Graph(user = DB_USER, password = DB_PWD)
	
class ApiRest():
	
	@cherrypy.expose
	@cherrypy.tools.json_out()
	@cherrypy.tools.json_in()
	def AddUser (self, **kwargs):
	
		input_json = cherrypy.request.json
		print(input_json)
		email = input_json['email']
		
		fbID = input_json['facebook-id']
		movies_likes = input_json['movies']
		name = input_json['name']
		surname = input_json['lastname']
		neo4jUtils. insertUser(fbID, email, name, surname)
		for i in movies_likes:
			movieFBID = i['fb_id']
			r = neo4jUtils.insertEdge(email, movieFBID, 'LIKES')
		
		similarity. setAllSimilarities(email)
		user = GRAPH.run('''
				MATCH (user:User)-[:LIKES]-(movies) WHERE user.email = "{0}" RETURN user,movies;
			   '''
                .format(email)).data()	
		return user
		
	@cherrypy.expose
	@cherrypy.tools.json_out()
	def GetUser(self, id):
		print(id)
		user = GRAPH.run('''
				MATCH (user:User)-[:LIKES]-(movies) WHERE user.email = "{0}" RETURN user,movies;
			   '''
                .format(id)).data()
		return user
	
	@cherrypy.expose
	@cherrypy.tools.json_out()
	def MovieRecommendation(self, id, skip, limit):
		movies = dict()
		movies['movies'] = neo4jUtils.getRecommendedMovies(id,skip,limit)
		return movies
	
	@cherrypy.expose
	@cherrypy.tools.json_out()
	@cherrypy.tools.json_in()
	def AddMovie (self, email):
		input_json = cherrypy.request.json
		movies_likes = input_json['likes']
		movies_dislikes = input_json['dislikes']
		for i in movies_likes:
			movieFBID = i['fb_id']
			r = neo4jUtils.insertEdge(email, movieFBID, 'LIKES')
		similarity. setAllSimilarities(email)	
		user = GRAPH.run('''
				MATCH (user:User)-[:LIKES]-(movies) WHERE user.email = "{0}" RETURN user,movies;
			   '''
                .format(email)).data()		
		return user	


def start_server():
		cherrypy.tree.mount(ApiRest(), '/')
		cherrypy.config.update({'error_page.404': error_page_404})
		cherrypy.config.update({'server.socket_port': 9999})
		cherrypy.config.update({'server.socket_host': '0.0.0.0'})
		cherrypy.engine.start()

if __name__ == '__main__':
    start_server()
