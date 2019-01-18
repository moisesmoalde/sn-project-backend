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

GRAPH = Graph(user = neo4jUtils.DB_USER, password = neo4jUtils.DB_PWD)

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
		name = input_json['firstname']
		surname = input_json['lastname']
		neo4jUtils. insertUser(fbID, email, name, surname)
		for i in movies_likes:
			movieFBID = i['fb_id']
			r = neo4jUtils.insertEdge(email, movieFBID, LIKES_TYPE)

		similarity. setAllSimilarities(email)
		user = dict()
		list_movies = list()
		r = GRAPH.run('''
				MATCH (user:User)-[:LIKES]-(movie) WHERE user.email = "{0}" RETURN movie;
			   '''
                .format(id)).data()
		for i in r:
			list_movies.append(r[i]['movie'])
		user['movies'] = list_movies
		r = GRAPH.run('''
				MATCH (user:User) WHERE user.email = "{0}" RETURN user;
			   '''
                .format(email)).data()
		user['user'] = r[0]['user']
		return user

	@cherrypy.expose
	@cherrypy.tools.json_out()
	def GetUser(self, id):
		user = dict()
		list_movies = list()
		r = GRAPH.run('''
				MATCH (user:User)-[:LIKES]-(movie) WHERE user.email = "{0}" RETURN movie;
			   '''
                .format(id)).data()
		for i in r:
			list_movies.append(r[i]['movie'])
		user['movies'] = list_movies
		r = GRAPH.run('''
				MATCH (user:User) WHERE user.email = "{0}" RETURN user;
			   '''
                .format(id)).data()
		user['user'] = r[0]['user']
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
		if  movies_likes:
			for i in movies_likes:
				movieFBID = i['fb_id']
				r = neo4jUtils.insertEdge(email, movieFBID, neo4jUtils.LIKES_TYPE)
		if   movies_dislikes:
			for i in movies_dislikes:
				movieFBID = i['fb_id']
				r = neo4jUtils.insertEdge(email, movieFBID, neo4jUtils.DISLIKES_TYPE)
		similarity. setAllSimilarities(email)
		user = dict()
		list_movies = list()
		r = GRAPH.run('''
				MATCH (user:User)-[:LIKES]-(movie) WHERE user.email = "{0}" RETURN movie;
			   '''
                .format(email)).data()
		for i in r:
			list_movies.append(r[i]['movie'])
		user['movies'] = list_movies
		r = GRAPH.run('''
				MATCH (user:User) WHERE user.email = "{0}" RETURN user;
			   '''
                .format(email)).data()
		user['user'] = r[0]['user']
		return user


def start_server():
		cherrypy.tree.mount(ApiRest(), '/')
		cherrypy.config.update({'error_page.404': error_page_404})
		cherrypy.config.update({'server.socket_port': 8080})
		cherrypy.config.update({'server.socket_host': '0.0.0.0'})
		cherrypy.engine.start()

if __name__ == '__main__':
    start_server()
