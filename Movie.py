
class Movie():
    'Object representing a movie in the DB'

    def __init__(self, movie):
        self.title = movie['title']
        self.year = int(movie['release_year'][1:-1])
        self.runtime = movie['facts']['Runtime']
        self.description = movie['description']
        self.tmdb_id = int(movie['link'].split('/')[-1])
        self.fb_id = movie['fb_link'].split('/')[-1]
        self.image_url = movie['image_url']
