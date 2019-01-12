
class User():
    'Object representing a user in the DB'

    def __init__(self, fbId, email, name, surname):
        self.fb_id = fbId
        self.email = email
        self.name = name
        self.surname = surname
