import webapp2

from model.model_creator_handler import ModelCreatorHandler


class MainHandler(webapp2.RequestHandler):
    def get(self):
        self.response.write('Hello world!')

app = webapp2.WSGIApplication([
    ('/', MainHandler),
    ('/createModels', ModelCreatorHandler)
], debug=True)
