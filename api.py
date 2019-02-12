import pandas as pd 
from flask import Flask
from flask_restful import Resource, Api
import sqlite3
import json
from pandas.io.json import json_normalize
import os

app = Flask(__name__)
api = Api(app)

class AppGenreController(Resource):
    def get(self):
        data = pd.read_csv(".\DataFiles\AppleStore.csv") 
        data_music_and_book = data.loc[(data['prime_genre']=='Music') | (data['prime_genre']=='Book')].sort_values(['rating_count_tot'],ascending=False).head(n=10)
        db = sqlite3.connect('.\DataFiles\db')
        cursor = db.cursor()
        
        df = pd.DataFrame({'id':data_music_and_book.id,
                   'track_name': data_music_and_book.track_name,
                   'n_citacoes':data_music_and_book.rating_count_tot,
                   'size_bytes':data_music_and_book.rating_count_tot,
                   'price':data_music_and_book.price,
                   'prime_genre':data_music_and_book.prime_genre
                    }
                   )
        
        df.to_csv(os.path.join('.\DataFiles', 'rating_genre.csv') , sep=',', encoding='utf-8',index=False)

        df.to_sql("rating_genre", db, if_exists="replace")

        dfreader = pd.read_sql("select id,track_name,n_citacoes,size_bytes,price,prime_genre from rating_genre", db)

        return dfreader.reset_index().to_json(orient='records')

class AppGenreNewsController(Resource):
    def get(self):
        data = pd.read_csv(".\DataFiles\AppleStore.csv") 
        data_news=data.loc[data['prime_genre']=='News']
        data_news_max_rating_count= data_news.loc[data_news['rating_count_tot']==data_news.rating_count_tot.max()]

        return { 'max_rating_news_app':
                    {'track_name': data_news_max_rating_count.track_name.max(),
                      'rating_count_tot': int( data_news_max_rating_count.rating_count_tot.max())
                    }
                }

api.add_resource(AppGenreController, '/')
api.add_resource( AppGenreNewsController, '/RatingNewsApp')

if __name__ == '__main__':
    app.run(debug=True)