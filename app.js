var mongolian  = require('mongolian')
  , mongo      = new mongolian
  , underscore = global._ = require('underscore')
  , inspect    = global.inspect = require('eyes').inspector()
  , db         = global.db = mongo.db("mapreduce-development")
  , Faker      = require('Faker');

var songs = db.collection("products");
songs.drop(function(){

  for(i=0; i < 1000; i++) {
    songs.save({product: Faker.Company.bs(), company: Faker.Company.companyName(), random: Math.random()});
  }

});


