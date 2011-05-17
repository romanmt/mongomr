var mongolian  = require('mongolian')
  , mongo      = new mongolian
  , underscore = global._ = require('underscore')
  , inspect    = global.inspect = require('eyes').inspector()
  , db         = global.db = mongo.db("mapreduce-development")
  , Faker      = require('Faker');

var products = db.collection("products");

var map = function() {
  emit(this.group, 1);
};

var reduce = function(k, vals) {
  var sum = 0;
  for(var i in vals) {
    sum += vals[i];
  }
  return sum;
};

products.drop(function(){

  for(i=0; i < 1000; i++) {
    products.save(
      { product: Faker.Company.bs()
      , company: Faker.Company.companyName()
      , random: Math.random()
      , group: i % 10
      });   
  };
  
  products.mapReduce(map, reduce, {query : { group : 2, random : {$lte: Math.random()}}},
    function(err, res) {
      if(err) throw err;
      inspect(res.find());
    }
  );
});


