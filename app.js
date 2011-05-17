var mongolian  = require('mongolian')
  , mongo      = new mongolian
  , underscore = global._ = require('underscore')
  , inspect    = global.inspect = require('eyes').inspector()
  , db         = global.db = mongo.db("mapreduce-development")
  , Faker      = require('Faker');

var products = db.collection("products");
var mrresult = db.collection("mrresult");

var map = function() {
  emit(this._id, 1);
};

var reduce = function(k, vals) {
  return 1;
};

products.drop(function(){

  for(i=0; i < 10000; i++) {
    products.save(
      { product: Faker.Company.bs()
      , company: Faker.Company.companyName()
      , random: Math.random()
      , group: i % 10
      });   
  };

  var sample = Math.random();

  var plist = function(direction, operation, group, limit) {
    var options = {limit: limit, query: {random: {}} , out: { } };
    options.query.random[direction] = sample;
    options.query.group = group;
    options.out[operation] = "mrreduce";
    products.mapReduce(map, reduce, options,
      function(err, res) {
        if(err) throw err;
        if(res.counts.output < limit) {
          console.log("Not enough data! need %d, got %d", limit, res.counts.output);
          plist("$gt", "merge", limit - res.counts.output);
        }
      });
    };
    plist("$lte", "replace", 2, 100);
});


