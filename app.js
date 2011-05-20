var mongolian  = require('mongolian')
  , mongo      = new mongolian
  , underscore = global._ = require('underscore')
  , inspect    = global.inspect = require('eyes').inspector()
  , db         = global.db = mongo.db("mapreduce-development")
  , async      = require('async')
  , Faker      = require('Faker');

var products = db.collection("products");
var randomized = db.collection("randomized");

var mapInjectRandom = function() {
  emit(this._id, 
      { _id    : this._id
      , random : Math.random()
      });
};

var mapPlist = function() {
  emit(this._id, 
      { _id    : this.value._id
      , random : this.value.random 
      });
}

var reduce = function(k, vals) {
  return vals;
};

var dropProducts = function (callback) {
    console.log("drop products");
    products.drop( function (){
        callback(null);
    });
};

var createProducts =  function (callback) {
    console.log("create products");
    for (i = 0; i < 20; i++) {
        products.save( { product: Faker.Company.bs()
                       , company: Faker.Company.companyName()
        });
    }
    callback(null)
};

var injectRandomAttribute = function (callback) {  
    console.log("inject random");
    products.mapReduce(mapInjectRandom, reduce, {out: 'randomized'}, callback);
};

var selectProductIds = function (res, callback) {
    console.log("select prod list");
    randomized.mapReduce(mapPlist, reduce,
          { out   : {inline : 1}
          , query : {'value.random' : {$gte : Math.random()}}
          , limit : 6},
          callback);
};

var selectProducts = function (res, callback) {
    console.log("select products");
    var ids = _.map(res.results, function(x) {return x.value._id} );
    products.find({_id : {$in :ids}}).toArray(callback);
};

var printResults = function (err, prods) {
    if (err) throw err;
    console.log("print products");
    inspect(prods);
};

async.waterfall(
    [ dropProducts
    , createProducts
    , injectRandomAttribute
    , selectProductIds
    , selectProducts
    ]
    , printResults);

